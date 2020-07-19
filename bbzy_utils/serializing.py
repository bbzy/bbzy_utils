import contextlib
import io
import json
import os
import pickle
from abc import abstractmethod
from collections import defaultdict
from typing import Any, Callable, Iterator, Generic, TypeVar, Type, Optional, Dict

T = TypeVar('T')


def write_file(data: bytes, base_path: str, ext_name: str = '.dat'):
    with open(base_path + ext_name, 'wb') as fp:
        fp.write(data)


def read_file(base_path: str, ext_name: str = '.dat') -> bytes:
    with open(base_path + ext_name, 'rb') as fp:
        return fp.read()


def write_text(text: str, base_path: str):
    with open(base_path + '.txt', 'w') as fp:
        fp.write(text)


def read_text(base_path: str) -> Any:
    with open(base_path + '.txt', 'r') as fp:
        return fp.read()


def dump_json(obj: Any, base_path: str, **kwargs):
    with open(base_path + '.json', 'w') as fp:
        json.dump(obj, fp, **kwargs)


def load_json(base_path: str, **kwargs) -> Any:
    with open(base_path + '.json', 'r') as fp:
        return json.load(fp, **kwargs)


def dump_pickle(obj: Any, base_path: str, chunk_size: int = 0):
    if chunk_size == 0:
        with open(base_path + '.pkl', 'wb') as fp:
            pickle.dump(obj, fp, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with io.BytesIO() as bio:
            pickle.dump(obj, bio, protocol=pickle.HIGHEST_PROTOCOL)
            bio.seek(0, io.SEEK_SET)
            with open(base_path + '.pkl', 'wb') as fp:
                while True:
                    content = bio.read(chunk_size)
                    if not content:
                        break
                    fp.write(content)


def load_pickle(base_path: str, chunk_size: int = 0) -> Any:
    if chunk_size == 0:
        with open(base_path + '.pkl', 'rb') as fp:
            return pickle.load(fp)
    else:
        with open(base_path + '.pkl', 'rb') as fp, io.BytesIO() as bio:
            while True:
                content = fp.read(chunk_size)
                if not content:
                    break
                bio.write(content)
            bio.seek(0, io.SEEK_SET)
            return pickle.load(bio)


def make_or_load_pickle(maker: Callable, base_path: str, chunk_size: int = 0, force_make: bool = False) -> Any:
    if force_make or not os.path.isfile(base_path + '.pkl'):
        data = maker()
        if isinstance(data, Iterator):
            data = list(data)
        dump_pickle(data, base_path, chunk_size)
        return data
    else:
        return load_pickle(base_path, chunk_size)


def load_pickle_context(base_path: str, chunk_size: int = 0):
    class LoadPickleContext(contextlib.AbstractContextManager):
        def __init__(self, base_path_: str):
            self._base_path = base_path_
            self._data = None

        def __enter__(self):
            with contextlib.suppress(FileNotFoundError):
                self._data = load_pickle(self._base_path, chunk_size)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if exc_type is None and self._data is None:
                raise ValueError('set_data() should be called')

        def set_data(self, data):
            self._data = data
            if self._data is not None:
                dump_pickle(data, self._base_path, chunk_size)

        def get_data(self):
            return self._data

        def data_loaded(self) -> bool:
            return self._data is not None

    return LoadPickleContext(base_path)


def to_jsonable(obj, cast_map: Optional[Dict[type, Callable[[Any], Any]]] = None) -> Any:
    if cast_map is None:
        cast_map = dict()

    def _to_jsonable(_obj):
        _type = type(_obj)
        _cast_pred = cast_map.get(_type)
        if _cast_pred:
            return _to_jsonable(_cast_pred(_obj))
        if _obj is None or isinstance(_obj, (str, int, float, bool)):
            return _obj
        if isinstance(_obj, (list, set, tuple)):
            return [_to_jsonable(_i) for _i in _obj]
        if isinstance(_obj, (dict, defaultdict)):
            return {_to_jsonable(_k): _to_jsonable(_v) for _k, _v in _obj.items()}
        _pred = getattr(_obj, 'to_jsonable', None)
        if _pred:
            return _to_jsonable(_pred())
        raise TypeError('Invalid type: {}'.format(_type))

    return _to_jsonable(obj)


def from_jsonable(obj, t: Type, cast_map: Optional[Dict[Type, Callable[[Any], Any]]] = None) -> Any:
    if cast_map is None:
        cast_map = dict()

    def _from_jsonable(_obj, _t: Type):
        _type_used = getattr(_t, '__origin__', None) or getattr(_t, '__extra__', None)
        if _type_used is None:
            # For types are not in typing
            _type_used = _t

        # ==== In Typing Mapping ====
        _cast_pred = cast_map.get(_type_used)
        if _cast_pred is not None:
            return _cast_pred(_obj)

        # ==== Primitive ====
        if _type_used is str:
            return str(_obj)
        elif _type_used is int:
            return int(_obj)
        elif _type_used is float:
            return float(_obj)

        # ==== Collection ====
        _arg_types = getattr(_t, '__args__', None)
        if _arg_types:
            if _type_used is tuple:
                return tuple(_from_jsonable(_v, _arg_types[_i]) for _i, _v in enumerate(_obj))
            elif _type_used is list:
                _vt = _arg_types[0]
                return [_from_jsonable(_v, _vt) for _i, _v in enumerate(_obj)]
            elif _type_used is dict:
                _kt, _vt = _arg_types
                return {_from_jsonable(_k, _kt): _from_jsonable(_v, _vt) for _k, _v in _obj.items()}
        # ==== ====
        _pred = getattr(_t, 'from_jsonable', None)
        if _pred:
            return _pred(_obj)
        raise TypeError('Invalid value: {}'.format(_obj))

    return _from_jsonable(obj, t)


class SerializableObjectBase(Generic[T]):
    def __init__(self, default_value: T):
        self._object = self.load()
        self._dirty = False

    @property
    def dirty(self):
        return self._dirty

    @dirty.setter
    def dirty(self, value: bool):
        self._dirty = value

    def get_object(self) -> T:
        return self._object

    def set_object(self, new_object: T):
        self._object = new_object
        self.save()
        self._dirty = False

    def set_object_without_saving(self, new_object: T):
        self._object = new_object
        self._dirty = True

    @abstractmethod
    def load(self) -> bool:
        """
        :return: True for loaded and False for file not found
        """
        raise NotImplementedError()

    @abstractmethod
    def save(self) -> None:
        raise NotImplementedError()

    def __enter__(self):
        """
        :rtype: SerializableObjectWrapper[T]
        """
        return SerializableObjectWrapper(self)  # type:

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._dirty:
            self.save()
            self._dirty = False


class SerializableObjectWrapper(Generic[T]):
    def __init__(self, serializable_object: SerializableObjectBase[T]):
        self._serializable_object = serializable_object

    def w(self) -> T:
        self._serializable_object.dirty = True
        return self._serializable_object.get_object()

    def r(self) -> T:
        return self._serializable_object.get_object()


class SerializablePickleObject(SerializableObjectBase[T]):
    def __init__(self, serializing_path: str, default_value: T):
        self._path = serializing_path + '.pkl'
        super().__init__(default_value)

    def load(self):
        if not os.path.isfile(self._path):
            return None
        with open(self._path, 'rb') as fp:
            self.set_object(pickle.load(fp))

    def save(self):
        with open(self._path, 'wb') as fp:
            pickle.dump(self.get_object(), fp)


class SerializableJsonObject(SerializableObjectBase[T]):
    def __init__(
            self,
            serializing_path: str,
            decl_type: Type,
            default_value: T,
            cast_map_for_loading: Optional[Dict[Type, Callable[[Any], Any]]] = None,
            cast_map_for_saving: Optional[Dict[Type, Callable[[Any], Any]]] = None,
    ):
        """
        :param decl_type: Complete type from typing module like List[Tuple[str, int]]
        """
        self._path = serializing_path + '.json'
        self._decl_type = decl_type
        self._cast_map_for_loading = cast_map_for_loading
        self._cast_map_for_saving = cast_map_for_saving
        super().__init__(default_value)

    @property
    def path(self):
        return self._path

    def load(self) -> bool:
        if not os.path.isfile(self._path):
            return False
        with open(self._path) as fp:
            self.set_object_without_saving(from_jsonable(json.load(fp), self._decl_type, self._cast_map_for_loading))
            return True

    def save(self):
        tmp_path = self._path + '.tmp'
        with open(tmp_path, 'w') as fp:
            json.dump(to_jsonable(self.get_object(), self._cast_map_for_saving), fp)
        os.rename(tmp_path, self._path)
