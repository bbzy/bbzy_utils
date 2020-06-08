from abc import abstractmethod
from typing import Any, Callable, Iterator, Generic, TypeVar, Type, Optional, Dict, Tuple
import json
import os
import io
import pickle
import contextlib
from functools import partial

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


class AutoSerializationWrapperBase(Generic[T]):
    def __init__(self, serializing_path: str, default_value: Optional[Any] = None):
        self._object = default_value
        self._path = serializing_path
        self._dirty = False
        loaded = self.load()
        if loaded is not None:
            self._object = loaded
        else:
            self._object = default_value
            self.save(self._object)

    @property
    def path(self):
        return self._path

    def get_object(self) -> T:
        self._dirty = True
        return self._object

    def set_object(self, obj: T):
        self._object = obj
        self.save(self._object)
        self._dirty = False

    @abstractmethod
    def load(self) -> T:
        raise NotImplementedError()

    @abstractmethod
    def save(self, obj: T):
        raise NotImplementedError()

    def __enter__(self) -> T:
        if self._dirty:
            self._object = self.load()
            self._dirty = False
        return self._object

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save(self._object)
        self._dirty = False


class AutoPickleWrapper(AutoSerializationWrapperBase[T]):
    def __init__(self, serializing_path: str, default_value: Optional[Any] = None):
        super().__init__(serializing_path + '.pkl', default_value)

    def load(self):
        if not os.path.isfile(self.path):
            return None
        with open(self.path, 'rb') as fp:
            loaded = pickle.load(fp)
            return loaded

    def save(self, obj):
        with open(self.path, 'wb') as fp:
            pickle.dump(obj, fp)


class AutoJsonWrapper(AutoSerializationWrapperBase[T]):
    def __init__(
            self,
            serializing_path: str,
            default_value: Optional[Any] = None,
            *,
            from_json: Callable[[Any], Any] = None,
            to_json: Callable[[Any], Any] = None,
    ):
        self._from_json = from_json
        self._to_json = to_json
        super().__init__(serializing_path + '.json', default_value)

    def load(self):
        if not os.path.isfile(self.path):
            return None
        with open(self.path) as fp:
            loaded = json.load(fp)
            if self._from_json:
                loaded = self._from_json(loaded)
            return loaded

    def save(self, obj):
        tmp_path = self.path + '.tmp'
        if self._to_json is not None:
            obj = self._to_json(obj)
        with open(tmp_path, 'w') as fp:
            json.dump(obj, fp)
        os.rename(tmp_path, self.path)


class AutoJsonWrapperEx(AutoSerializationWrapperBase[T]):
    class JsonEncoder(json.JSONEncoder):
        def __init__(self, cast_map: Optional[Dict[Type, Tuple[Callable, Callable]]], *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._cast_map = cast_map

        def iterencode(self, o, _one_shot=False):
            if self._cast_map is not None:
                t = type(o)
                cast_pred = self._cast_map.get(t)
                if cast_pred is not None:
                    return cast_pred[1](o)
            return super().iterencode(o, _one_shot)

        def default(self, o):
            return getattr(o, 'to_json')()

    def __init__(
            self,
            serializing_path: str,
            decl_type: Type,
            default_value: Optional[Any] = None,
            cast_map: Optional[Dict[Type, Tuple[Callable, Callable]]] = None,
    ):
        """
        :param decl_type: Complete type from typing module like List[Tuple[str, int]]
        :param cast_map: The first element of the value is from_pred and the second is to_pred
        """
        self._decl_type = decl_type
        self._cast_map = cast_map
        super().__init__(serializing_path + '.json', default_value)

    def load(self):
        if not os.path.isfile(self.path):
            return None
        with open(self.path) as fp:
            loaded = json.load(fp)
            return self._from_json(self._decl_type, loaded)

    def save(self, obj):
        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'w') as fp:
            json.dump(obj, fp, cls=partial(AutoJsonWrapperEx.JsonEncoder, self._cast_map))
        os.rename(tmp_path, self.path)

    def _from_json(self, t: Type, obj):
        origin = getattr(t, '__origin__', None) or getattr(t, '__extra__', None)
        if origin is None:
            # For types are not in typing
            origin = t
        # ==== In Typing Mapping ====
        cast_pred = self._cast_map.get(origin)
        if cast_pred is not None:
            return cast_pred[0](origin)
        # ==== Primitive ====
        if origin is str:
            return str(obj)
        elif origin is int:
            return int(obj)
        elif origin is float:
            return float(obj)
        # ==== Collection ====
        args = getattr(t, '__args__', None)
        if origin is tuple:
            return tuple(self._from_json(args[i], v) for i, v in enumerate(obj))
        elif origin is list:
            vt = args[0]
            return [self._from_json(vt, v) for i, v in enumerate(obj)]
        elif origin is dict:
            kt, vt = args
            return {self._from_json(kt, k): self._from_json(vt, v) for k, v in obj.items()}
        # ==== ====
        return getattr(t, 'from_json')(obj)
