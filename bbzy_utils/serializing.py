from abc import abstractmethod
from typing import Any, Callable, Iterator, Generic, TypeVar
import json
import os
import io
import pickle
import contextlib

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
    def __init__(self, obj: T, serializing_path: str):
        self._object = obj
        self._path = serializing_path
        self._dirty = False
        loaded = self.load()
        if loaded is not None:
            self._object = loaded
        else:
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
    def __init__(self, obj: T, serializing_path: str):
        super().__init__(obj, serializing_path + '.pkl')

    def load(self):
        if not os.path.isfile(self.path):
            return None
        with open(self.path, 'rb') as fp:
            return pickle.load(fp)

    def save(self, obj):
        with open(self.path, 'wb') as fp:
            pickle.dump(obj, fp)


class AutoJsonWrapper(AutoSerializationWrapperBase[T]):
    def __init__(self, obj: T, serializing_path: str):
        super().__init__(obj, serializing_path + '.json')

    def load(self):
        if not os.path.isfile(self.path):
            return False
        with open(self.path, 'rb') as fp:
            return json.load(fp)

    def save(self, obj):
        with open(self.path, 'wb') as fp:
            json.dump(obj, fp)
