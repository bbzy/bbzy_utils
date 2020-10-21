from contextlib import contextmanager
from typing import Iterable, List, Any, Callable, TypeVar, Dict, \
    Iterator, Sized, Union, Reversible, ContextManager, Set
import itertools

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


def dict_get(d: Dict[K, V], keys: Iterable[K]) -> Iterator[V]:
    return (d.get(k) for k in keys)


def iter_adj(a: Iterable, n: int) -> Iterator[tuple]:
    iters = itertools.tee(a, n)
    return zip(*(itertools.islice(iters[i], i, None) for i in range(n)))


def dict_set_default(d: Dict[K, V], key: K, pred: Callable[[Any], V], *args, **kwargs) -> V:
    v = d.get(key)
    if not v:
        v = d[key] = pred(*args, **kwargs)
    return v


def find(a: Iterable, v: Any, *, key: Callable = None) -> int:
    for i, va in enumerate(a):
        if key is not None:
            va = key(va)
        if va == v:
            return i
    return -1


def rfind(a: Union[Reversible, Sized], v: Any, *, key: Callable = None) -> int:
    for i, va in enumerate(reversed(a)):
        if key is not None:
            va = key(va)
        if va == v:
            return len(a) - i - 1
    return -1


def list_select(a: list, indices: Iterable[int]) -> list:
    new_list = list()
    for i in indices:
        new_list.append(a[i])
    return new_list


def list_remove(a: list, indices: Iterable[int]) -> list:
    new_list = list()
    indices = set(indices)
    for i in range(len(a)):
        if i not in indices:
            new_list.append(a[i])
    return new_list


@contextmanager
def list_select_context(a: list) -> ContextManager[List[int]]:
    indices = list()
    try:
        yield indices
    finally:
        new_list = list()
        for i in indices:
            new_list.append(a[i])
        a[:] = new_list


@contextmanager
def list_remove_context(a: list) -> ContextManager[Set[int]]:
    indices = set()
    try:
        yield indices
    finally:
        if indices:
            new_list = list()
            for i, v in enumerate(a):
                if i not in indices:
                    new_list.append(v)
            a[:] = new_list


@contextmanager
def dict_select_context(d: dict) -> ContextManager[List[int]]:
    keys = list()
    try:
        yield keys
    finally:
        new_dict = dict()
        for k in keys:
            new_dict[k] = d[k]
        d.clear()
        d.update(new_dict)


@contextmanager
def dict_remove_context(d: dict) -> ContextManager[set]:
    keys = set()
    try:
        yield keys
    finally:
        if keys:
            for k in keys:
                d.pop(k)
