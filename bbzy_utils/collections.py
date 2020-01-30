from collections import defaultdict
from typing import Generic, TypeVar, DefaultDict, Set, Optional, Dict

T = TypeVar('T')
LT = TypeVar('LT')
RT = TypeVar('RT')


class TwoClassesGraph(Generic[LT, RT]):
    def __init__(self):
        self._left_nodes = defaultdict(set)  # type: DefaultDict[LT, Set[RT]]
        self._right_nodes = defaultdict(set)  # type: DefaultDict[RT, Set[LT]]

    def get_from_left(self, k: LT) -> Set[RT]:
        return self._left_nodes.get(k, set())

    def get_from_right(self, k: RT) -> Set[LT]:
        return self._right_nodes.get(k, set())

    def get_first_from_left(self, k: LT, default: Optional[RT] = None) -> RT:
        s = self._left_nodes.get(k)
        if not s:
            return default
        return next(iter(s))

    def get_first_from_right(self, k: RT, default: Optional[LT] = None) -> LT:
        s = self._right_nodes.get(k)
        if not s:
            return default
        return next(iter(s))

    def set_edge(self, left_node: LT, right_node: RT):
        self._left_nodes[left_node].add(right_node)
        self._right_nodes[right_node].add(left_node)

    def has_edge(self, left_node: LT, right_node: RT):
        if left_node not in self._left_nodes:
            return False
        return right_node in self._left_nodes[left_node]

    def remove_edge(self, left_node: LT, right_node: RT):
        self._left_nodes[left_node].remove(right_node)
        self._right_nodes[right_node].remove(left_node)
        if not self._left_nodes[left_node]:
            del self._left_nodes[left_node]
        if not self._right_nodes[right_node]:
            del self._right_nodes[right_node]

    def is_in_left(self, k: LT):
        return k in self._left_nodes

    def is_in_right(self, k: RT):
        return k in self._right_nodes

    def get_left_keys(self):
        return self._left_nodes.keys()

    def get_right_keys(self):
        return self._right_nodes.keys()

    def remove_left_key(self, k: LT):
        for i in self._left_nodes[k]:
            self._right_nodes[i].remove(k)
            if not self._right_nodes[i]:
                del self._right_nodes[i]
        del self._left_nodes[k]

    def remove_right_key(self, k: RT):
        for i in self._right_nodes[k]:
            self._left_nodes[i].remove(k)
            if not self._left_nodes[i]:
                del self._left_nodes[i]
        del self._right_nodes[k]


class TwoClassesDict(Generic[LT, RT]):
    def __init__(self):
        self._left_nodes = dict()  # type: Dict[LT, RT]
        self._right_nodes = dict()  # type: Dict[RT, LT]

    def get_from_left(self, k: LT) -> Set[RT]:
        res = self.get_first_from_left(k)
        if res is None:
            return set()
        return {res}

    def get_from_right(self, k: RT) -> Set[LT]:
        res = self.get_first_from_right(k)
        if res is None:
            return set()
        return {res}

    def get_first_from_left(self, k: LT, default: Optional[RT] = None) -> RT:
        return self._left_nodes.get(k, default)

    def get_first_from_right(self, k: RT, default: Optional[LT] = None) -> LT:
        return self._right_nodes.get(k, default)

    def set_edge(self, left_node: LT, right_node: RT):
        if left_node in self._left_nodes:
            raise KeyError('{} already in left nodes'.format(left_node))
        if right_node in self._right_nodes:
            raise KeyError('{} already in right nodes'.format(right_node))
        self._left_nodes[left_node] = right_node
        self._right_nodes[right_node] = left_node

    def has_edge(self, left_node: LT, right_node: RT):
        if left_node not in self._left_nodes:
            return False
        return right_node == self._left_nodes[left_node]

    def remove_edge(self, left_node: LT, right_node: RT):
        if left_node not in self._left_nodes:
            raise KeyError('{} not found in left nodes'.format(left_node))
        if self._left_nodes[left_node] != right_node:
            raise KeyError('Edge({}, {}) not found'.format(left_node, right_node))
        del self._left_nodes[left_node]
        del self._right_nodes[right_node]

    def is_in_left(self, k: LT):
        return k in self._left_nodes

    def is_in_right(self, k: RT):
        return k in self._right_nodes

    def get_left_keys(self):
        return self._left_nodes.keys()

    def get_right_keys(self):
        return self._right_nodes.keys()

    def remove_left_key(self, k: LT):
        v = self._left_nodes[k]
        del self._left_nodes[k]
        del self._right_nodes[v]

    def remove_right_key(self, k: RT):
        v = self._right_nodes[k]
        del self._right_nodes[k]
        del self._left_nodes[v]
