from typing import Any


class BestKeeper:
    def __init__(self):
        self._score = None
        self._addition = None

    def set_into(self, score, addition: Any = None) -> bool:
        if self._score is None or score > self._score:
            self._score = score
            self._addition = addition
            return True
        return False

    def reset(self):
        self._score = None
        self._addition = None
        
    @property
    def score(self):
        return self._score

    @property
    def addition(self):
        return self._addition
