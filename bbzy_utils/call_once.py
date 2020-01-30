from typing import Callable


def call_once(func: Callable):
    to_run = True

    def _fun(*args, **kwargs):
        nonlocal to_run
        if to_run:
            to_run = False
            return func(*args, **kwargs)
        return None

    return _fun
