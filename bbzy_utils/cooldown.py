import asyncio
import time
from functools import wraps
from typing import Callable


class CallIgnoredException(Exception):
    def __init__(self, seconds_left: float):
        self.seconds_left = seconds_left


def _cooldown_call(cooldown: float, wait_action: Callable[[float], None]):
    def wrapper(func: Callable):
        last_call_time = 0

        @wraps(func)
        def callback(*args, **kwargs):
            nonlocal last_call_time
            cur_time = time.time()
            time_left = cooldown - (cur_time - last_call_time)
            if time_left > 0:
                wait_action(time_left)
                last_call_time = cur_time + time_left
            else:
                last_call_time = cur_time
            return func(*args, **kwargs)

        return callback

    return wrapper


def cooldown_call_wait(cooldown: float):
    return _cooldown_call(cooldown, lambda seconds_left: time.sleep(seconds_left))


def cooldown_call_ignore(cooldown: float):
    return _cooldown_call(cooldown, CallIgnoredException.__init__)


def async_cooldown_call_wait(cooldown: float):
    def wrapper(func: Callable):
        last_call_time = 0
        use_cooldown = True

        @wraps(func)
        async def callback(*args, **kwargs):
            if use_cooldown:
                nonlocal last_call_time
                cur_time = time.time()
                seconds_left = cooldown - (cur_time - last_call_time)
                if seconds_left > 0:
                    await asyncio.sleep(seconds_left)
                    last_call_time = cur_time + seconds_left
                else:
                    last_call_time = cur_time
            return await func(*args, **kwargs)

        def set_use_cooldown(v: bool):
            nonlocal use_cooldown
            use_cooldown = v

        callback.set_use_cooldown = set_use_cooldown
        return callback

    return wrapper
