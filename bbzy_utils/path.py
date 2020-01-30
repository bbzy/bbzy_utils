import os
from typing import Optional


class DelayPathBase:
    @classmethod
    def join(cls, *args):
        return os.path.join(*map(str, args))


class DelayDirPath(DelayPathBase):
    def __init__(self, *dirs):
        self._dir_path = self.join(*dirs)

    def __call__(self, file_name: Optional[str] = None):
        if not os.path.isdir(self._dir_path):
            os.makedirs(self._dir_path, exist_ok=True)
        if file_name is None:
            file_name = ''
        return self.join(self._dir_path, file_name)

    def __str__(self):
        return self._dir_path

    def delay_dir(self, *dirs):
        return DelayDirPath(self._dir_path, *dirs)


class DelayFilePath(DelayPathBase):
    def __init__(self, *paths):
        self._dir_path = self.join(*paths[:-1])
        self._full_path = self.join(self._dir_path, paths[-1])

    def __call__(self):
        if not os.path.isdir(self._dir_path):
            os.makedirs(self._dir_path, exist_ok=True)
        return self._full_path

    def __str__(self):
        return self._full_path


def join_dirs(*args, create_dirs=True):
    path = os.path.join(*args)
    if create_dirs:
        os.makedirs(path, exist_ok=True)
    return path


def join_file_path(*args, create_dirs=True):
    dir_path = os.path.join(*args[:-1])
    if create_dirs:
        os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, args[-1])
