from multiprocessing import Pool
from multiprocessing.pool import AsyncResult
from typing import List, Callable, Any, Iterator


class ProcessPool:
    def __init__(self, processors: int = None, **kwargs):
        if __debug__ and processors is None:
            processors = 1
        self._pool = Pool(processors, **kwargs)
        self._results = list()  # type: List[AsyncResult]

    def queue_task(self, task: Callable, *args, **kwargs) -> AsyncResult:
        res = self._pool.apply_async(task, args=args, kwds=kwargs)
        self._results.append(res)
        return res

    def get_async_results(self) -> List[AsyncResult]:
        slot = self._results
        self._results = type(self._results)()
        return slot

    def get_results(self) -> Iterator[Any]:
        return (i.get() for i in self.get_async_results())

    def check_exceptions(self) -> None:
        list(self.get_results())

    def __enter__(self):
        self._pool.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._pool.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        return self._pool.close()

    def join(self):
        return self._pool.join()
