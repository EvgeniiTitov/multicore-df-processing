import typing as t
import logging
import sys
import time
from functools import wraps


T = t.TypeVar("T")


logging.basicConfig(
    format="'%(asctime)s %(levelname)s "
    "- %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


def round_robin_workers(workers: t.Sequence[T]) -> t.Iterator[T]:
    while True:
        for worker in workers:
            yield worker


class LoggerMixin:
    @property
    def logger(self) -> logging.Logger:
        name = ".".join([__name__, self.__class__.__name__])
        return logging.getLogger(name)


def get_object_size(item: object) -> int:
    return sys.getsizeof(item)


def timer(func: t.Callable) -> t.Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> t.Any:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        stop = time.perf_counter()
        print(f"Took {stop - start} seconds")
        return result

    return wrapper
