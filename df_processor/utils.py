import typing as t
import logging
import sys


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
