import typing as t
from multiprocessing import Queue as MPQueue
import threading
import os

import pandas as pd
import numpy as np

from .worker import Worker
from .utils import round_robin_workers, LoggerMixin
from .results_accumulator import ResultsAccumulator
from .messages import TaskMessage


class DFProcessor(LoggerMixin):
    _RESULT_QUEUE_SIZE = 50
    _N_WORKERS = os.cpu_count()
    _WORKER_QUEUE_SIZE = 10

    def __init__(
        self,
        worker_queue_size: t.Optional[int] = None,
        n_workers: t.Optional[int] = None,
    ) -> None:
        if n_workers and n_workers <= 0:
            raise ValueError("n_workers must be >= 1")
        if worker_queue_size and worker_queue_size <= 0:
            raise ValueError("worker_queue_size must be >= 1")
        self._n_workers = n_workers or DFProcessor._N_WORKERS
        self._worker_queue_size = (
            worker_queue_size or DFProcessor._WORKER_QUEUE_SIZE
        )
        self._result_queue: "MPQueue[t.Any]" = MPQueue(
            DFProcessor._RESULT_QUEUE_SIZE
        )
        self._stop_event = threading.Event()
        self._results_accumulator = ResultsAccumulator(
            self._result_queue, self._stop_event
        )
        self._results_accumulator.start()

        self._running_workers: t.List[Worker] = []
        self.logger.info("DFProcessor initialized")

    def _start_workers(self, n_workers: int) -> None:
        for i in range(n_workers):
            worker = Worker(i, self._worker_queue_size, self._result_queue)
            worker.start()
            self._running_workers.append(worker)
        self.logger.info(f"{n_workers} workers started")

    def process_df(
        self,
        df: pd.DataFrame,
        func: t.Callable[[t.Union[pd.Series, pd.DataFrame]], t.Any],
        n_partitions: int,
    ) -> t.Any:
        if not callable(func):
            raise TypeError("func must be callable")
        rows = df.shape[0]
        if n_partitions < 1 or n_partitions > rows:
            raise ValueError("n_partitions must be >= 1 and <= df.shape[0]")

        # Start workers
        n_workers_required = (
            self._n_workers
            if self._n_workers <= n_partitions  # type: ignore
            else n_partitions
        )
        self._start_workers(n_workers_required)  # type: ignore

        # Split dataframe into partitions
        partition_indices = [
            (p[0], p[-1]) for p in np.array_split(range(rows), n_partitions)
        ]
        rows_per_partition = partition_indices[0][-1] - partition_indices[0][0]
        self.logger.info(
            f"Total rows: {rows}; "
            f"Number of partitions: {n_partitions}; "
            f"Rows per partition: {rows_per_partition}; "
            f"Partition indices: {partition_indices}"
        )
        # Distribute partitions among workers
        get_worker_round_robin = round_robin_workers(self._running_workers)
        counter = 0
        while partition_indices:
            left, right = partition_indices.pop(0)
            worker_task = TaskMessage(
                counter, func=func, df=df.iloc[left:right]
            )
            for worker in get_worker_round_robin:
                enqueued = worker.enqueue_task_within_timeout(worker_task)
                if enqueued:
                    self.logger.info(
                        f"Sent partition {counter} to worker {worker.index}"
                    )
                    break
            counter += 1

        # Stop workers to ensure they're done processing partitions
        self._stop_workers()

        # Stop the accumulator to ensure it collected all results from
        # th workers
        self._stop_results_accumulator()
        results = self._results_accumulator.results
        self.logger.info(f"Total of {len(results)} results received")
        return results

    def _stop_workers(self) -> None:
        for worker in self._running_workers:
            worker.stop_worker()
        self.logger.info("Stop message send to workers, trying to join")
        for worker in self._running_workers:
            worker.join()
        self.logger.info("Workers joined")

    def _stop_results_accumulator(self) -> None:
        self._stop_event.set()
        self._results_accumulator.join()
        self.logger.info("ResultsAccumulator joined")
