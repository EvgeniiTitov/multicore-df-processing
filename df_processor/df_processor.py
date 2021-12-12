import time
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
        if n_workers is not None and n_workers <= 0:
            raise ValueError("n_workers must be >= 1")
        if worker_queue_size is not None and worker_queue_size <= 0:
            raise ValueError("worker_queue_size must be >= 1")
        self._n_workers = n_workers or DFProcessor._N_WORKERS
        self._worker_queue_size = (
            worker_queue_size or DFProcessor._WORKER_QUEUE_SIZE
        )
        self._result_queue: "MPQueue[t.Any]" = MPQueue(
            DFProcessor._RESULT_QUEUE_SIZE
        )
        self._stop_event = threading.Event()
        self._results_accumulator: ResultsAccumulator = None  # type: ignore
        self._running_workers: t.List[Worker] = []
        self.logger.info("DFProcessor initialized")

    def process_df(
        self,
        df: pd.DataFrame,
        func: t.Callable[[t.Union[pd.Series, pd.DataFrame]], t.Any],
        n_partitions: int,
    ) -> t.Any:
        """
        Processes passed dataframe using provided callable.
        Before processing the provided data frame will get split into N
        partitions. Each partition processing will get scheduled to one of the
        workers.
        Results are collected automatically and returned to the user
        """
        if not callable(func):
            raise TypeError("func must be callable")
        rows = df.shape[0]
        if n_partitions < 1 or n_partitions > rows:
            raise ValueError("n_partitions must be >= 1 and <= df.shape[0]")

        # Start workers and results accumulator
        n_workers_required = (
            self._n_workers
            if self._n_workers <= n_partitions  # type: ignore
            else n_partitions
        )
        self._start_workers(n_workers_required)  # type: ignore
        self._start_results_accumulator(n_partitions)

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
                counter, func=func, df=df.iloc[left : right + 1, :]
            )
            for worker in get_worker_round_robin:
                enqueued = worker.enqueue_task_within_timeout(worker_task)
                if enqueued:
                    self.logger.info(
                        f"Sent partition {counter} to worker {worker.index}"
                    )
                    break
                else:
                    time.sleep(0.25)
            counter += 1

        # Stop workers to ensure they're done processing partitions
        self._stop_workers()

        # Stop the accumulator to ensure it collected all results from
        # the workers
        self._stop_results_accumulator()
        results = self._results_accumulator.get_results()
        self.logger.info(f"Total of {len(results)} results received")
        return results

    def _start_workers(self, n_workers: int) -> None:
        for i in range(n_workers):
            worker = Worker(i, self._worker_queue_size, self._result_queue)
            worker.start()
            self._running_workers.append(worker)
        self.logger.info(f"{n_workers} workers started")

    def _stop_workers(self) -> None:
        for worker in self._running_workers:
            worker.stop_worker()
        self.logger.info("Stop message sent to workers, trying to join")
        for worker in self._running_workers:
            worker.join()
        self.logger.info("Workers joined")

    def _start_results_accumulator(self, n_partitions: int) -> None:
        self._results_accumulator = ResultsAccumulator(
            n_partitions, self._result_queue, self._stop_event
        )
        self._results_accumulator.start()
        self.logger.info("ResultsAccumulator started")

    def _stop_results_accumulator(self) -> None:
        self._stop_event.set()
        self._results_accumulator.join()
        self.logger.info("ResultsAccumulator joined")
