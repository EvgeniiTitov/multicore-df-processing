import typing as t
from multiprocessing import Queue as MPQueue
import threading
import os
import math

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

        self._workers: t.List[Worker] = []
        # TODO: You could start workers after checking the number of splits?
        self._start_workers()
        self.logger.info("DFProcessor initialized")

    def _start_workers(self) -> None:
        for i in range(self._n_workers):  # type: ignore
            worker = Worker(i, self._worker_queue_size, self._result_queue)
            worker.start()
            self._workers.append(worker)
        self.logger.info(f"{self._n_workers} workers started")

    def process_df(
        self,
        df: pd.DataFrame,
        func: t.Callable[[t.Union[pd.Series, pd.DataFrame]], t.Any],
        rows_per_batch: int,
    ) -> t.Any:
        if not callable(func):
            raise TypeError("func must be callable")
        rows = df.shape[0]
        if rows_per_batch < 1 or rows_per_batch > rows:
            raise ValueError("n_rows_batch must be >= 1 and <= df.shape[0]")
        splits = math.ceil(rows / rows_per_batch)
        partition_indices = np.array_split(range(rows), splits)
        self.logger.info(
            f"Rows: {rows}; Batch size: {rows_per_batch}; "
            f"Number of batches: {splits}"
        )
        self.logger.info(
            f"Partition indices: "
            f"{' '.join([str((p[0], p[-1])) for p in partition_indices])}"
        )
        # Distribute partitions among workers
        get_worker_round_robin = round_robin_workers(self._workers)
        counter = 0
        while partition_indices:
            p_index = partition_indices.pop(0)
            worker_task = TaskMessage(
                counter, func=func, df=df.iloc[p_index[0] : p_index[-1]]
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
        for worker in self._workers:
            worker.stop_worker()
        self.logger.info("Stop message send to workers, trying to join")
        for worker in self._workers:
            worker.join()
        self.logger.info("Workers joined")

    def _stop_results_accumulator(self) -> None:
        self._stop_event.set()
        self._results_accumulator.join()
        self.logger.info("ResultsAccumulator joined")
