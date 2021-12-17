import multiprocessing
from multiprocessing import Queue as MPQueue
import typing as t

from .messages import Message, TaskMessage, StopMessage
from .utils import LoggerMixin
from .exceptions import UserProvidedCallableError


# TODO:
#       1. Each worker gets indices, pulls DF from PlasmaStore, slices it,
#          passes to the user provided function


class Worker(multiprocessing.Process, LoggerMixin):
    def __init__(
        self,
        worker_index: int,
        task_queue_size: int,
        result_queue: MPQueue,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.index = worker_index
        if task_queue_size <= 0:
            raise ValueError("Worker task queue size must be >= 1")
        self._task_queue: "MPQueue[Message]" = MPQueue(maxsize=task_queue_size)
        self._result_queue: "MPQueue[t.List[Message]]" = result_queue
        self.logger.info(f"Worker {worker_index} initialized")

    def enqueue_task_within_timeout(
        self, task: TaskMessage, *, timeout: float = 0.1
    ) -> bool:
        """
        Send a task to the running worker. Note, TaskMessage attributes
        must be serializable
        """
        if timeout < 0:
            raise ValueError("Timeout must be >= 0")
        try:
            self._task_queue.put(task, timeout=timeout)
        except Exception:
            self.logger.warning(
                f"Worker {self.index}'s queue is full, didn't enqueue message"
            )
            return False
        else:
            return True

    def stop_worker(self) -> None:
        # TODO: Could block indefinitely
        self._task_queue.put(StopMessage())

    def run(self) -> None:
        while True:
            message = self._task_queue.get()
            if isinstance(message, StopMessage):
                break
            elif isinstance(message, TaskMessage):
                try:
                    result = message.func(*message.args, **message.kwargs)
                except UserProvidedCallableError as e:
                    self.logger.exception(
                        f"Failed while calling the provided callable. "
                        f"Error: {e}"
                    )
                    raise e
                else:
                    self._result_queue.put([result])
            else:
                self.logger.warning(
                    f"Unknown message received {message}. Skipped"
                )
        self.logger.info("Worker stopped")
