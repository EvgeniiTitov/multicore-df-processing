import multiprocessing
from multiprocessing import Queue as MPQueue
import typing as t

from .messages import Message, TaskMessage, StopMessage
from .utils import LoggerMixin
from .exceptions import UserProvidedCallableError
from .plasma_store import CustomPlasmaClient


class Worker(multiprocessing.Process, LoggerMixin):
    def __init__(
        self,
        worker_index: int,
        task_queue_size: int,
        result_queue: MPQueue,
        plasma_socket_name: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.index = worker_index
        if task_queue_size <= 0:
            raise ValueError("Worker task queue size must be >= 1")
        self._task_queue: "MPQueue[Message]" = MPQueue(maxsize=task_queue_size)
        self._result_queue: "MPQueue[t.List[Message]]" = result_queue
        self._plasma_client = CustomPlasmaClient(
            parent=False, socket_name=plasma_socket_name
        )
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

    def process_task_message(self, message: TaskMessage) -> t.Any:
        plasma_object_id = message.object_id
        left, right = message.slice_indices
        # TODO: Add try-except here in case object not found
        # TODO: This is blocking call, careful -> use timeout_ms=1000
        df = self._plasma_client.plasma_client.get(plasma_object_id)
        df_partition = df.iloc[left : right + 1, :]
        try:
            result = message.func(
                df_partition, *message.args, **message.kwargs
            )
        except UserProvidedCallableError as e:
            self.logger.exception(
                f"Failed while calling the provided callable. " f"Error: {e}"
            )
            raise e
        return result

    def run(self) -> None:
        while True:
            message = self._task_queue.get()
            if isinstance(message, StopMessage):
                break
            elif isinstance(message, TaskMessage):
                result = self.process_task_message(message)
                self._result_queue.put([result])
            else:
                self.logger.warning(
                    f"Unknown message received {message}. Skipped"
                )
        self.logger.info("Worker stopped")
