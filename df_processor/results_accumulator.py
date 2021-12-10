import threading
from multiprocessing import Queue as MPQueue
import typing as t

from tqdm import tqdm

from .utils import LoggerMixin


class ResultsAccumulator(threading.Thread, LoggerMixin):
    def __init__(
        self,
        n_partitions: int,
        results_queue: MPQueue,
        stop_event: threading.Event,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._n_partitions = n_partitions
        self._stop_event = stop_event
        self._results_queue = results_queue
        self._results: t.List[t.Any] = []
        self._message_shown = False
        self.logger.info("ResultsAccumulator initialized")

    def get_results(self) -> t.List[t.Any]:
        return self._results

    def run(self) -> None:
        partitions_received = 0
        pbar = tqdm(total=self._n_partitions)
        pbar.set_description("Partitions processed:")
        while True:
            if self._stop_event.is_set():
                if partitions_received == self._n_partitions:
                    break
                elif not self._message_shown:
                    self.logger.warning(
                        "ResultsAccumulator received stop signal. "
                        "Finishing accumulating results"
                    )
                    self._message_shown = True
            try:
                result: t.Any = self._results_queue.get(timeout=0.5)
            except Exception:
                pass
            else:
                self._results.append(result)
                partitions_received += 1
                pbar.update(1)
        pbar.close()
        self.logger.info("ResultsAccumulator stopped")
