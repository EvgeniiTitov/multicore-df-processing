import subprocess
import time

from pyarrow import plasma

from .utils import LoggerMixin


class PlasmaStore(LoggerMixin):
    def __init__(
        self,
        socket_name: str = "/tmp/plasma",
        size: int = 1000000000,
        directory: str = "/tmp",
    ) -> None:
        self.socket_name = socket_name
        self.size = size
        self.directory = directory

        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def _start_plasma_store(self) -> None:
        if not self._running:
            try:
                self._process: subprocess.Popen = subprocess.Popen(
                    [
                        "plasma_store",
                        "-m",
                        f"{self.size}",
                        "-s",
                        f"{self.socket_name}",
                        "-d",
                        f"{self.directory}",
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except Exception as e:
                self.logger.exception(
                    f"Failed while starting PlasmaStore. Error: {e}"
                )
                raise e
            else:
                self._running = True
        else:
            self.logger.warning("PlasmaStore is already running!")

    def start(self) -> None:
        self._start_plasma_store()
        time.sleep(0.5)

    def stop(self) -> None:
        if self._running:
            # TODO: Blocking, consider timeout
            self._process.terminate()
            self._running = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class CustomPlasmaClient(LoggerMixin):
    def __init__(
        self,
        parent: bool,
        socket_name: str = "/tmp/plasma",
        size: int = 1000000000,
        directory: str = "/tmp",
    ) -> None:
        self._parent = parent
        self._socket_name = socket_name
        self._size = size
        self._directory = directory

        if self._parent:
            self._plasma_store = PlasmaStore(
                socket_name=socket_name, size=size, directory=directory
            )
            self._plasma_store.start()
            self.logger.info(
                f"PlasmaStore started. "
                f"Socket name: {socket_name}. Size: {size}"
            )
        else:
            self._plasma_store = None  # type: ignore

        self._plasma_client: plasma.PlasmaClient = plasma.connect(socket_name)
        self.logger.info(
            f"{'Main process' if parent else 'Worker'} "
            f"connected to the PlasmaStore"
        )

    @property
    def plasma_client(self) -> plasma.PlasmaClient:
        return self._plasma_client

    def __del__(self) -> None:
        if self._parent:
            self._plasma_store.stop()
            self.logger.info("PlasmaStore stopped")
