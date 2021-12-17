import typing as t

from pyarrow import plasma


class Message:
    pass


class StopMessage(Message):
    pass


class TaskMessage(Message):
    def __init__(
        self,
        index: int,
        func: t.Callable,
        plasma_object_id: plasma.ObjectID,
        indices: t.Tuple[int, int],
        *args,
        **kwargs
    ) -> None:
        self.index = index
        self.func = func
        self.object_id = plasma_object_id
        self.slice_indices = indices
        self.args = args
        self.kwargs = kwargs
