import typing as t


class Message:
    pass


class StopMessage(Message):
    pass


class TaskMessage(Message):
    def __init__(self, index: int, func: t.Callable, *args, **kwargs):
        self.index = index
        self.func = func
        self.args = args
        self.kwargs = kwargs
