# Copyright 2017, Inderpreet Singh, All rights reserved.

from queue import Queue, Empty, Full
from typing import TypeVar, Generic, Optional


T = TypeVar('T')


class StreamQueue(Generic[T]):
    """
    A queue that transfers events from one thread to another.
    Useful for web streams that wait for listener events from other threads.
    The producer thread calls put() to insert events. The consumer stream
    calls get_next_event() to receive event in its own thread.

    The queue is bounded: if a consumer stalls (e.g. a dead SSE client not yet
    reaped), the oldest events are dropped rather than growing memory without
    bound. Dropped events are harmless in practice — a reconnecting client
    receives a fresh full snapshot.
    """
    __MAX_SIZE = 10000

    def __init__(self):
        self.__queue = Queue(maxsize=StreamQueue.__MAX_SIZE)

    def put(self, event: T):
        try:
            self.__queue.put(event, block=False)
        except Full:
            # Drop the oldest event to make room, then enqueue the newest.
            try:
                self.__queue.get(block=False)
            except Empty:
                pass
            try:
                self.__queue.put(event, block=False)
            except Full:
                pass

    def get_next_event(self) -> T | None:
        """
        Returns the next event if there is one, otherwise returns None
        :return:
        """
        try:
            return self.__queue.get(block=False)
        except Empty:
            return None


from bottle import HTTPResponse

# Maximum allowed length for filenames passed to controller actions
MAX_FILENAME_LEN = 4096


def check_length(value: str, max_len: int, label: str) -> "HTTPResponse | None":
    """
    Validate that a string does not exceed max_len characters.
    Returns a 400 HTTPResponse if the check fails, otherwise None.
    """
    if len(value) > max_len:
        return HTTPResponse(
            body="{} exceeds maximum length of {} characters".format(label, max_len),
            status=400
        )
    return None
