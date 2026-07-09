# Copyright 2017, Inderpreet Singh, All rights reserved.

from typing import Type, Callable, Optional
from abc import ABC, abstractmethod
import time

import bottle
from bottle import static_file

from common import Context
from controller import Controller


class IHandler(ABC):
    """
    Abstract class that defines a web handler
    """

    @abstractmethod
    def add_routes(self, web_app: "WebApp"):
        """
        Add all the handled routes to the given web app
        :param web_app:
        :return:
        """
        pass


class IStreamHandler(ABC):
    """
    Abstract class that defines a streaming data provider
    """

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def get_value(self) -> str | None:
        pass

    @abstractmethod
    def cleanup(self):
        pass

    @classmethod
    def register(cls, web_app: "WebApp", **kwargs):
        """
        Register this streaming handler with the web app
        :param web_app: web_app instance
        :param kwargs: args for stream handler ctor
        :return:
        """
        web_app.add_streaming_handler(cls, **kwargs)


class WebApp(bottle.Bottle):
    """
    Web app implementation
    """

    _STREAM_POLL_INTERVAL_IN_MS = 100
    # Emit a heartbeat comment if no real event has been sent for this long, so a
    # dead/disconnected SSE client is detected promptly (the write raises, unwinding
    # the generator and releasing its worker thread + listener queues) instead of
    # the connection lingering and leaking a thread from the fixed server pool.
    _STREAM_HEARTBEAT_INTERVAL_IN_S = 15

    def __init__(self, context: Context, controller: Controller):
        super().__init__()
        self.logger = context.logger.getChild("WebApp")
        self.__controller = controller
        self.__html_path = context.args.html_path
        self.__status = context.status
        self.logger.info("Html path set to: {}".format(self.__html_path))
        self._stop = False
        self.__api_key: str = getattr(context.config.web, 'api_key', '') or ''
        self.__streaming_handlers: list[tuple[Type[IStreamHandler], dict]] = []

    def add_default_routes(self):
        """
        Add the default routes. This must be called after all the handlers have
        been added.
        :return:
        """
        # Streaming route
        self.get("/server/stream")(self.__web_stream)

        # Front-end routes
        self.route("/")(self.__index)
        self.route("/dashboard")(self.__index)
        self.route("/settings")(self.__index)
        self.route("/autoqueue")(self.__index)
        self.route("/logs")(self.__index)
        self.route("/about")(self.__index)
        # For static files
        self.route("/<file_path:path>")(self.__static)

    def add_handler(self, path: str, handler: Callable):
        self.get(path)(handler)

    def add_streaming_handler(self, handler: Type[IStreamHandler], **kwargs):
        self.__streaming_handlers.append((handler, kwargs))

    def process(self):
        """
        Advance the web app state
        :return:
        """
        pass

    def stop(self):
        """
        Exit gracefully, kill any connections and clean up any state
        :return:
        """
        object.__setattr__(self, '_stop', True)

    def __index(self):
        """
        Serves the index.html static file
        :return:
        """
        return self.__static("index.html")

    # noinspection PyMethodMayBeStatic
    def __static(self, file_path: str):
        """
        Serves all the static files
        :param file_path:
        :return:
        """
        return static_file(file_path, root=self.__html_path)

    def __web_stream(self):
        # Initialize all the handlers
        handlers = [cls(**kwargs) for (cls, kwargs) in self.__streaming_handlers]

        try:
            # Setup the response header
            bottle.response.content_type = "text/event-stream"
            bottle.response.cache_control = "no-cache"

            # Call setup on all handlers
            for handler in handlers:
                handler.setup()

            # Get streaming values until the connection closes
            last_activity = time.time()
            while not self._stop:
                sent = False
                for handler in handlers:
                    # Process all values from this handler
                    while True:
                        value = handler.get_value()
                        if value:
                            yield value
                            sent = True
                        else:
                            break

                now = time.time()
                if sent:
                    last_activity = now
                elif now - last_activity >= WebApp._STREAM_HEARTBEAT_INTERVAL_IN_S:
                    # No real events for a while: send an SSE comment as a heartbeat.
                    # If the client is gone, this write fails and unwinds the
                    # generator (finally -> cleanup), freeing the worker thread.
                    yield ": ping\n\n"
                    last_activity = now

                time.sleep(WebApp._STREAM_POLL_INTERVAL_IN_MS / 1000)

        finally:
            self.logger.debug("Stream connection stopped by {}".format("server" if self._stop else "client"))

            # Cleanup all handlers
            for handler in handlers:
                handler.cleanup()
