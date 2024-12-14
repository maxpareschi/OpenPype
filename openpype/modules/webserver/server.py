import os
import re
import threading
import asyncio

from aiohttp import web

from openpype.lib import Logger
from .cors_middleware import cors_middleware


class WebServerManager:
    """Manger that care about web server thread."""

    def __init__(self, port=None, host=None):
        self._log = None

        self.port = port or 8079
        self.host = host or "localhost"

        self.client = None
        self.handlers = {}
        self.on_stop_callbacks = []

        self.app = web.Application(
            middlewares=[
                cors_middleware(
                    origins=[re.compile(r"^https?\:\/\/localhost")]
                )
            ]
        )

        # add route with multiple methods for single "external app"

        self.webserver_thread = WebServerThread(self)

    @property
    def log(self):
        if self._log is None:
            self._log = Logger.get_logger(self.__class__.__name__)
        return self._log

    @property
    def url(self):
        return "{}:{}".format(self.host, self.port)

    def add_route(self, *args, **kwargs):
        self.app.router.add_route(*args, **kwargs)

    def add_static(self, *args, **kwargs):
        self.app.router.add_static(*args, **kwargs)

    def start_server(self):
        if self.webserver_thread and not self.webserver_thread.is_alive():
            self.webserver_thread.start()

    def stop_server(self):
        if not self.is_running:
            return
        try:
            self.log.debug("Stopping Web server")
            self.webserver_thread.is_running = False
            self.webserver_thread.stop()

        except Exception:
            self.log.warning(
                "Error has happened during Killing Web server",
                exc_info=True
            )

    @property
    def is_running(self):
        if not self.webserver_thread:
            return False
        return self.webserver_thread.is_running

    def thread_stopped(self):
        for callback in self.on_stop_callbacks:
            callback()


class WebServerThread(threading.Thread):
    """ Listener for requests in thread."""

    def __init__(self, manager):
        self._log = None

        super(WebServerThread, self).__init__()

        self.is_running = False
        self.manager = manager
        self.loop = None
        self.runner = None
        self.site = None
        self.tasks = []

    @property
    def log(self):
        if self._log is None:
            self._log = Logger.get_logger(self.__class__.__name__)
        return self._log

    @property
    def port(self):
        return self.manager.port

    @property
    def host(self):
        return self.manager.host

    def run(self):
        self.is_running = True

        try:
            self.log.info("Starting WebServer server")
            self.loop = asyncio.new_event_loop()  # create new loop for thread
            asyncio.set_event_loop(self.loop)

            self.loop.run_until_complete(self.start_server())

            self.log.debug(
                "Running Web server on URL: \"{}:{}\"".format(self.host, self.port)
            )

            asyncio.ensure_future(self.check_shutdown(), loop=self.loop)
            self.loop.run_forever()

        except Exception:
            self.log.warning(
                "Web Server service has failed", exc_info=True
            )
        finally:
            self.loop.close()  # optional

        self.is_running = False
        self.manager.thread_stopped()
        self.log.info("Web server stopped")

    async def start_server(self):
        """ Starts runner and TCPsite """
        self.runner = web.AppRunner(self.manager.app)
        await self.runner.setup()

        ssl_cert = os.environ.get("OPENPYPE_WEBSERVER_SSL_CERT", None)
        ssl_key = os.environ.get("OPENPYPE_WEBSERVER_SSL_KEY", None)

        print((f"  - {{ FtrackWebServer }}: [  Detected host: '{self.host}', "
               f"Detected port: '{self.port}'  ]"))

        if not (ssl_cert and ssl_key):
            self.site = web.TCPSite(self.runner, self.host, self.port)
            await self.site.start()
        else:
            import ssl
            print("  - { FtrackWebServer }: [  SSL enabled.  ]")
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
            # ssl_context.load_verify_locations(Path(cert_dir, "22dogsCA.crt").as_posix())
            # ssl_context.verify_mode = ssl.CERT_REQUIRED
            self.site = web.TCPSite(self.runner, self.host, self.port, ssl_context=ssl_context)
            await self.site.start()

    def stop(self):
        """Sets is_running flag to false, 'check_shutdown' shuts server down"""
        self.is_running = False

    async def check_shutdown(self):
        """ Future that is running and checks if server should be running
            periodically.
        """
        while self.is_running:
            while self.tasks:
                task = self.tasks.pop(0)
                self.log.debug("waiting for task {}".format(task))
                await task
                self.log.debug("returned value {}".format(task.result))

            await asyncio.sleep(0.5)

        self.log.debug("Starting shutdown")
        await self.site.stop()
        self.log.debug("Site stopped")
        await self.runner.cleanup()
        self.log.debug("Runner stopped")
        tasks = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
        ]
        list(map(lambda task: task.cancel(), tasks))  # cancel all the tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.log.debug(
            f'Finished awaiting cancelled tasks, results: {results}...'
        )
        await self.loop.shutdown_asyncgens()
        # to really make sure everything else has time to stop
        await asyncio.sleep(0.07)
        self.loop.stop()
