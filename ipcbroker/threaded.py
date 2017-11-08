from time import sleep
from threading import Thread
from threading import Event


class Threaded:
    def __init__(self, name=None):
        self.__threaded_name = name
        self.__threaded_thread = None
        self.__threaded_stop_event = Event()

    def work(self):
        pass

    def __thread_worker(self):
        while not self.__threaded_stop_event.is_set():
            self.work()
            sleep(0.01)

    def start(self):
        self.__threaded_stop_event.clear()
        if (
            self.__threaded_thread is not None and
            self.__threaded_thread.is_alive()
        ):
            raise Exception('Thread already alive')
        if self.__threaded_name is None:
            self.__threaded_thread = Thread(target=self.__thread_worker)
        else:
            self.__threaded_thread = Thread(target=self.__thread_worker,
                                            name=self.__threaded_name)
        self.__threaded_thread.start()
        return self

    def stop(self):
        if (
            self.__threaded_thread is None or
            not self.__threaded_thread.is_alive()
        ):
            raise Exception('Thread is already stoped')
        self.__threaded_stop_event.set()
        self.__threaded_thread.join()
        return self

    def is_alive(self):
        return self.__threaded_thread.is_alive()
