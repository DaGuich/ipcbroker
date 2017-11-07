from multiprocessing import Pipe
from multiprocessing.connection import Connection
from multiprocessing.connection import wait
from threading import Lock
from threading import Thread
from time import sleep
from queue import Queue

from ipcbroker.message import Message


class Broker:
    POLL_TIMEOUT = 0.1

    def __init__(self):
        self.__client_connections = list()
        self.__registered_functions = dict()
        self.__message_queue = Queue()
        self.__return_queue = Queue()

        self.__thread = None
        self.__thread_lock = Lock()
        self.__thread_run = False

    def __del__(self):
        if self.__thread is not None:
            self.stop_loop()
        for con in self.__client_connections:
            con.close()

    def register_client(self):
        recv, send = Pipe(True)
        with self.__thread_lock:
            self.__client_connections.append(send)
        return recv

    def start_loop(self):
        if self.__thread is not None and self.__thread.is_alive():
            raise Exception('Thread already running')
        self.__thread = Thread(target=self.__loop(),
                               name='ipc-broker',
                               daemon=False)
        self._run_permission = True
        self.__thread.start()

    def stop_loop(self, timeout=5):
        self._run_permission = False
        try:
            self.__thread.join(timeout)
            self.__thread = None
        except EOFError:
            pass

    def __loop(self):
        def func():
            while self._run_permission:
                self.__poll()
                sleep(0.01)
        return func

    def __poll(self):
        recv_cons = wait(self.__client_connections, self.POLL_TIMEOUT)

        for recv_con in recv_cons:
            while recv_con.poll(self.POLL_TIMEOUT):
                message = recv_con.recv()
                if message.action == 'return':
                    self.__return_queue.put((recv_con, message))
                else:
                    self.__message_queue.put((recv_con, message))

        while not self.__message_queue.empty():
            client, message = self.__message_queue.get()
            if message.action == 'register_function':
                self.__register_function(client, message)
                continue
            elif message.action == 'close':
                self.__client_connections.remove(client)
                return_message = Message('close',
                                         None,
                                         message.com_id)
                client.send(return_message)
                continue
            self.__call_function(client, message)

    def __register_function(self,
                            client: Connection,
                            message: Message):
        name = message.payload
        if name in self.__registered_functions:
            exception = KeyError('Function already registered'),
            return_message = Message('return',
                                     exception,
                                     message.com_id)
            client.send(return_message)
        self.__registered_functions[name] = client
        return_message = Message('return',
                                 'OK',
                                 message.com_id)
        client.send(return_message)

    def __call_function(self,
                        client: Connection,
                        message: Message):
        name = message.action
        if name not in self.__registered_functions:
            exception = KeyError('Function not registered')
            return_msg = Message('return',
                                 exception,
                                 message.com_id)
            client.send(return_msg)

        func_client = self.__registered_functions[name]
        func_client.send(message)
        return_msg = func_client.recv()
        while (
            return_msg.com_id != message.com_id and
            return_msg.action != 'return'
        ):
            if return_msg.action == 'return':
                self.__return_queue.put(return_msg)
            else:
                self.__message_queue.put(return_msg)

            return_msg = func_client.recv()
        client.send(return_msg)

    @property
    def _run_permission(self):
        with self.__thread_lock:
            return self.__thread_run

    @_run_permission.setter
    def _run_permission(self, value):
        with self.__thread_lock:
            self.__thread_run = value

    @property
    def n_clients(self):
        with self.__thread_lock:
            return len(self.__client_connections)

    @property
    def n_functions(self):
        with self.__thread_lock:
            return len(self.__registered_functions)
