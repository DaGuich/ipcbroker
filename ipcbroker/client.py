from queue import Queue
from threading import Thread
from threading import Lock
from time import sleep
from itertools import count

from ipcbroker.broker import Broker
from ipcbroker.message import Message


class Client:
    POLL_TIMEOUT = 0.1
    __instance_counter = count(0)

    def __init__(self,
                 broker: Broker):
        self.__id = next(self.__instance_counter)

        if not isinstance(broker, Broker):
            raise TypeError('broker is not a Broker: {}'.format(type(broker)))

        self.__broker_con = broker.register_client()
        self.__message_queue = Queue()
        self.__registered_funcs = dict()

        self.__thread = None
        self.__thread_lock = Lock()
        self.__thread_run = False
        self.__thread_name = 'ipc-client-{}'.format(self.__id)

    def __call__(self, name, *args, **kwargs):
        if name not in self.__registered_funcs:
            return self.__remote_call(name)(*args, **kwargs)
        func = self.__registered_funcs[name]
        return func(*args, **kwargs)

    def __getattr__(self, item):
        if item in self.__registered_funcs:
            return self.__registered_funcs[item]
        return self.__remote_call(item)

    def register_function(self, name, callback):
        if name in self.__registered_funcs:
            raise KeyError('Function already locally registered')
        if not callable(callback):
            raise TypeError('Callback is not callable')
        message = Message('register_function',
                          name)
        self.__broker_con.send(message)
        return_message = self.__broker_con.recv()
        while (
            return_message.com_id != message.com_id and
            return_message.action != 'return'
        ):
            self.__message_queue.put(return_message)
            return_message = self.__broker_con.recv()
        if return_message.payload == 'OK':
            self.__registered_funcs[name] = callback
            return True
        else:
            return False

    def start_loop(self):
        if self.__thread is not None and self.__thread.is_alive():
            raise Exception('Thread {} already running'.format(self.__thread_name))
        self.__thread = Thread(target=self.__loop(),
                               name=self.__thread_name,
                               daemon=False)
        self.__thread.daemon = False
        self._run_permission = True
        self.__thread.start()

    def stop_loop(self, timeout=5):
        self.__wait_for_close()
        self._run_permission = False
        try:
            self.__thread.join(timeout)
            self.__thread = None
        except EOFError:
            pass

    def __remote_call(self, name):
        def func(*args, **kwargs):
            payload_dict = {'args': args,
                            'kwargs': kwargs}
            message = Message(name,
                              payload_dict)
            self.__broker_con.send(message)
            return_message = self.__broker_con.recv()
            while (
                return_message.action != 'return' and
                return_message.com_id != message.com_id
            ):
                self.__message_queue.put(return_message)
                return_message = self.__broker_con.recv()
            return return_message.payload
        return func

    def __loop(self):
        def func():
            while self._run_permission:
                self.__poll()
                sleep(0.01)
        return func

    def __process_message(self,
                          message: Message):
        if not isinstance(message.payload, dict):
            exc = TypeError('Payload is not argument dict')
            return_message = Message('return',
                                     exc,
                                     message.com_id)
            self.__broker_con.send(return_message)
            return

        if message.action not in self.__registered_funcs:
            exc = KeyError('Function not known')
            return_message = Message('return',
                                     exc,
                                     message.com_id)
            self.__broker_con.send(return_message)
            return

        if 'args' not in message.payload:
            message.payload['args'] = tuple()

        if 'kwargs' not in message.payload:
            message.payload['kwargs'] = dict()

        # return_value = self(message.action,
        #                     *message.payload['args'],
        #                     **message.payload['kwargs'])
        action_func = getattr(self, message.action)
        return_value = action_func(*message.payload['args'],
                                   **message.payload['kwargs'])
        return_message = Message('return',
                                 return_value,
                                 message.com_id)
        self.__broker_con.send(return_message)

    def __poll(self):
        while self.__broker_con.poll(self.POLL_TIMEOUT):
            try:
                message = self.__broker_con.recv()
                self.__message_queue.put(message)
            except (EOFError, OSError):
                pass

        while not self.__message_queue.empty():
            message = self.__message_queue.get()
            self.__process_message(message)

    def __wait_for_close(self):
        message = Message('close',
                          None)
        self.__broker_con.send(message)
        return_message = self.__broker_con.recv()
        while (
            message.com_id != return_message.com_id and
            return_message.action != 'close'
        ):
            self.__message_queue.put(return_message)
            return_message = self.__broker_con.recv()

    @property
    def _run_permission(self):
        with self.__thread_lock:
            return self.__thread_run

    @_run_permission.setter
    def _run_permission(self, value):
        with self.__thread_lock:
            self.__thread_run = value
