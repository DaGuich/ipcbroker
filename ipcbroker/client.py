from queue import Queue
from threading import Lock

from ipcbroker.broker import Broker
from ipcbroker.message import Message
from ipcbroker.threaded import Threaded


class Client(Threaded):
    POLL_TIMEOUT = 0.1

    def __init__(self,
                 broker: Broker,
                 name=None):
        if name is None:
            super().__init__()
        else:
            super().__init__(name)
        if not isinstance(broker, Broker):
            raise TypeError('broker is not a Broker: {}'.format(type(broker)))

        self.__broker_con = broker.register_client()
        self.__message_queue = Queue()
        self.__registered_funcs = dict()

        self.__connection_lock = Lock()

    def __call__(self, name, *args, **kwargs):
        if name not in self.__registered_funcs:
            return self.__remote_call(name)(*args, **kwargs)
        func = self.__registered_funcs[name]
        return func(*args, **kwargs)

    def __getattr__(self, item):
        # check if method is locally registered
        if item in self.__registered_funcs:
            return self.__registered_funcs[item]

        # if not do a remote call via the broker
        return self.__remote_call(item)

    def work(self):
        # check if new message has arrived
        try:
            with self.__connection_lock:
                while self.__broker_con.poll(self.POLL_TIMEOUT):
                    message = self.__broker_con.recv()
                    self.__message_queue.put(message)
        except (EOFError, OSError):
            pass

        # process messages in message queue
        while not self.__message_queue.empty():
            message = self.__message_queue.get()
            self.__process_message(message)

    def register_function(self, name, callback, long_running=False):
        # check if the function is already locally registered
        if name in self.__registered_funcs:
            raise KeyError('Function already locally registered')

        # check if the callback is a callable
        if not callable(callback):
            raise TypeError('Callback is not callable')

        with self.__connection_lock:
            if long_running:
                message = Message('register_function',
                                  name,
                                  flags=['long_running'])
            else:
                message = Message('register_function',
                                  name)
            # send register request to broker
            self.__broker_con.send(message)

            # wait for response
            return_message = self.__broker_con.recv()
            while (
                return_message.com_id != message.com_id and
                return_message.action != 'return'
            ):
                self.__message_queue.put(return_message)
                return_message = self.__broker_con.recv()

            # if response says OK return True otherwise False
            if return_message.payload == 'OK':
                self.__registered_funcs[name] = callback
                return True
            else:
                return False

    def __remote_call(self, name):
        """
        Return a function to call a remote client method

        :param name: name of the method
        :return: remote handler
        """
        def func(*args, **kwargs):
            # fill the dictionary with the args and kwargs for
            # the remote method
            payload_dict = {'args': args,
                            'kwargs': kwargs}

            with self.__connection_lock:
                # send request to broker
                message = Message(name,
                                  payload_dict)
                self.__broker_con.send(message)

                # wait for response
                return_message = self.__broker_con.recv()
                while (
                    return_message.action != 'return' and
                    return_message.com_id != message.com_id
                ):
                    self.__message_queue.put(return_message)
                    return_message = self.__broker_con.recv()

                # return the payload
                return return_message.payload
        return func

    def __process_message(self,
                          message: Message):
        # check if the message payload is dict with arguments
        # args and kwargs
        # if not required use empty dict
        if not isinstance(message.payload, dict):
            exc = TypeError('Payload is not argument dict')
            return_message = Message('return',
                                     exc,
                                     message.com_id)
            self.__broker_con.send(return_message)
            return

        # check if requested method is registered
        if message.action not in self.__registered_funcs:
            exc = KeyError('Function not known')
            return_message = Message('return',
                                     exc,
                                     message.com_id)
            self.__broker_con.send(return_message)
            return

        # if args not in payload dict add it
        if 'args' not in message.payload:
            message.payload['args'] = tuple()

        # if kwargs not in payload dict add it
        if 'kwargs' not in message.payload:
            message.payload['kwargs'] = dict()

        # call method
        action_func = getattr(self, message.action)
        return_value = action_func(*message.payload['args'],
                                   **message.payload['kwargs'])

        # and return result
        return_message = Message('return',
                                 return_value,
                                 message.com_id)
        self.__broker_con.send(return_message)
