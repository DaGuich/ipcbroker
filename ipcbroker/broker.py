from multiprocessing import Pipe
from multiprocessing.connection import Connection
from multiprocessing.connection import wait
from queue import Queue

from ipcbroker.message import Message


class Broker:
    POLL_TIMEOUT = 0.1

    def __init__(self):
        self.__client_connections = list()
        self.__registered_functions = dict()
        self.__message_queue = Queue()
        self.__return_queue = Queue()

    def __del__(self):
        for con in self.__client_connections:
            con.close()

    def register_client(self):
        """
        Register a client at the broker

        :return: a multiprocessing connection to communicate with the broker
        """
        recv, send = Pipe(True)
        self.__client_connections.append(send)
        return recv

    def work(self):
        # read connections
        recv_cons = wait(self.__client_connections, self.POLL_TIMEOUT)

        # fill message queue
        for recv_con in recv_cons:
            while recv_con.poll(self.POLL_TIMEOUT):
                message = recv_con.recv()
                if message.action == 'return':
                    self.__return_queue.put((recv_con, message))
                else:
                    self.__message_queue.put((recv_con, message))

        # process messages in queue
        while not self.__message_queue.empty():
            client, message = self.__message_queue.get()
            self.__process_message(client, message)

    def __process_message(self, client, message):
        if message.action == 'register_function':
            self.__register_function(client, message)
            return
        self.__call_function(client, message)

    def __register_function(self,
                            client: Connection,
                            message: Message):
        # function name is in payload
        name = message.payload

        # check if function is already registered
        if name in self.__registered_functions:
            exception = KeyError('Function already registered'),
            return_message = Message('return',
                                     exception,
                                     message.com_id)
            client.send(return_message)
            return

        # register function and send OK back
        self.__registered_functions[name] = client
        return_message = Message('return',
                                 'OK',
                                 message.com_id)
        client.send(return_message)

    def __call_function(self,
                        client: Connection,
                        message: Message):
        # function name in action field
        name = message.action

        # check if function is registered
        if name not in self.__registered_functions:
            # send back KeyError (AttributeError better?)
            exception = KeyError('Function not registered')
            return_msg = Message('return',
                                 exception,
                                 message.com_id)
            client.send(return_msg)
            return

        # get the appropriate method connection and send the request
        func_client = self.__registered_functions[name]
        func_client.send(message)

        # wait for return
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
    def n_clients(self):
        return len(self.__client_connections)

    @property
    def n_functions(self):
        return len(self.__registered_functions)
