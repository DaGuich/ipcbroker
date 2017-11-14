import logging
from multiprocessing import Lock
from multiprocessing import Pipe
from multiprocessing.connection import Connection
from multiprocessing.connection import wait
from queue import Queue

from ipcbroker.message import Message
from ipcbroker.threaded import Threaded


class Broker(Threaded):
    POLL_TIMEOUT = 0.1

    def __init__(self,
                 name=None):
        if name is not None:
            super().__init__(name=name)
        else:
            super().__init__()
        self.__client_connections = list()
        self.__registered_functions = dict()
        self.__message_queue = Queue()
        self.__return_queue = Queue()

        self.__connection_lock = Lock()

        self.__logger = logging.getLogger(__name__)

    def register_client(self):
        """
        Register a client at the broker

        :return: a multiprocessing connection to communicate with the broker
        """
        recv, send = Pipe(True)
        self.__client_connections.append(send)
        return recv

    def work(self):
        with self.__connection_lock:
            # read connections
            recv_cons = wait(self.__client_connections, self.POLL_TIMEOUT)

            # fill message queue
            for recv_con in recv_cons:
                while recv_con.poll(self.POLL_TIMEOUT):
                    try:
                        message = recv_con.recv()
                        if message.action == 'return':
                            self.__return_queue.put((recv_con, message))
                        else:
                            self.__message_queue.put((recv_con, message))
                    except (EOFError, OSError):
                        self.__logger.error('Error receiving from pipe')
                        break

        qsize = self.__message_queue.qsize()
        if qsize > 0:
            self.__logger.debug('{} messages queued'.format(qsize))
            # process messages in queue
            while not self.__message_queue.empty():
                client, message = self.__message_queue.get()
                self.__process_message(client, message)

    def __process_message(self, client, message):
        if message.action == 'register_function':
            self.__register_function(client, message)
            return
        elif message.action == 'close':
            self.__client_connections.remove(client)
            return
        self.__call_function(client, message)

    def __register_function(self,
                            client: Connection,
                            message: Message):
        # function name is in payload
        name = message.payload

        long_running = False
        if 'long_running' in message.flags:
            long_running = True

        # check if function is already registered
        if name in self.__registered_functions:
            exception = KeyError('Method already registered'),
            return_message = Message('return',
                                     exception,
                                     message.com_id)
            client.send(return_message)
            return

        # register function and send OK back
        self.__registered_functions[name] = {
            'client': client,
            'long_running': long_running
        }
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
            exception = AttributeError('No such method registered')
            return_msg = Message('return',
                                 exception,
                                 message.com_id)
            client.send(return_msg)
            return

        # get the appropriate method connection and send the request
        func_client = self.__registered_functions[name]['client']
        long_running = self.__registered_functions[name]['long_running']
        func_client.send(message)

        poll_timeout = self.POLL_TIMEOUT * 10

        try:
            with self.__connection_lock:
                # wait for return
                if not long_running and not func_client.poll(poll_timeout):
                    raise Exception('No response')
                return_msg = func_client.recv()
                while (
                    return_msg.com_id != message.com_id and
                    return_msg.action != 'return'
                ):
                    if return_msg.action == 'return':
                        self.__return_queue.put(return_msg)
                    else:
                        self.__message_queue.put(return_msg)

                    if not long_running and not func_client.poll(poll_timeout):
                        raise Exception('No response')
                    return_msg = func_client.recv()
                client.send(return_msg)
        except Exception as exception:
            exc_message = Message('return',
                                  exception,
                                  message.com_id)
            client.send(exc_message)

    @property
    def n_clients(self):
        return len(self.__client_connections)

    @property
    def n_functions(self):
        return len(self.__registered_functions)
