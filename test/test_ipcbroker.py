from unittest import TestCase

# from ipcbroker.broker import Broker
# from ipcbroker.client import Client
from ipcbroker import Broker, Client


def add(a, b):
    return a + b


def sub(a, b):
    return a - b


class IpcBrokerTestCase(TestCase):
    def setUp(self):
        self.broker = Broker()
        self.broker.start_loop()

    def tearDown(self):
        self.broker.stop_loop()
        self.broker = None

    def test_client_parameter_not_broker(self):
        with self.assertRaises(TypeError):
            client = Client('example')

    def test_register_client(self):
        ca = Client(self.broker)
        ca.start_loop()
        self.assertEqual(self.broker.n_clients, 1)
        cb = Client(self.broker)
        cb.start_loop()
        self.assertEqual(self.broker.n_clients, 2)
        cb.stop_loop()
        ca.stop_loop()

    def test_register_function(self):
        ca = Client(self.broker)
        ca.start_loop()
        success = ca.register_function('add', add)
        self.assertTrue(success)
        self.assertEqual(self.broker.n_functions, 1)
        ca.stop_loop()

    def test_call_function_local(self):
        ca = Client(self.broker)
        ca.start_loop()
        ca.register_function('add', add)
        self.assertEqual(ca.add(1, 2), 3)
        ca.stop_loop()

    def test_call_function_remote(self):
        ca = Client(self.broker)
        cb = Client(self.broker)
        ca.start_loop()
        cb.start_loop()
        success = ca.register_function('add', add)
        self.assertTrue(success)
        success = cb.register_function('sub', sub)
        self.assertTrue(success)
        self.assertEqual(cb.add(1, 2), 3)
        self.assertEqual(ca.sub(5, 2), 3)
        cb.stop_loop()
        ca.stop_loop()


