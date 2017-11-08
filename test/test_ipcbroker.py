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

    def tearDown(self):
        self.broker = None

    def test_client_parameter_not_broker(self):
        with self.assertRaises(TypeError):
            client = Client('example')

    def test_register_client(self):
        ca = Client(self.broker)
        self.assertEqual(self.broker.n_clients, 1)
        cb = Client(self.broker)
        self.assertEqual(self.broker.n_clients, 2)

    def test_register_function(self):
        ca = Client(self.broker)
        success = ca.register_function('add', add)
        self.assertTrue(success)
        self.assertEqual(self.broker.n_functions, 1)

    def test_call_function_local(self):
        ca = Client(self.broker)
        ca.register_function('add', add)
        self.assertEqual(ca.add(1, 2), 3)

    def test_call_function_remote(self):
        ca = Client(self.broker)
        cb = Client(self.broker)
        success = ca.register_function('add', add)
        self.assertTrue(success)
        success = cb.register_function('sub', sub)
        self.assertTrue(success)
        self.assertEqual(cb.add(1, 2), 3)
        self.assertEqual(ca.sub(5, 2), 3)


