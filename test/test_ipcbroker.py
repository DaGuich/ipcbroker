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
        self.broker = Broker('broker').start()

    def tearDown(self):
        self.broker.stop()
        self.broker = None

    def test_client_parameter_not_broker(self):
        with self.assertRaises(TypeError):
            client = Client('example', 'tcpnb_client')

    def test_register_client(self):
        ca = Client(self.broker, 'trc_ca').start()
        self.assertEqual(self.broker.n_clients, 1)
        cb = Client(self.broker, 'trc_cb').start()
        self.assertEqual(self.broker.n_clients, 2)
        self.assertTrue(ca.is_alive())
        self.assertTrue(cb.is_alive())
        ca.stop()
        cb.stop()

    def test_register_function(self):
        ca = Client(self.broker, 'trf_ca').start()
        success = ca.register_function('add', add)
        self.assertTrue(success)
        self.assertEqual(self.broker.n_functions, 1)
        ca.stop()

    def test_call_function_local(self):
        ca = Client(self.broker, 'tcfl_ca').start()
        ca.register_function('add', add)
        self.assertEqual(ca.add(1, 2), 3)
        self.assertTrue(ca.is_alive())
        ca.stop()

    def test_call_function_remote(self):
        ca = Client(self.broker, 'tcfr_ca').start()
        cb = Client(self.broker, 'tcfr_cb').start()
        success = ca.register_function('add', add)
        self.assertTrue(success)
        success = cb.register_function('sub', sub)
        self.assertTrue(success)
        self.assertEqual(cb.add(1, 2), 3)
        self.assertEqual(ca.sub(5, 2), 3)
        ca.stop()
        cb.stop()


