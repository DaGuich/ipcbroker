import time
import multiprocessing
import ipcbroker


class BrokerProcess(multiprocessing.Process):
    def __init__(self, brk):
        super().__init__()
        self.broker = brk
        self.stop_event = multiprocessing.Event()

    def run(self):
        while not self.stop_event.is_set():
            self.broker.work()


class ClientAProcess(multiprocessing.Process):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.stop_event = multiprocessing.Event()

    def run(self):
        self.client.register_function('sub', self.sub())
        time.sleep(5)
        print(self.client.add(4, 2))
        time.sleep(10)

    def sub(self):
        def method(a, b):
            return a - b
        return method


class ClientBProcess(multiprocessing.Process):
    def __init__(self, client):
        super().__init__()
        self.client = client
        self.next_event = multiprocessing.Event()

    def run(self):
        self.client.register_function('add', self.add())
        while not self.next_event.is_set():
            self.client.work()

    def add(self):
        def method(a, b):
            return a + b
        return method


if __name__ == '__main__':
    broker = ipcbroker.Broker()
    client_a = ipcbroker.Client(broker)
    client_b = ipcbroker.Client(broker)

    bp = BrokerProcess(broker)
    cap = ClientAProcess(client_a)
    cbp = ClientBProcess(client_b)
    bp.start()
    cbp.start()
    cap.start()
    print('All started')
    cap.join()
    print('cap joined')
    cbp.next_event.set()
    bp.stop_event.set()
    cbp.join()
    print('cbp joined')
    bp.join()
    print('bp joined')
