# Python IPC-Broker

| Branch | Status |
| ------ | ------ |
| master | [![Codacy Badge](https://api.codacy.com/project/badge/Grade/a58ccc60625b437491ff5e523cad3f65)](https://www.codacy.com/app/matthias.gilch.mg/ipcbroker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=DaGuich/ipcbroker&amp;utm_campaign=Badge_Grade)|

The IPC-Broker manages the communication between python multiprocessing processes. It provides an interface
to call methods that are provided in another process. The usage is very simple.

```python
import ipcbroker

# in main process
broker = ipcbroker.Broker().start()

# create clients before starting all processes
alice = ipcbroker.Client(broker)
bob = ipcbroker.Client(broker)
```
```python 
import time
# in child process (with a client as parameter)
def add(a, b):
    return a + b

alice.start()    
alice.register_function('add', add) 
# wait for registering of other process
time.sleep(2)

# call own add function
alice.add(2, 3)     # returns 5

# or bobs sub function
alice.sub(5, 2)     # return 3
```

```python
import time
# in another child process
def sub(a, b):
    return a - b

bob.start()
bob.register_function('sub', sub)
time.sleep(2)
# call alice's add function
bob.add(2, 3)       # returns 5


```