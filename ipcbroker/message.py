import random
import string


class Message:
    COM_ID_LENGTH = 15

    def __init__(self,
                 action: str,
                 payload,
                 com_id=None,
                 flags=None):
        if com_id is None:
            self.__com_id = ''.join([random.choice(string.ascii_uppercase)
                                     for _ in range(self.COM_ID_LENGTH)])
        else:
            self.__com_id = com_id

        if flags is None:
            flags = list()

        if not isinstance(flags, list):
            raise TypeError('flags is not a list')

        self.__action = action
        self.__payload = payload
        self.__flags = flags

    def __str__(self):
        ret_string = str(self.__com_id)
        ret_string += ': '
        ret_string += str(self.__action)
        ret_string += ' - '
        ret_string += str(self.__payload)
        return ret_string

    @property
    def com_id(self):
        return self.__com_id

    @property
    def action(self):
        return self.__action

    @property
    def payload(self):
        if isinstance(self.__payload, Exception):
            raise self.__payload
        return self.__payload

    @property
    def flags(self):
        return self.__flags
