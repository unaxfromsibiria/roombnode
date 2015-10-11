'''
@author: Michael Vorotyntsev
@email: linkofwise@gmail.com
@github: unaxfromsibiria
'''

import json
from enum import Enum


def enum_as_int(enum_value=None):
    return getattr(enum_value, 'value', 0)


class TransportConvertor:
    _cls = None
    _fields = {}
    _data_size_limit = 2 ** 10

    def __init__(self, cls, connection_buffer_size, **options):
        assert issubclass(cls, AbstractTransport)
        self._cls = cls
        for field_name, convert_method in cls.get_fields():
            assert callable(convert_method)
            if field_name in self._fields:
                self._fields[field_name].append(convert_method)
            else:
                self._fields[field_name] = [convert_method]

        try:
            self._data_size_limit = int(connection_buffer_size)
        except (ValueError, TypeError):
            pass

    def dump(self, record):
        obj = {}
        data = None

        for field, methods in self._fields.items():
            value = getattr(record, field, None)
            for method in methods:
                if issubclass(method, Enum):
                    convert = enum_as_int
                else:
                    convert = method

                try:
                    if field == 'data':
                        data = convert(value)
                    else:
                        obj[field] = convert(value)
                except (TypeError, ValueError):
                    continue
                else:
                    break

        if data is None:
            for __ in range(1):
                yield json.dumps(obj)
        else:
            record_count = 1
            limit = self._data_size_limit
            if len(data) > limit:
                record_count = len(data) / limit + 1

            for index in range(record_count):
                next_obj = dict(**obj)
                if 'part' in next_obj:
                    next_obj['part'] = record_count > 1
                if record_count > 1:
                    next_obj['data'] = data[index * limit:(index + 1) * limit]
                else:
                    next_obj['data'] = data
                yield json.dumps(next_obj)

    def loads(self, data):
        data = json.loads(data)
        record = self._cls()
        if isinstance(data, dict):
            for field, methods in self._fields.items():
                for convert in methods:
                    try:
                        record.set_value(field, convert(data.get(field)))
                    except (ValueError, TypeError):
                        continue
                    else:
                        break

        return record


class AbstractTransport:

    def __init__(self):
        raise NotImplementedError()

    def set_value(self, field, value):
        setattr(self, field, value)

    @classmethod
    def get_fields(cls):
        """
        [('field1', str), ('field2', int)]
        :rtype list:
        """
        raise NotImplementedError()

    @classmethod
    def get_convertor(cls, options):
        return TransportConvertor(cls, **options)


class CommandTarget(Enum):
    Unknown = 0
    Quit = 1
    Sigin = 2
    Auth = 3
    Clientdata = 4
    ClientState = 5


class ActionTarget(Enum):
    Unknown = 0
    Quit = 1
    Skip = 2
    VerificationRequest = 3
    Error = 4
    WhoAreYou = 5
    Wait = 6
    TakeCuid = 7


class Command(AbstractTransport):
    target = CommandTarget.Unknown
    part = False
    cid = None
    data = None

    def __init__(self, target=None, data=None, cid=None, part=False):
        self.target = target
        self.cid = cid or ''
        self.data = data or ''
        self.part = part

    @classmethod
    def get_fields(cls):
        return (
            ('target', CommandTarget),
            ('part', bool),
            ('cid', str),
            ('data', str),
        )


class ServerAction(AbstractTransport):
    target = ActionTarget.Unknown
    cid = None
    data = None

    def __init__(self, target=ActionTarget.Unknown, data=None, cid=None):
        self.target = target
        self.cid = cid or ''
        self.data = data or ''

    @classmethod
    def get_fields(cls):
        return (
            ('target', ActionTarget),
            ('cid', str),
            ('data', str),
        )
