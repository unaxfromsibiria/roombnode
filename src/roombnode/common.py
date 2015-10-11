'''
@author: Michael Vorotyntsev
@email: linkofwise@gmail.com
@github: unaxfromsibiria
'''
import codecs
import logging
import json

from enum import Enum


class ClientGroupEnum(Enum):
    Service = 1
    Server = 2
    Manager = 3


class Configuration(dict):
    _logger = None

    def __init__(self, **source):
        self.update(source)
        self._logger = logging.getLogger(self.get('logger'))

    @property
    def port(self):
        return self.get('port')

    @property
    def host(self):
        return self.get('host')

    @property
    def logger(self):
        return self._logger

    @property
    def is_completed(self):
        return True

    @property
    def worker_count(self):
        return int(self.get('workers') or 1)

    @property
    def manage_worker_count(self):
        return int(self.get('manage_workers') or 1)

    @property
    def reconnect_delay(self):
        return float(self.get('reconnect_delay') or 2)


class JsonFileConfiguration(Configuration):

    def __init__(self, path):
        with codecs.open(path) as conf:
            json_text = conf.read_all()
            data = json.loads(json_text)
            assert isinstance(data, dict)
            super().__init__(**data)
