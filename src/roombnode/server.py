'''
@author: Michael Vorotyntsev
@email: linkofwise@gmail.com
@github: unaxfromsibiria
'''

import asyncio
import functools
import logging
import inspect
import os
import re
import signal
import string
import tempfile
import weakref
import uuid

from contextlib import contextmanager
from datetime import datetime, timedelta
from random import SystemRandom

from . import action
from .common import Configuration
from .transport import Command, CommandTarget, ServerAction, AbstractTransport

_COMMAND_FILE_EXT = 'cmd'


def sleep_method(delay):
    return asyncio.sleep(delay)


class BaseDataStorage:

    _runtime_id = None
    _logger = None
    _actual_timeout = 60
    _rand = None

    def __init__(self, server):
        self._runtime_id = str(uuid.uuid4())
        self._logger = server.logger
        self.setup(**server._conf)
        self._active = True
        self._rand = SystemRandom()

    def get_old_time(self):
        return datetime.now() - timedelta(seconds=self._actual_timeout)

    def get_random_key(self):
        return '{}_{}'.format(
            datetime.now().strftime('%Y%m%d%H%M%S%f'),
            ''.join(self._rand.choice(
                string.ascii_lowercase) for _ in range(8)))

    def setup(self, **options):
        pass

    def clear(self):
        raise NotImplementedError()

    def remove_by_key(self, key):
        raise NotImplementedError()

    def find_lost_data(self):
        """
        find data from lost process
        """
        raise NotImplementedError()

    def put(self, data):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def stop(self):
        self._active = False

    @asyncio.coroutine
    def auto_clear_worker(self):
        iter_timeout = 5
        step_iter_count = self._actual_timeout / iter_timeout
        step_index = 0

        while self._active:
            step_index += 1
            yield from sleep_method(iter_timeout)
            if step_index < step_iter_count:
                continue

            step_index = 0
            self._logger.info('Run auto clear storage...')
            try:
                result = self.clear()
            except Exception as err:
                self._logger.error(err)
            else:
                if result:
                    self._logger.info(
                        'storage clear result: {}'.format(result))


class TmpFileDataStorage(BaseDataStorage):
    """
    Default save server command into temp fs
    """
    tmp_dir = None
    _re_file = '(?P<datetime>\d{{14}})\d.+_\S{{8}}.{}'.format(_COMMAND_FILE_EXT)

    def setup(self, tmp_dir=None, **options):
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        actual_timeout = int(options.get('storage_actual_timeout') or 0)
        if actual_timeout > 0:
            self._actual_timeout = actual_timeout

        # check
        file_name = os.path.join(
            self.tmp_dir, '.check_{}'.format(self._runtime_id))
        try:
            self._logger.debug(
                '{} check file write {}'.format(self, file_name))
            with open(file_name, mode='w') as check_file:
                check_file.write('tmp')
            os.remove(file_name)
        except Exception as err:
            self._logger.error(err)
            raise

    def put(self, data):
        result = self.get_random_key()
        file_name = os.path.join(
            self.tmp_dir, '{}.{}'.format(result, _COMMAND_FILE_EXT))
        try:
            with open(file_name, mode='w') as tmp_file:
                tmp_file.write(data)
        except Exception as err:
            self._logger.error(err)
            result = None

        return result

    def remove_by_key(self, key):
        file_name = os.path.join(
            self.tmp_dir, '{}.{}'.format(key, _COMMAND_FILE_EXT))
        try:
            os.remove(file_name)
        except Exception as err:
            # auto clear do it
            self._logger.debug(err)
            pass

    def find_files(self, old=False):
        old_time = self.get_old_time()

        for _, _, files in os.walk(self.tmp_dir):
            for filename in files:
                find_date = re.search(self._re_file, filename)

                if find_date:
                    file_path = os.path.join(self.tmp_dir, filename)
                    target_file_date = datetime.strptime(
                        find_date.group('datetime'), '%Y%m%d%H%M%S')

                    if target_file_date < old_time and old:
                        yield (target_file_date, file_path)
                    elif not old:
                        yield (target_file_date, file_path)

    def clear(self):
        result = 0
        for __, old_file in self.find_files(old=True):
            os.remove(old_file)
            result += 1
        return result

    def find_lost_data(self):
        result = []
        for file_date, file_path in self.find_files():
            with open(file_path, encoding='utf-8') as cmd_file:
                data = cmd_file.read()
                if data:
                    result.append((file_date, data))
            os.remove(file_path)
        result.sort(key=lambda item: item[0], reverse=False)
        for __, file_path in result:
            yield file_path


class ConnectionClient(asyncio.Protocol):
    _transport = None
    _command_converter = {}
    _put_to_queue = None
    _logger_name = None

    def setup(self, server):
        self._put_to_queue = weakref.WeakMethod(server.command_queue.put_nowait)
        self._logger_name = server.logger.name

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        pass

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        put_method = self._put_to_queue()
        if put_method:
            put_method(data.decode("utf-8"))
        else:
            logger = logging.getLogger(self._logger_name)
            logger.error('Connection received data for closed server!')

    def eof_received(self):
        pass

    def send_command(self, command, options):
        cls = command.__class__
        converter = self._command_converter.get(cls)
        if not converter:
            converter = command.get_convertor(options)
            self._command_converter[cls] = converter

        for command_data in converter.dump(command):
            data = command_data.encode()
            print(data)
            self._transport.write(data)


class ServerSatateChanger:

    def __init__(self, state_data):
        assert isinstance(state_data, dict)
        self.state_data = state_data

    @property
    def client_id(self):
        return self.state_data.get('client_id')

    @client_id.setter
    def client_id(self, client_id):
        self.state_data.update(new_client_id=client_id)

    def send_command(self, command):
        assert self.state_data.get('command') is None, 'One command for one accept!'
        self.state_data.update(command=command)

    def option(self, name):
        return self.state_data['conf'].get(name)

    def log_error(self, msg):
        self.state_data['error'].append(msg)

    def log_info(self, msg):
        self.state_data['info'].append(msg)

    def get_server_state_info(self):
        # TODO:
        return {}

    def set_server_wait_mode(self, on=True):
        # TODO:
        pass


class ServerStateManager:

    _server = None

    def __init__(self, server):
        assert isinstance(server, BaseServer)
        self._server = weakref.ref(server)

    @property
    def server(self):
        return self._server()

    @contextmanager
    def accept(self):
        server = self.server
        state_data = {
            'info': [],
            'error': [],
        }
        if server:
            state_data.update(
                client_id=server._client_id,
                conf=server._conf)

        try:
            yield ServerSatateChanger(state_data)
        except Exception as err:
            server = self.server
            if server:
                server.logger.error(err)
        else:
            server = self.server
            if server:
                # save server data
                new_client_id = state_data.get('new_client_id')
                if new_client_id:
                    server._client_id = new_client_id

                for msg in state_data['error']:
                    server.logger.error(msg)

                for msg in state_data['info']:
                    server.logger.info(msg)

                command = state_data.get('command')
                if command and command.target:
                    if server._client_id:
                        command.cid = server._client_id
                    server._send_command(command)


class BaseActionPool:

    _action_command_cls = None
    _command_cls = None
    _convertor = None
    _actions = None
    _send_command = None
    _server_manager = None

    def __init__(self, server):

        assert issubclass(self._action_command_cls, AbstractTransport)
        assert issubclass(self._command_cls, AbstractTransport)
        self._convertor = self._action_command_cls.get_convertor(server._conf)
        self._actions = {}
        self._local_data = {}
        self._server_manager = ServerStateManager(server)

        for __, action_cls in inspect.getmembers(action, inspect.isclass):
            if issubclass(action_cls, action.BaseAction):
                if action.BaseAction is action_cls:
                    continue

                new_action = action_cls()
                assert isinstance(new_action, action.BaseAction)
                new_action.setup()
                for target in new_action.targets:
                    if target in self._actions:
                        self._actions[target].append(new_action)
                    else:
                        self._actions[target] = [new_action]

        for target in self._actions:
            self._actions[target].sort(key=lambda item: item.top, reverse=True)

    def execute(self, action_command_data, restore=False):
        action_command = self._convertor.loads(action_command_data)
        actions = self._actions.get(action_command.target)
        with self._server_manager.accept() as manager:
            if actions:
                command = self.create_command()
                prev_context = None
                for action in actions:
                    if action.processing(
                            manager, action_command, self._local_data, prev_context, command):
                        break
                    else:
                        prev_context = action.context
            else:
                command = self.create_quit_command(
                    msg='Not implimented action for target: {}'.format(
                        action_command.target))

            manager.send_command(command)

    def create_command(self, **data):
        return self._command_cls(**data)

    def create_quit_command(self, msg='quit'):
        return self._command_cls(
            target=CommandTarget.Quit, data=msg)


class ActionPool(BaseActionPool):
    _action_command_cls = ServerAction
    _command_cls = Command


class BaseServer:

    _conf = None
    _loop = None
    _active = True
    _connected = False
    _client_tcp = None
    _need_close = False
    _client_id = None
    logger = None
    command_queue = None
    storage = action_pool = None

    _STD_ITER_DELAY = 1

    def __init__(self, conf):
        assert isinstance(conf, Configuration)
        assert conf.is_completed
        self._conf = conf
        self._active = False
        self.logger = conf.logger
        storage_cls = conf.get('storage_cls') or TmpFileDataStorage
        assert issubclass(storage_cls, BaseDataStorage)
        self.storage = storage_cls(self)

        action_pool_cls = conf.get('action_pool_cls') or ActionPool
        assert issubclass(action_pool_cls, BaseActionPool)
        self.action_pool = action_pool_cls(self)

    @property
    def active(self):
        return self._active

    def close_if_need(self):
        if self._need_close:
            if self.command_queue.empty():
                self._active = False
            else:
                self.logger.warn('Wait command queue cleaning..')
            self.storage.stop()

    @asyncio.coroutine
    def _node_connection(self):
        while not self._connected:
            try:
                self._client_tcp = yield from self._loop.create_connection(
                    ConnectionClient, self._conf.host, self._conf.port)
            except OSError:
                self.logger.info('try connection...')
                yield from self.sleep_method(self._conf.reconnect_delay)
            else:
                self._active = True
                self._connected = True

    def _close_signal_receiver(self, signame):
        self.logger.info('Close signal {}, wait out'.format(signame))
        self._need_close = True

    def _hello_server(self):
        command = self.action_pool.create_command(target=CommandTarget.Sigin)
        self._send_command(command)

    def _send_command(self, command):
        self._client_tcp[1].send_command(command, self._conf)

    def _create_workers(self):

        times = {
            'delay': self._STD_ITER_DELAY,
            'manager_delay': self._STD_ITER_DELAY / 2.0,
        }
        misc_flags = {
            'need_find_command': True,
        }
        self.logger.debug('times: {}'.format(times))

        @asyncio.coroutine
        def worker(index, server):
            while server.active:
                yield from sleep_method(times['delay'])
                # TODO: check call queue
                server.logger.info('todo {}'.format(index))

        @asyncio.coroutine
        def command_manager(server):

            def move_to_processing(server_command, storage_key=None, lost=False):
                try:
                    server.action_pool.execute(server_command, restore=lost)
                except Exception as err:
                    server.logger.error(err)
                finally:
                    if not lost:
                        server.storage.remove_by_key(storage_key)

            lost_commands = None
            if misc_flags.get('need_find_command') and server.active:
                misc_flags.update(need_find_command=False)
                try:
                    lost_commands = list(server.storage.find_lost_data())
                except Exception as err:
                    server.logger.error(err)
                else:
                    if lost_commands:
                        self.logger.warn(
                            'Has data of {} lost commands'.format(len(lost_commands)))

            while server.active:
                if lost_commands:
                    for command_data in lost_commands:
                        move_to_processing(command_data, lost=True)
                    lost_commands = None

                elif server.command_queue.empty():
                    yield from sleep_method(times['manager_delay'])
                else:
                    new_command_data = yield from server.command_queue.get()
                    storage_key = server.storage.put(new_command_data)
                    if storage_key is None:
                        # big fail, return command after delay
                        yield from sleep_method(times['delay'])
                        server.command_queue.put_nowait(new_command_data)
                        server.logger.warn(
                            'Return command to queue, some problem :(')
                    else:
                        move_to_processing(new_command_data, storage_key)

                server.close_if_need()

        workers = [
            asyncio.async(worker(w_index, self), loop=self._loop)
            for w_index in range(1, self._conf.worker_count + 1)
        ]
        for __ in range(self._conf.manage_worker_count):
            workers.append(asyncio.async(command_manager(self), loop=self._loop))

        workers.append(asyncio.async(self.storage.auto_clear_worker(), loop=self._loop))
        return workers

    def run(self):
        self.logger.info('Connect to {host}:{port}'.format(**self._conf))
        loop = asyncio.get_event_loop()
        self._loop = loop
        self._need_close = False
        self._client_tcp = None
        self._connected = False
        loop.run_until_complete(self._node_connection())

        for signame in ('SIGINT', 'SIGTERM'):
            signal_id = getattr(signal, signame)
            loop.add_signal_handler(
                signal_id,
                functools.partial(self._close_signal_receiver, signame))

        self.command_queue = asyncio.Queue(loop=loop)
        self._client_tcp[1].setup(self)
        self._hello_server()
        workers = self._create_workers()
        loop.run_until_complete(asyncio.wait(workers))
        loop.close()
