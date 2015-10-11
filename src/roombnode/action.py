'''
@author: Michael Vorotyntsev
@email: linkofwise@gmail.com
@github: unaxfromsibiria
'''

import json
import string
import sys
from random import SystemRandom
from hashlib import sha1

from .common import ClientGroupEnum
from .helpers import MetaOnceObject
from .transport import ActionTarget, CommandTarget


class BaseAction:

    __metaclass__ = MetaOnceObject

    _targets = None
    _option = None

    top = 0

    @property
    def targets(self):
        return self._targets

    def __init__(self):
        self.context = {}

    def setup(self):
        pass

    def processing(
            self,
            server_manager,
            action_command,
            local_data,
            perv_context,
            result_command):
        """
        Terminate sequence return True
        """
        return True


class QuitAction(BaseAction):

    _targets = [
        ActionTarget.Quit,
    ]

    def processing(self, server_manager, action_command, local_data, perv_context, command):
        server_manager.log_info('closing connection completed')
        return True


class AuthAction(BaseAction):

    _targets = [
        ActionTarget.VerificationRequest,
    ]

    def setup(self):
        self.rand = SystemRandom()
        self.variants = string.digits + string.ascii_letters

    def processing(self, server_manager, action_command, local_data, perv_context, command):
        command.target = CommandTarget.Auth
        command.cid = action_command.cid
        key = action_command.data
        size = len(key)
        client_solt = ''.join(
            self.rand.choice(self.variants) for _ in range(size))
        content = '{}{}{}'.format(
            client_solt, key, server_manager.option('security_key'))
        content = sha1(bytes(content, 'utf-8')).hexdigest()
        command.data = '{}{}'.format(client_solt, content)
        return True


class ServerErrorAction(BaseAction):

    top = sys.maxsize

    _targets = [
        ActionTarget.VerificationRequest,
        ActionTarget.Wait,
        ActionTarget.WhoAreYou,
        ActionTarget.Error,
    ]

    def processing(self, server_manager, action_command, local_data, perv_context, command):
        has_error = action_command.target == ActionTarget.Error
        if has_error:
            server_manager.log_error(
                'Close connection, server return error: {}'.format(action_command.data))
            command.target = CommandTarget.Quit
            command.cid = action_command.cid
        return has_error


class SendInfoAction(BaseAction):

    _targets = [
        ActionTarget.WhoAreYou,
    ]

    def processing(self, server_manager, action_command, local_data, perv_context, command):
        client_id = server_manager.client_id
        # was here
        if not client_id:
            # take server connection id for server client id
            client_id = action_command.cid
            server_manager.client_id = client_id
            server_manager.log_info(
                'New server client id (cid): {}'.format(client_id))

        command.target = CommandTarget.Clientdata
        command.cid = action_command.cid
        # setup ClientDescription
        data = {
            'group': ClientGroupEnum.Server.value,
            'cid': client_id,
        }
        command.data = json.dumps(data)
        return True


class StartWaitAction(BaseAction):

    _targets = [
        ActionTarget.Wait,
    ]

    def processing(self, server_manager, action_command, local_data, perv_context, command):
        command.target = CommandTarget.ClientState
        data = server_manager.get_server_state_info()
        command.data = json.dumps(data)
        server_manager.set_server_wait_mode(True)
        return True
