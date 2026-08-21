import copy
import tempfile
import unittest
from unittest.mock import patch

from proteus import Model
import trytond.config as tryton_config
from trytond.pool import Pool
from trytond.tests.test_tryton import drop_db
from trytond.tests.tools import activate_modules
from trytond.transaction import Transaction

from trytond.modules.syncthing.service import (
    SyncthingClient, SyncthingService)


class TestConfigurationSync(unittest.TestCase):

    def setUp(self):
        drop_db()
        super().setUp()

    def tearDown(self):
        drop_db()
        super().tearDown()

    def test(self):
        if not tryton_config.has_section('file_sync'):
            tryton_config.add_section('file_sync')
        tryton_config.set('queue', 'worker', 'true')

        with tempfile.TemporaryDirectory() as directory:
            tryton_config.set('file_sync', 'path', directory)

            config = activate_modules('syncthing')
            Tag = Model.get('brainbow.tag', config=config)
            User = Model.get('res.user', config=config)

            reader = User(name='Reader', login='reader')
            reader.save()
            writer = User(name='Writer', login='writer')
            writer.save()

            root = Tag(name='Shared', sync=True, syncthing=True)
            root.read_only_users.append(reader)
            root.read_write_users.append(writer)
            root.save()

            with Transaction().start(
                    config.database_name, reader.id,
                    context=config.context) as transaction:
                Device = Pool(config.database_name).get('syncthing.device')
                Device.create([{
                            'name': 'Reader laptop',
                            'device_id': 'READER-DEVICE-ID',
                            'addresses': 'dynamic',
                            }])
                transaction.commit()
            with Transaction().start(
                    config.database_name, writer.id,
                    context=config.context) as transaction:
                Device = Pool(config.database_name).get('syncthing.device')
                Device.create([{
                            'name': 'Writer workstation',
                            'device_id': 'WRITER-DEVICE-ID',
                            'addresses': 'dynamic\ntcp://writer.test:22000',
                            }])
                transaction.commit()

            with Transaction().start(
                    config.database_name, config.user,
                    context=config.context):
                pool = Pool(config.database_name)
                Queue = pool.get('ir.queue')
                Configuration = pool.get('syncthing.configuration')
                configuration, = Configuration.search([])
                tasks = [task for task in Queue.search([
                            ('finished_at', '=', None),
                            ])
                    if task.data['model'] == 'syncthing.configuration'
                    and task.data['method'] == 'synchronize']
                self.assertGreaterEqual(len(tasks), 3)
                self.assertTrue(all(
                        tuple(task.data['instances']) == (configuration.id,)
                        for task in tasks))

            calls = []

            def reconcile(client, role, folders, devices, marker):
                calls.append((
                        client.url, role, copy.deepcopy(folders),
                        copy.deepcopy(devices), marker))

            with patch.multiple(
                    'trytond.modules.syncthing.service',
                    SEND_ONLY_URL='http://send-only.test',
                    SEND_ONLY_API_KEY='read-key',
                    SEND_RECEIVE_URL='http://send-receive.test',
                    SEND_RECEIVE_API_KEY='write-key'), \
                    patch.object(SyncthingClient, 'reconcile', reconcile):
                with Transaction().start(
                        config.database_name, config.user,
                        context=config.context) as transaction:
                    SyncthingService().synchronize()
                    transaction.commit()

            self.assertEqual({call[1] for call in calls},
                {'sendonly', 'sendreceive'})
            send_only, = [call for call in calls if call[1] == 'sendonly']
            send_receive, = [
                call for call in calls if call[1] == 'sendreceive']
            self.assertEqual(set(send_only[3]), {'READER-DEVICE-ID'})
            self.assertEqual(set(send_receive[3]), {'WRITER-DEVICE-ID'})
            send_only_folder, = send_only[2].values()
            send_receive_folder, = send_receive[2].values()
            self.assertEqual(
                send_only_folder['remote_device_ids'], {'READER-DEVICE-ID'})
            self.assertEqual(
                send_receive_folder['remote_device_ids'],
                {'WRITER-DEVICE-ID'})
            self.assertEqual(send_only_folder['path'],
                send_receive_folder['path'])
            self.assertTrue(send_only_folder['path'].endswith('/Shared'))

            with Transaction().start(
                    config.database_name, config.user,
                    context=config.context):
                ServerUser = Pool(config.database_name).get('res.user')
                self.assertIn(
                    'syncthing_devices', ServerUser._preferences_fields)

            state = {
                '/rest/config/devices': [
                    {
                        'deviceID': 'UNMANAGED',
                        'name': 'Manual device',
                        'addresses': ['dynamic'],
                    }, {
                        'deviceID': 'STALE',
                        'name': 'tryton-test-old device',
                        'addresses': ['dynamic'],
                    }],
                '/rest/config/folders': [
                    {
                        'id': 'manual-folder',
                        'label': 'Manual',
                        'path': '/manual',
                        'type': 'sendreceive',
                        'devices': [{'deviceID': 'LOCAL'}],
                    }, {
                        'id': 'tryton-test-old',
                        'label': 'Stale',
                        'path': '/stale',
                        'type': 'sendonly',
                        'devices': [{'deviceID': 'LOCAL'}],
                    }],
                '/rest/config/defaults/device': {
                    'deviceID': '',
                    'name': '',
                    'addresses': ['dynamic'],
                    'compression': 'metadata',
                    },
                '/rest/config/defaults/folder': {
                    'id': '',
                    'label': '',
                    'path': '',
                    'type': 'sendreceive',
                    'devices': [],
                    },
                '/rest/system/status': {'myID': 'LOCAL'},
                }
            requests = []

            def request(method, endpoint, payload=None):
                requests.append((method, endpoint, copy.deepcopy(payload)))
                if method == 'PUT':
                    state[endpoint] = copy.deepcopy(payload)
                    return None
                return copy.deepcopy(state[endpoint])

            client = SyncthingClient.__new__(SyncthingClient)
            client.request = request
            desired_folders = {
                'tryton-test-1': {
                    'label': 'Shared',
                    'path': '/srv/files/Shared',
                    'remote_device_ids': {'REMOTE'},
                    'fsWatcherEnabled': True,
                    },
                }
            desired_devices = {
                'REMOTE': {
                    'name': 'tryton-test-Reader laptop',
                    'addresses': ['dynamic'],
                    },
                }
            client.reconcile(
                'sendonly', desired_folders, desired_devices,
                'tryton-test-')
            put_requests = [request for request in requests
                if request[0] == 'PUT']
            self.assertEqual(len(put_requests), 2)
            self.assertIn('UNMANAGED', {
                    device['deviceID']
                    for device in state['/rest/config/devices']})
            self.assertNotIn('STALE', {
                    device['deviceID']
                    for device in state['/rest/config/devices']})
            managed_folder, = [folder
                for folder in state['/rest/config/folders']
                if folder['id'] == 'tryton-test-1']
            self.assertEqual(managed_folder['type'], 'sendonly')
            self.assertEqual(
                {device['deviceID'] for device in managed_folder['devices']},
                {'LOCAL', 'REMOTE'})

            requests.clear()
            client.reconcile(
                'sendonly', desired_folders, desired_devices,
                'tryton-test-')
            self.assertFalse([request for request in requests
                    if request[0] == 'PUT'])
