import copy
import hashlib
import json
import logging
import ssl
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import trytond.config as config
from trytond.pool import Pool
from trytond.transaction import Transaction, without_check_access

from trytond.modules.file_sync.sync import Synchronizer


SEND_ONLY_URL = config.get('syncthing', 'send_only_url')
SEND_ONLY_API_KEY = config.get('syncthing', 'send_only_api_key')
SEND_RECEIVE_URL = config.get('syncthing', 'send_receive_url')
SEND_RECEIVE_API_KEY = config.get('syncthing', 'send_receive_api_key')
TIMEOUT = config.getfloat('syncthing', 'timeout', default=10)
VERIFY_SSL = config.getboolean('syncthing', 'verify_ssl', default=True)
CA_FILE = config.get('syncthing', 'ca_file')

logger = logging.getLogger(__name__)


class SyncthingError(Exception):
    pass


class SyncthingClient:

    def __init__(self, url, api_key):
        self.url = url.rstrip('/')
        self.api_key = api_key
        self.timeout = TIMEOUT
        self.ssl_context = None
        if self.url.startswith('https://'):
            if VERIFY_SSL:
                self.ssl_context = ssl.create_default_context(cafile=CA_FILE)
            else:
                self.ssl_context = ssl.create_default_context()
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

    def reconcile(self, role, desired_folders, desired_devices, marker):
        current_devices = self.request('GET', '/rest/config/devices')
        current_folders = self.request('GET', '/rest/config/folders')
        default_device = self.request(
            'GET', '/rest/config/defaults/device')
        default_folder = self.request(
            'GET', '/rest/config/defaults/folder')
        status = self.request('GET', '/rest/system/status')
        local_device_id = status['myID']

        devices_by_id = {
            device['deviceID']: device for device in current_devices}
        reconciled_devices = [
            device for device in current_devices
            if not device.get('name', '').startswith(marker)
            and device.get('deviceID') not in desired_devices
            ]
        for device_id, values in sorted(desired_devices.items()):
            device = copy.deepcopy(
                devices_by_id.get(device_id, default_device))
            device.update(values)
            device['deviceID'] = device_id
            reconciled_devices.append(device)

        folders_by_id = {
            folder['id']: folder for folder in current_folders}
        reconciled_folders = [
            folder for folder in current_folders
            if not folder.get('id', '').startswith(marker)
            and folder.get('id') not in desired_folders
            ]
        for folder_id, values in sorted(desired_folders.items()):
            folder = copy.deepcopy(
                folders_by_id.get(folder_id, default_folder))
            folder.update(values)
            folder['id'] = folder_id
            folder['type'] = role
            remote_ids = sorted(values['remote_device_ids'])
            current_folder_devices = {
                device['deviceID']: device
                for device in folder.get('devices', [])}
            folder['devices'] = [copy.deepcopy(
                    current_folder_devices.get(
                        device_id, {'deviceID': device_id}))
                for device_id in [local_device_id] + remote_ids]
            folder.pop('remote_device_ids', None)
            reconciled_folders.append(folder)

        reconciled_devices.sort(key=lambda item: item['deviceID'])
        reconciled_folders.sort(key=lambda item: item['id'])
        if reconciled_devices != sorted(
                current_devices, key=lambda item: item['deviceID']):
            self.request(
                'PUT', '/rest/config/devices', reconciled_devices)
        if reconciled_folders != sorted(
                current_folders, key=lambda item: item['id']):
            self.request(
                'PUT', '/rest/config/folders', reconciled_folders)

    def request(self, method, endpoint, payload=None):
        data = None
        headers = {
            'Accept': 'application/json',
            'X-API-Key': self.api_key,
            }
        if payload is not None:
            data = json.dumps(payload).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        request = Request(
            self.url + endpoint, data=data, headers=headers, method=method)
        try:
            with urlopen(
                    request, timeout=self.timeout,
                    context=self.ssl_context) as response:
                body = response.read()
        except HTTPError as exception:
            detail = exception.read().decode('utf-8', errors='replace')
            raise SyncthingError(
                f'Syncthing {method} {endpoint} returned '
                f'{exception.code}: {detail}') from exception
        except URLError as exception:
            raise SyncthingError(
                f'Syncthing {method} {endpoint} failed: '
                f'{exception.reason}') from exception
        if not body:
            return None
        return json.loads(body.decode('utf-8'))


class SyncthingService:

    def synchronize(self):
        with without_check_access():
            self._synchronize()

    def _synchronize(self):
        Tag = Pool().get('brainbow.tag')
        Device = Pool().get('syncthing.device')
        roots = Tag.search([
                ('parent', '=', None),
                ('active', '=', True),
                ('sync', '=', True),
                ('syncthing', '=', True),
                ])
        devices = Device.search([('active', '=', True)])
        devices_by_user = {}
        for device in devices:
            devices_by_user.setdefault(device.user, []).append(device)

        database = Transaction().database.name
        database_token = hashlib.sha256(
            database.encode('utf-8')).hexdigest()[:10]
        marker = f'tryton-{database_token}-'
        synchronizer = Synchronizer(required=True)
        desired = {
            'sendonly': ({}, {}),
            'sendreceive': ({}, {}),
            }

        for root in roots:
            read_write, read_only = root.access_users()
            folder_id = f'{marker}{root.id}'
            path = synchronizer._tag_directory(root, root)
            for role, users in [
                    ('sendonly', read_only),
                    ('sendreceive', read_write),
                    ]:
                desired_folders, desired_devices = desired[role]
                remote_device_ids = set()
                for user in users:
                    if not user.active:
                        continue
                    for device in devices_by_user.get(user, []):
                        remote_device_ids.add(device.device_id)
                        desired_devices[device.device_id] = {
                            'name': (
                                f'{marker}{user.rec_name}: {device.name}'),
                            'addresses': self._addresses(device),
                            }
                desired_folders[folder_id] = {
                    'label': root.rec_name,
                    'path': path,
                    'remote_device_ids': remote_device_ids,
                    'fsWatcherEnabled': True,
                    }

        servers = [
            ('sendonly', SEND_ONLY_URL, SEND_ONLY_API_KEY),
            ('sendreceive', SEND_RECEIVE_URL, SEND_RECEIVE_API_KEY),
            ]
        for role, url, key in servers:
            if not url or not key:
                logger.info(
                    'Syncthing server %s is not configured; skipping', role)
                continue
            folders, server_devices = desired[role]
            SyncthingClient(url, key).reconcile(
                role, folders, server_devices, marker)

    @staticmethod
    def _addresses(device):
        addresses = [line.strip() for line in (device.addresses or '').splitlines()
            if line.strip()]
        return addresses or ['dynamic']
