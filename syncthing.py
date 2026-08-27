from trytond.exceptions import UserError
from trytond.i18n import gettext
from trytond.model import (
    DeactivableMixin, ModelSingleton, ModelSQL, ModelView, Unique, fields)
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Bool, Eval
from trytond.transaction import Transaction, without_check_access


class Category(metaclass=PoolMeta):
    __name__ = 'office.category'

    syncthing = fields.Boolean(
        "Syncthing",
        states={
            'invisible': Bool(Eval('parent')) | ~Bool(Eval('sync')),
            },
        depends=['parent', 'sync'])

    @staticmethod
    def default_syncthing():
        return False

    @classmethod
    def validate_fields(cls, categories, field_names):
        super().validate_fields(categories, field_names)
        if not field_names or field_names & {'parent', 'sync', 'syncthing'}:
            for category in categories:
                if category.syncthing and (category.parent or not category.sync):
                    raise UserError(gettext(
                            'syncthing.msg_syncthing_synchronized_root'))

    @classmethod
    def create(cls, vlist):
        categories = super().create(vlist)
        if not Transaction().context.get('syncthing_skip_queue'):
            Pool().get('syncthing.configuration').queue_synchronize()
        return categories

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        pairs = list(zip(actions, actions))
        super().write(*args)
        if Transaction().context.get('syncthing_skip_queue'):
            return
        watched = {
            'name', 'parent', 'active', 'sync', 'syncthing',
            'read_only_groups', 'read_write_groups',
            'read_only_users', 'read_write_users',
            }
        if any(watched & set(values) for _, values in pairs):
            Pool().get('syncthing.configuration').queue_synchronize()

    @classmethod
    def delete(cls, categories):
        super().delete(categories)
        if not Transaction().context.get('syncthing_skip_queue'):
            Pool().get('syncthing.configuration').queue_synchronize()


class SyncEntry(metaclass=PoolMeta):
    __name__ = 'file.sync.entry'

    @classmethod
    def __setup__(cls):
        super().__setup__()
        cls._file_sync_ignore_patterns = (
            cls._file_sync_ignore_patterns | {
                '.stfolder',
                '.stignore',
                '.stversions',
                '.syncthing.*',
                })


class User(metaclass=PoolMeta):
    __name__ = 'res.user'

    syncthing_read_only_device_id = fields.Function(
        fields.Char("Read-only Syncthing Device ID"),
        'get_syncthing_server_device_ids')
    syncthing_read_write_device_id = fields.Function(
        fields.Char("Read-write Syncthing Device ID"),
        'get_syncthing_server_device_ids')
    syncthing_devices = fields.One2Many(
        'syncthing.device', 'user', "Syncthing Devices")

    @classmethod
    def __setup__(cls):
        super().__setup__()
        cls._preferences_fields.extend([
                'syncthing_read_only_device_id',
                'syncthing_read_write_device_id',
                'syncthing_devices',
                ])

    @classmethod
    def get_preferences_fields_view(cls):
        # The preferences view makes fields writable even when Function fields
        # have no setter, so restore their natural read-only state.
        view = super().get_preferences_fields_view()
        for name in {
                'syncthing_read_only_device_id',
                'syncthing_read_write_device_id',
                }:
            view['fields'][name]['readonly'] = True
        return view

    @classmethod
    def get_syncthing_server_device_ids(cls, users, names):
        from .service import (
            SEND_ONLY_API_KEY, SEND_ONLY_CERTIFICATE_FINGERPRINT,
            SEND_ONLY_URL, SEND_RECEIVE_API_KEY,
            SEND_RECEIVE_CERTIFICATE_FINGERPRINT, SEND_RECEIVE_URL,
            SyncthingClient, SyncthingError)

        servers = {
            'syncthing_read_only_device_id': (
                SEND_ONLY_URL, SEND_ONLY_API_KEY,
                SEND_ONLY_CERTIFICATE_FINGERPRINT),
            'syncthing_read_write_device_id': (
                SEND_RECEIVE_URL, SEND_RECEIVE_API_KEY,
                SEND_RECEIVE_CERTIFICATE_FINGERPRINT),
            }
        result = {name: {user.id: None for user in users} for name in names}
        for name in names:
            url, api_key, fingerprint = servers[name]
            if not url or not api_key:
                continue
            try:
                device_id = SyncthingClient(
                    url, api_key, fingerprint).request(
                    'GET', '/rest/system/status')['myID']
            except (KeyError, SyncthingError):
                continue
            for user in users:
                result[name][user.id] = device_id
        return result

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        pairs = list(zip(actions, actions))
        super().write(*args)
        if (not Transaction().context.get('syncthing_skip_queue')
                and any({'groups', 'active'} & set(values)
                    for _, values in pairs)):
            Pool().get('syncthing.configuration').queue_synchronize()


class Group(metaclass=PoolMeta):
    __name__ = 'res.group'

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        pairs = list(zip(actions, actions))
        super().write(*args)
        if (not Transaction().context.get('syncthing_skip_queue')
                and any({'users'} & set(values) for _, values in pairs)):
            Pool().get('syncthing.configuration').queue_synchronize()


class Rule(metaclass=PoolMeta):
    __name__ = 'ir.rule'

    @classmethod
    def _get_context(cls, model_name):
        context = super()._get_context(model_name)
        if model_name == 'syncthing.device':
            context['user_id'] = Transaction().user
        return context

    @classmethod
    def _get_cache_key(cls, model_names):
        key = super()._get_cache_key(model_names)
        if 'syncthing.device' in model_names:
            key = (*key, Transaction().user)
        return key


class Device(DeactivableMixin, ModelSQL, ModelView):
    "Syncthing Device"
    __name__ = 'syncthing.device'

    user = fields.Many2One(
        'res.user', "User", required=True, ondelete='CASCADE',
        states={
            'readonly': Eval('id', 0) > 0,
            })
    device_id = fields.Char("Device ID", required=True)
    name = fields.Char("Name", required=True)
    addresses = fields.Text(
        "Addresses",
        help="One Syncthing address per line. Use 'dynamic' for discovery.")

    @classmethod
    def __setup__(cls):
        super().__setup__()
        table = cls.__table__()
        cls._sql_constraints += [
            ('device_id_unique', Unique(table, table.device_id),
                'syncthing.msg_device_id_unique'),
            ]

    @staticmethod
    def default_user():
        return Transaction().user

    @staticmethod
    def default_addresses():
        return 'dynamic'

    @classmethod
    def create(cls, vlist):
        devices = super().create(vlist)
        cls._queue_configuration()
        return devices

    @classmethod
    def write(cls, *args):
        super().write(*args)
        cls._queue_configuration()

    @classmethod
    def delete(cls, devices):
        super().delete(devices)
        cls._queue_configuration()

    @classmethod
    def _queue_configuration(cls):
        if not Transaction().context.get('syncthing_skip_queue'):
            Pool().get('syncthing.configuration').queue_synchronize()


class Configuration(ModelSingleton, ModelSQL, ModelView):
    "Syncthing Configuration"
    __name__ = 'syncthing.configuration'

    @classmethod
    def queue_synchronize(cls):
        with without_check_access():
            configurations = cls.search([], limit=1)
        if configurations:
            cls.__queue__.synchronize(configurations)

    @classmethod
    def synchronize(cls, configurations):
        # The singleton is a persistent target for the asynchronous task.
        from .service import SyncthingService
        SyncthingService().synchronize()
