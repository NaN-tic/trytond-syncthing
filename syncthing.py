from trytond.exceptions import UserError
from trytond.i18n import gettext
from trytond.model import (
    DeactivableMixin, ModelSingleton, ModelSQL, ModelView, Unique, fields)
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Bool, Eval
from trytond.transaction import Transaction, without_check_access


class Tag(metaclass=PoolMeta):
    __name__ = 'brainbow.tag'

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
    def validate_fields(cls, tags, field_names):
        super().validate_fields(tags, field_names)
        if not field_names or field_names & {'parent', 'sync', 'syncthing'}:
            for tag in tags:
                if tag.syncthing and (tag.parent or not tag.sync):
                    raise UserError(gettext(
                            'syncthing.msg_syncthing_synchronized_root'))

    @classmethod
    def create(cls, vlist):
        tags = super().create(vlist)
        if not Transaction().context.get('syncthing_skip_queue'):
            Pool().get('syncthing.configuration').queue_synchronize()
        return tags

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
    def delete(cls, tags):
        super().delete(tags)
        if not Transaction().context.get('syncthing_skip_queue'):
            Pool().get('syncthing.configuration').queue_synchronize()


class User(metaclass=PoolMeta):
    __name__ = 'res.user'

    syncthing_devices = fields.One2Many(
        'syncthing.device', 'user', "Syncthing Devices")

    @classmethod
    def __setup__(cls):
        super().__setup__()
        cls._preferences_fields.append('syncthing_devices')

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
