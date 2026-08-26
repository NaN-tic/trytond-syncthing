from trytond.pool import Pool

from . import syncthing


def register():
    Pool.register(
        syncthing.Category,
        syncthing.SyncEntry,
        syncthing.User,
        syncthing.Group,
        syncthing.Rule,
        syncthing.Device,
        syncthing.Configuration,
        module='syncthing', type_='model')
