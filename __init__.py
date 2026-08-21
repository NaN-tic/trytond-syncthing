from trytond.pool import Pool

from . import syncthing


def register():
    Pool.register(
        syncthing.Tag,
        syncthing.User,
        syncthing.Group,
        syncthing.Device,
        syncthing.Configuration,
        module='syncthing', type_='model')
