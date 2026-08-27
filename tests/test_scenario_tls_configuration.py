import hashlib
import ssl
import unittest
from unittest.mock import patch
from urllib.error import URLError

from trytond.modules.syncthing import service
from trytond.modules.syncthing.service import (
    SyncthingClient, SyncthingError)
from trytond.tests.test_tryton import drop_db


class TestTLSConfiguration(unittest.TestCase):

    def setUp(self):
        drop_db()
        super().setUp()

    def tearDown(self):
        drop_db()
        super().tearDown()

    def test(self):
        certificate = b'self-signed server certificate'
        fingerprint = hashlib.sha256(certificate).hexdigest()
        formatted_fingerprint = 'sha256:' + ':'.join(
            fingerprint[index:index + 2].upper()
            for index in range(0, len(fingerprint), 2))
        connections = []

        class Socket:

            def getpeercert(self, binary_form=False):
                if not binary_form:
                    raise AssertionError('certificate must use DER format')
                return certificate

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b'{"myID": "SERVER-ID"}'

        class Connection:

            def __init__(self, host, port, timeout=None, context=None):
                self.host = host
                self.port = port
                self.timeout = timeout
                self.context = context
                self.sock = Socket()
                self.requests = []
                connections.append(self)

            def connect(self):
                pass

            def request(self, method, target, body=None, headers=None):
                self.requests.append((method, target, body, headers))

            def getresponse(self):
                return Response()

            def close(self):
                pass

        with patch(
                'trytond.modules.syncthing.service.HTTPSConnection',
                Connection):
            client = SyncthingClient(
                'https://syncthing.test:8384', 'secret',
                formatted_fingerprint)
            self.assertEqual(
                client.request('GET', '/rest/system/status'),
                {'myID': 'SERVER-ID'})
            self.assertEqual(connections[-1].host, 'syncthing.test')
            self.assertEqual(connections[-1].port, 8384)
            request, = connections[-1].requests
            self.assertEqual(request[:3], (
                    'GET', '/rest/system/status', None))
            self.assertEqual(request[3]['X-API-Key'], 'secret')

            with self.assertRaisesRegex(
                    SyncthingError, 'fingerprint mismatch'):
                SyncthingClient(
                    'https://syncthing.test:8384', 'secret',
                    '00' * 32).request('GET', '/rest/system/status')
            self.assertFalse(connections[-1].requests)

            verification_error = ssl.SSLCertVerificationError(
                1, 'self-signed certificate')
            with patch(
                    'trytond.modules.syncthing.service.urlopen',
                    side_effect=URLError(verification_error)), \
                    patch.object(service.logger, 'error') as log:
                with self.assertRaises(SyncthingError):
                    SyncthingClient(
                        'https://syncthing.test:8384', 'secret').request(
                            'GET', '/rest/system/status')
            log.assert_called_once()
            self.assertIn(formatted_fingerprint, str(log.call_args))
            self.assertFalse(connections[-1].requests)

        with self.assertRaisesRegex(
                SyncthingError, 'Invalid Syncthing certificate fingerprint'):
            SyncthingClient(
                'https://syncthing.test:8384', 'secret', 'not-a-sha256')

        connections.clear()
        with patch(
                'trytond.modules.syncthing.service.VERIFY_SSL', False), \
                patch(
                    'trytond.modules.syncthing.service.HTTPSConnection',
                    Connection), \
                patch(
                    'trytond.modules.syncthing.service.urlopen',
                    return_value=Response()) as urlopen, \
                patch.object(service.logger, 'warning') as warning:
            client = SyncthingClient(
                'https://syncthing.test:8384', 'secret')
            self.assertEqual(client.ssl_context.verify_mode, ssl.CERT_NONE)
            self.assertFalse(client.ssl_context.check_hostname)
            self.assertEqual(
                client.request('GET', '/rest/system/status'),
                {'myID': 'SERVER-ID'})
            self.assertEqual(
                client.request('GET', '/rest/system/status'),
                {'myID': 'SERVER-ID'})
            warning.assert_called_once()
            self.assertIn(
                formatted_fingerprint, str(warning.call_args))
            self.assertEqual(urlopen.call_count, 2)
            self.assertEqual(len(connections), 1)
            self.assertFalse(connections[0].requests)
