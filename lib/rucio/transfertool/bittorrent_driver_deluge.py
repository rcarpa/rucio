# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import logging
import socket
import ssl
import struct
import warnings
from typing import TYPE_CHECKING

import zlib
from deluge.transfer import rencode

from rucio.common.utils import resolve_ip
from rucio.db.sqla.constants import RequestState
from rucio.transfertool.transfertool import TransferStatusReport
from .bittorrent_driver import BittorrentDriver

if TYPE_CHECKING:
    from rucio.core.rse import RseData


RPC_RESPONSE = 1
RPC_ERROR = 2
RPC_EVENT = 3

MESSAGE_HEADER_SIZE = 5
READ_SIZE = 10


class DelugeClientException(Exception):
    """Base exception for all deluge client exceptions"""


class ConnectionLostException(DelugeClientException):
    pass


class CallTimeoutException(DelugeClientException):
    pass


class InvalidHeaderException(DelugeClientException):
    pass


class FailedToReconnectException(DelugeClientException):
    pass


class RemoteException(DelugeClientException):
    pass


class DelugeTransferStatusReport(TransferStatusReport):

    supported_db_fields = [
        'state',
        'external_id',
    ]

    def __init__(self, request_id, external_id, deluge_response):
        super().__init__(request_id)

        if deluge_response[b'is_finished']:
            new_state = RequestState.DONE
        else:
            new_state = RequestState.SUBMITTED

        self.state = new_state
        self.external_id = None
        if new_state in [RequestState.FAILED, RequestState.DONE]:
            self.external_id = external_id

    def initialize(self, session, logger=logging.log):
        pass

    def get_monitor_msg_fields(self, session, logger=logging.log):
        return {'protocol': 'deluge'}


class DelugeDriver(BittorrentDriver):
    """

    Initially heavily inspired from https://github.com/JohnDoee/deluge-client
    """
    external_name = 'deluge'
    required_rse_attrs = ('deluge_management_address', )  # , 'deluge_management_user', 'deluge_management_password')

    timeout = 20

    @classmethod
    def make_driver(cls, rse: "RseData", logger=logging.log):
        driver = cls(
            host=rse.name,
            port=58846,
            username='rucio',
            password='rucio90df',
            logger=logger,
        )
        driver.connect()
        return driver

    def __init__(self, host, port, username, password, decode_utf8=False, automatic_reconnect=True, logger=logging.log):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.deluge_version = None
        # This is only applicable if deluge_version is 2
        self.deluge_protocol_version = None

        self.decode_utf8 = decode_utf8
        if not self.decode_utf8:
            warnings.warn('Using `decode_utf8=False` is deprecated, please set it to True.'
                          'The argument will be removed in a future release where it will be always True', DeprecationWarning)

        self.automatic_reconnect = automatic_reconnect

        self.request_id = 1
        self.connected = False
        self._create_socket()

        self.logger = logger

    def listen_addr(self) -> tuple[str, int]:
        ip = self.core.get_external_ip()
        ip = ip.decode() if isinstance(ip, bytes) else ip
        port = self.core.get_ssl_listen_port()
        if not ip:
            ip = resolve_ip(self.host)
        return ip, port

    def management_addr(self) -> tuple[str, int]:
        return self.host, self.port

    def add_torrent(self, file_name: str, file_content: bytes, download_location: str, seed_mode: bool = False):
        options = {
            'download_location': download_location,
        }
        if seed_mode:
            options['seed_mode'] = True
        self.core.add_torrent_file(
            filename=file_name,
            filedump=base64.b64encode(file_content),
            options=options,
        )

    def add_peer(self, torrent_id: str, ip: str, port: int):
        return self.core.connect_peer(torrent_id=torrent_id, ip=ip, port=port)

    def set_ssl_certificate(self, torrent_id: str, certificate: str, private_key: str, dh_params: str):
        self.core.set_ssl_torrent_cert(torrent_id, certificate, private_key, dh_params)

    def get_status(self, request_id: str, torrent_id: str):
        deluge_response = self.core.get_status(torrent_id=torrent_id, keys=[])
        return DelugeTransferStatusReport(request_id, torrent_id, deluge_response)

    def _create_socket(self, ssl_version=None):
        if ssl_version is not None:
            self._socket = ssl.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), ssl_version=ssl_version)
        else:
            self._socket = ssl.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        self._socket.settimeout(self.timeout)

    def connect(self):
        """
        Connects to the Deluge instance
        """
        self._connect()
        self.logger(logging.DEBUG, 'Connected to Deluge, detecting daemon version')
        self._detect_deluge_version()
        self.logger(logging.DEBUG, 'Daemon version {} detected, logging in'.format(self.deluge_version))
        if self.deluge_version == 2:
            result = self.call('daemon.login', self.username, self.password, client_version='deluge-client')
        else:
            result = self.call('daemon.login', self.username, self.password)
        self.logger(logging.DEBUG, 'Logged in with value %r' % result)
        self.connected = True

    def _connect(self):
        self.logger(logging.INFO, 'Connecting to %s:%s' % (self.host, self.port))
        try:
            self._socket.connect((self.host, self.port))
        except ssl.SSLError as e:
            # Note: have not verified that we actually get errno 258 for this error
            if (hasattr(ssl, 'PROTOCOL_SSLv3')
                    and (getattr(e, 'reason', None) == 'UNSUPPORTED_PROTOCOL' or e.errno == 258)):
                self.logger(logging.WARNING, 'Was unable to ssl handshake, trying to force SSLv3 (insecure)')
                self._create_socket(ssl_version=ssl.PROTOCOL_SSLv3)
                self._socket.connect((self.host, self.port))
            else:
                raise

    def disconnect(self):
        """
        Disconnect from deluge
        """
        if self.connected:
            self._socket.close()
            self._socket = None
            self.connected = False

    def _detect_deluge_version(self):
        if self.deluge_version is not None:
            return

        self._send_call(1, None, 'daemon.info')
        self._send_call(2, None, 'daemon.info')
        self._send_call(2, 1, 'daemon.info')
        result = self._socket.recv(1)
        if result[:1] == b'D':
            # This is a protocol deluge 2.0 was using before release
            self.deluge_version = 2
            self.deluge_protocol_version = None
            # If we need the specific version of deluge 2, this is it.
            _daemon_version = self._receive_response(2, None, partial_data=result)
        elif ord(result[:1]) == 1:
            self.deluge_version = 2
            self.deluge_protocol_version = 1
            # If we need the specific version of deluge 2, this is it.
            _daemon_version = self._receive_response(2, 1, partial_data=result)
        else:
            self.deluge_version = 1
            # Deluge 1 doesn't recover well from the bad request. Re-connect the socket.
            self._socket.close()
            self._create_socket()
            self._connect()

    def _send_call(self, deluge_version, protocol_version, method, *args, **kwargs):
        self.request_id += 1
        if method == 'daemon.login':
            debug_args = list(args)
            if len(debug_args) >= 2:
                debug_args[1] = '<password hidden>'
            self.logger(logging.DEBUG, 'Calling reqid %s method %r with args:%r kwargs:%r' % (self.request_id, method, debug_args, kwargs))
        else:
            self.logger(logging.DEBUG, 'Calling reqid %s method %r with args:%r kwargs:%r' % (self.request_id, method, args, kwargs))

        req = ((self.request_id, method, args, kwargs), )
        req = zlib.compress(rencode.dumps(req))

        if deluge_version == 2:
            if protocol_version is None:
                # This was a protocol for deluge 2 before they introduced protocol version numbers
                self._socket.send(b'D' + struct.pack("!i", len(req)))
            elif protocol_version == 1:
                self._socket.send(struct.pack('!BI', protocol_version, len(req)))
            else:
                raise Exception('Deluge protocol version {} is not (yet) supported.'.format(protocol_version))
        self._socket.send(req)

    def _receive_response(self, deluge_version, protocol_version, partial_data=b''):
        expected_bytes = None
        data = partial_data
        while True:
            try:
                d = self._socket.recv(READ_SIZE)
            except ssl.SSLError:
                raise CallTimeoutException()

            data += d
            if deluge_version == 2:
                if expected_bytes is None:
                    if len(data) < 5:
                        continue

                    header = data[:MESSAGE_HEADER_SIZE]
                    data = data[MESSAGE_HEADER_SIZE:]

                    if protocol_version is None:
                        if header[0] != b'D'[0]:
                            raise InvalidHeaderException('Expected D as first byte in reply')
                    elif ord(header[:1]) != protocol_version:
                        raise InvalidHeaderException(
                            'Expected protocol version ({}) as first byte in reply'.format(protocol_version)
                        )

                    if protocol_version is None:
                        expected_bytes = struct.unpack('!i', header[1:])[0]
                    else:
                        expected_bytes = struct.unpack('!I', header[1:])[0]

                if len(data) >= expected_bytes:
                    data = zlib.decompress(data)
                    break
            else:
                try:
                    data = zlib.decompress(data)
                except zlib.error:
                    if not d:
                        raise ConnectionLostException()
                    continue
                break

        data = list(rencode.loads(data, decode_utf8=self.decode_utf8))
        msg_type = data.pop(0)
        _request_id = data.pop(0)

        if msg_type == RPC_ERROR:
            if self.deluge_version == 2:
                exception_type, exception_msg, _, traceback = data
                # On deluge 2, exception arguments are sent as tuple
                if self.decode_utf8:
                    exception_msg = ', '.join(exception_msg)
                else:
                    exception_msg = b', '.join(exception_msg)
            else:
                exception_type, exception_msg, traceback = data[0]
            if self.decode_utf8:
                exception = type(str(exception_type), (RemoteException, ), {})
                exception_msg = '%s\n%s' % (exception_msg,
                                            traceback)
            else:
                exception = type(str(exception_type.decode('utf-8', 'ignore')), (RemoteException, ), {})
                exception_msg = '%s\n%s' % (exception_msg.decode('utf-8', 'ignore'),
                                            traceback.decode('utf-8', 'ignore'))
            raise exception(exception_msg)
        elif msg_type == RPC_RESPONSE:
            retval = data[0]
            return retval

    def reconnect(self):
        """
        Reconnect
        """
        self.disconnect()
        self._create_socket()
        self.connect()

    def call(self, method, *args, **kwargs):
        """
        Calls an RPC function
        """
        tried_reconnect = False
        for _ in range(2):
            try:
                self._send_call(self.deluge_version, self.deluge_protocol_version, method, *args, **kwargs)
                return self._receive_response(self.deluge_version, self.deluge_protocol_version)
            except (socket.error, ConnectionLostException, CallTimeoutException):
                if self.automatic_reconnect:
                    if tried_reconnect:
                        raise FailedToReconnectException()
                    else:
                        try:
                            self.reconnect()
                        except (socket.error, ConnectionLostException, CallTimeoutException):
                            raise FailedToReconnectException()

                    tried_reconnect = True
                else:
                    raise

    def __getattr__(self, item):
        return RPCCaller(self.call, item)

    def __enter__(self):
        """Connect to client while using with statement."""
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        """Disconnect from client at end of with statement."""
        self.disconnect()


class RPCCaller(object):
    def __init__(self, caller, method=''):
        self.caller = caller
        self.method = method

    def __getattr__(self, item):
        return RPCCaller(self.caller, self.method + '.' + item)

    def __call__(self, *args, **kwargs):
        return self.caller(self.method, *args, **kwargs)
