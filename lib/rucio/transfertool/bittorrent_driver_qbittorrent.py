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

import logging
from typing import TYPE_CHECKING, Optional

import qbittorrentapi

from rucio.common.utils import resolve_ip
from rucio.db.sqla.constants import RequestState
from rucio.transfertool.transfertool import TransferStatusReport
from .bittorrent_driver import BittorrentDriver

if TYPE_CHECKING:
    from rucio.core.rse import RseData


class QBittorrentTransferStatusReport(TransferStatusReport):

    supported_db_fields = [
        'state',
        'external_id',
    ]

    def __init__(self, request_id, external_id, qbittorrent_response: Optional[qbittorrentapi.TorrentDictionary]):
        super().__init__(request_id)

        if qbittorrent_response and qbittorrent_response.state_enum.is_complete == 1:
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
        return {'protocol': 'qbittorrent'}


class QBittorrentDriver(BittorrentDriver):

    external_name = 'qbittorrent'
    required_rse_attrs = ('qbittorrent_management_address', )

    @classmethod
    def make_driver(cls, rse: "RseData", logger=logging.log):
        return cls(
            host=rse.name,
            port=8080,
            username='rucio',
            password='rucio90df',
            logger=logger,
        )

    def __init__(self, host, port, username, password, logger=logging.log):
        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
        )
        self.logger = logger

    def listen_addr(self) -> tuple[str, int]:
        preferences = self.client.app_preferences()
        port = preferences['ssl_listen_port']
        ip = resolve_ip(self.client.host)
        return ip, port

    def management_addr(self) -> tuple[str, int]:
        return self.client.host, self.client.port

    def add_torrent(self, file_name: str, file_content: bytes, download_location: str, seed_mode: bool = False):
        self.client.torrents_add(
            rename=file_name,
            torrent_files=file_content,
            save_path=download_location,
            is_skip_checking=seed_mode,
            is_sequential_download=True,
        )

    def add_peer(self, torrent_id: str, ip: str, port: int):
        self.client.torrents_add_peers(torrent_hashes=[torrent_id], peers=[f'{ip}:{port}'])

    def set_ssl_certificate(self, torrent_id: str, certificate: str, private_key: str, dh_params: str):
        self.client.torrents._client._post(
            _name='torrents',
            _method='setSSLCertificate',
            data={
                'hash': torrent_id
            },
            files={
                'certificate': certificate.encode(),
                'privateKey': private_key.encode(),
                'dhParams': dh_params.encode()
            }
        )

    def get_status(self, request_id: str, torrent_id: str):
        info = self.client.torrents_info(torrent_hashes=[torrent_id])
        return QBittorrentTransferStatusReport(request_id, external_id=torrent_id, qbittorrent_response=info[0] if info else None)
