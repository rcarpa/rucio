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
from os import path
from typing import TYPE_CHECKING, Optional, Sequence, Type

from rucio.common.config import config_get
from rucio.common.extra import import_extras
from rucio.core.did_meta_plugins import get_metadata
from rucio.transfertool.transfertool import Transfertool, TransferToolBuilder
from .bittorrent_driver import BittorrentDriver

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import datetime

if TYPE_CHECKING:
    from rucio.core.rse import RseData

DRIVER_NAME_RSE_ATTRIBUTE = 'bittorrent_driver'
DRIVER_CLASSES_BY_NAME: dict[str, Type[BittorrentDriver]] = {}

EXTRA_MODULES = import_extras(['libtorrent', 'deluge', 'qbittorrentapi'])
if EXTRA_MODULES['libtorrent']:
    import libtorrent as lt  # noqa

    if EXTRA_MODULES['deluge']:
        from .bittorrent_driver_deluge import DelugeDriver
        DRIVER_CLASSES_BY_NAME[DelugeDriver.external_name] = DelugeDriver

    if EXTRA_MODULES['qbittorrentapi']:
        from .bittorrent_driver_qbittorrent import QBittorrentDriver
        DRIVER_CLASSES_BY_NAME[QBittorrentDriver.external_name] = QBittorrentDriver

DH_PARAMS = """
-----BEGIN DH PARAMETERS-----
MIIBCAKCAQEA+oeNEEXOCzrdmDwkKb31I+WaGIeRlx9jvF4sold3Mrw8tQ8rqyfc
GNfjEUhqSnyROQ9Wf8BvQJ94Fcw3oV9Os3APZtHOwTag3PzSe2ImCHTWL+LbQD/m
bl2zDJ2xD6j1ZmyGes8DZC8RyBEMSS/aoWFKWKzlba5WXTzC8n/2MBReoOm2eMhF
wUG21UW/MQQ+i1sHrC0d0zPdvnqXAa7tnO70j/kLhxv8446fsbXJo4G/iIAR1RSD
UbMIXHrloW/G5BviauWNxIwvfTYTlzfzwhhCDieLI/GwuAF388BKG4KQ181qrTFO
iTniEzsEklfNUEZ59lwiDmJF1qmmH017PwIBAg==
-----END DH PARAMETERS-----
"""


def generate_x509_cert(common_name, org_name, org_unit_name, san_list=None):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    builder = x509.CertificateBuilder(
    ).subject_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
                #x509.NameAttribute(NameOID.ORGANIZATION_NAME, org_name),
                #x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, org_unit_name),
            ]
        )
    ).not_valid_before(
        datetime.datetime.utcnow() - datetime.timedelta(days=1)
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=90)
    ).serial_number(
        x509.random_serial_number()
    ).public_key(
        private_key.public_key()
    )

    sans = []
    # Check if we are generating a wildcard; act accordingly
    if common_name.startswith('*.'):
        sans.append(str(common_name))
        sans.append(str(common_name[2:]))

    if san_list:
        sans = sans + san_list
    if sans:
        san_objects = [x509.DNSName(str(san).strip()) for san in sans if str(san).strip()]
        builder = builder.add_extension(x509.SubjectAlternativeName(san_objects), critical=False)

    return private_key, builder


class BittorrentTransfertool(Transfertool):
    """
    Use bittorrent to perform the peer-to-peer transfer.

    Supports qbittorrent and deluge
    """
    external_name = 'bittorrent'

    required_rse_attrs = (DRIVER_NAME_RSE_ATTRIBUTE, )

    def __init__(self, external_host, logger=logging.log):
        super().__init__(external_host=external_host, logger=logger)

        self._drivers_by_rse_id = {}
        self.ca_cert, self.ca_key = None, None

    @classmethod
    def _pick_management_api_driver_cls(cls, rse) -> Optional[Type[BittorrentDriver]]:
        driver_cls = DRIVER_CLASSES_BY_NAME.get(rse.attributes.get(DRIVER_NAME_RSE_ATTRIBUTE))
        if driver_cls is None:
            return None
        if not all(rse.attributes.get(attribute) is not None for attribute in driver_cls.required_rse_attrs):
            return None
        return driver_cls

    def _driver_for_rse(self, rse: "RseData") -> BittorrentDriver:
        driver = self._drivers_by_rse_id.get(rse.id)
        if driver:
            return driver

        driver_cls = self._pick_management_api_driver_cls(rse)
        driver = driver_cls.make_driver(rse)
        self._drivers_by_rse_id[rse.id] = driver
        return driver

    @staticmethod
    def _get_torrent_meta(scope, name):
        meta = get_metadata(scope=scope, name=name, plugin='all')
        pieces_root = base64.b64decode(meta.get('bittorrent_pieces_root', ''))
        pieces_layers = base64.b64decode(meta.get('bittorrent_pieces_layers', ''))
        piece_length = meta.get('bittorrent_piece_length', 0)
        return pieces_root, pieces_layers, piece_length

    def x509_ca(self):
        if not self.ca_cert or not self.ca_key or not self.ca_cert.not_valid_after < datetime.datetime.utcnow() + datetime.timedelta(days=60):
            common_name = 'Rucio Bittorrent CA'
            private_key, builder = generate_x509_cert(common_name=common_name, org_name='Rucio', org_unit_name=self.external_name)
            builder = builder.add_extension(
                x509.BasicConstraints(ca=True, path_length=1), critical=True,
            ).issuer_name(x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ]))
            certificate = builder.sign(
                private_key=private_key, algorithm=hashes.SHA256(),
                backend=default_backend()
            )
            self.ca_cert, self.ca_key = certificate, private_key
        return self.ca_cert, self.ca_key

    def generate_x509_peer_cert(self, torrent_name, peer_name, ca_cert, ca_key):
        private_key, builder = generate_x509_cert(common_name=peer_name, org_name='Rucio', org_unit_name=self.external_name, san_list=[torrent_name])
        builder = builder.issuer_name(ca_cert.issuer)
        certificate = builder.sign(
            private_key=ca_key, algorithm=hashes.SHA256(),
            backend=default_backend()
        )
        return certificate, private_key

    @classmethod
    def submission_builder_for_path(cls, transfer_path, logger=logging.log):
        hop = transfer_path[0]
        if hop.rws.byte_count == 0:
            logger(logging.INFO, f"Bittorrent cannot transfer fully empty torrents. Skipping {hop}")
            return [], None

        if not cls.can_perform_transfer(hop.src.rse, hop.dst.rse):
            logger(logging.INFO, f"The required RSE attributes are not set. Skipping {hop}")
            return [], None

        for rse in [hop.src.rse, hop.dst.rse]:
            driver_cls = cls._pick_management_api_driver_cls(rse)
            if not driver_cls:
                logger(logging.INFO, f"The rse '{rse}' is not configured correctly for bittorrent")
                return [], None

        #pieces_root, _pieces_layers, piece_length = cls._get_torrent_meta(hop.rws.scope, hop.rws.name)
        #if not pieces_root or not piece_length:
        #    logger(logging.INFO, "The required bittorrent metadata not set on the DID")
        #    return [], None

        return [hop], TransferToolBuilder(cls, external_host='Bittorrent Transfertool')

    def group_into_submit_jobs(self, transfer_paths):
        return [{'transfers': transfer_path, 'job_params': {}} for transfer_path in transfer_paths]

    @staticmethod
    def _build_torrent(
            trackers: list[str],
            scope: "InternalScope",
            name: str,
            length: int,
            piece_length: int,
            pieces_root: bytes,
            pieces_layers: Optional[bytes] = None,
            ssl_cert_pem: Optional[bytes] = None,
    ) -> tuple[str, bytes]:
        torrent_dict = {
            b'info': {
                b'meta version': 2,
                b'private': 1,
                b'name': f'{scope}:{name}'.encode(),
                b'piece length': piece_length,
                b'file tree': {
                    name.encode(): {
                        b'': {
                            b'length': length,
                            b'pieces root': pieces_root,
                        }
                    }
                },
                b'ssl-cert': ssl_cert_pem,
            },
        }
        if trackers:
            torrent_dict[b'announce'] = trackers[0].encode()
            if len(trackers) > 1:
                torrent_dict[b'announce-list'] = [t.encode() for t in trackers]
        if pieces_layers:
            torrent_dict[b'piece layers'] = {
                pieces_root: pieces_layers
            }

        ti = lt.torrent_info(torrent_dict)
        t = lt.create_torrent(ti)
        info_hash = str(ti.info_hash())
        torrent = t.generate()
        return info_hash, lt.bencode(torrent)

    @staticmethod
    def _connect_without_tracker(torrent_id, peers_drivers: Sequence[BittorrentDriver]):
        peer_addr = []
        for i, driver in enumerate(peers_drivers):
            peer_addr.append(driver.listen_addr())

        for src, src_driver in enumerate(peers_drivers):
            for dst, dst_driver in enumerate(peers_drivers):
                if src != dst:
                    src_ip, src_port = peer_addr[src]
                    dst_ip, dst_port = peer_addr[dst]
                    src_driver.add_peer(torrent_id=torrent_id, ip=dst_ip, port=dst_port)
                    dst_driver.add_peer(torrent_id=torrent_id, ip=src_ip, port=src_port)

    def submit(self, transfers, job_params, timeout=None):
        [transfer] = transfers
        rws = transfer.rws

        tracker = transfer.dst.rse.attributes.get('bittorrent-tracker-addr')
        #tracker = 'http://192.168.1.10:9000/announce'
        if not tracker:
            tracker = config_get('transfers', 'bittorrent-tracker-addr', raise_exception=False, default=None)

        src_driver = self._driver_for_rse(transfer.src.rse)
        dst_driver = self._driver_for_rse(transfer.dst.rse)

        pieces_root, pieces_layers, piece_length = self._get_torrent_meta(rws.scope, rws.name)
        if not pieces_root:
            pass
            #src_driver.create_torrent()
            #add_bittorent_meta

        ca_cert, ca_key = self.x509_ca()

        torrent_id, torrent = self._build_torrent(
            trackers=[tracker] if tracker else [],
            scope=rws.scope,
            name=rws.name,
            length=rws.byte_count,
            piece_length=piece_length,
            pieces_root=pieces_root,
            pieces_layers=pieces_layers,
            ssl_cert_pem=ca_cert.public_bytes(encoding=serialization.Encoding.PEM),
        )
        peer_cert, peer_key = self.generate_x509_peer_cert(torrent_name=rws.name, peer_name='a', ca_cert=ca_cert, ca_key=ca_key)

        peer_cert_pem = peer_cert.public_bytes(encoding=serialization.Encoding.PEM).decode()
        peer_key_pem = peer_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

        peer_cert, peer_key = self.generate_x509_peer_cert(torrent_name=rws.name, peer_name='b', ca_cert=ca_cert, ca_key=ca_key)
        peer_cert_pem2 = peer_cert.public_bytes(encoding=serialization.Encoding.PEM).decode()
        peer_key_pem2 = peer_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

        with open(f'/opt/rucio/lib/bittorent/{torrent_id}.torrent', 'wb') as f:
            f.write(torrent)
        with open(f'/opt/rucio/lib/bittorent/ca.pem', 'wb') as f:
            f.write(ca_cert.public_bytes(encoding=serialization.Encoding.PEM))
        with open(f'/opt/rucio/lib/bittorent/peer1/{torrent_id}', 'w') as f:
            f.write(peer_cert_pem)
        with open(f'/opt/rucio/lib/bittorent/peer1/{torrent_id}.key', 'w') as f:
            f.write(peer_key_pem)
        with open(f'/opt/rucio/lib/bittorent/peer1/{torrent_id}.dh', 'w') as f:
            f.write(DH_PARAMS)
        with open(f'/opt/rucio/lib/bittorent/peer2/{torrent_id}', 'w') as f:
            f.write(peer_cert_pem2)
        with open(f'/opt/rucio/lib/bittorent/peer2/{torrent_id}.key', 'w') as f:
            f.write(peer_key_pem2)
        with open(f'/opt/rucio/lib/bittorent/peer2/{torrent_id}.dh', 'w') as f:
            f.write(DH_PARAMS)

        src_driver.add_torrent(
            file_name=rws.name,
            file_content=torrent,
            download_location=path.dirname(transfer.dest_url),
            seed_mode=True,
        )
        dst_driver.add_torrent(
            file_name=rws.name,
            file_content=torrent,
            download_location=path.dirname(transfer.legacy_sources[0][1]),
        )
        src_driver.set_ssl_certificate(torrent_id=torrent_id, certificate=peer_cert_pem, private_key=peer_key_pem, dh_params=DH_PARAMS)
        dst_driver.set_ssl_certificate(torrent_id=torrent_id, certificate=peer_cert_pem2, private_key=peer_key_pem2, dh_params=DH_PARAMS)

        if not tracker:
            self._connect_without_tracker(torrent_id, [src_driver, dst_driver])

        return torrent_id

    def bulk_query(self, requests_by_eid, timeout=None):
        response = {}
        for transfer_id, requests in requests_by_eid.items():
            for request_id, request in requests.items():
                driver = self._driver_for_rse(request['dst_rse'])
                response.setdefault(transfer_id, {})[request_id] = driver.get_status(request_id=request_id, torrent_id=transfer_id)
        return response

    def query(self, transfer_ids, details=False, timeout=None):
        pass

    def cancel(self, transfer_ids, timeout=None):
        pass

    def update_priority(self, transfer_id, priority, timeout=None):
        pass
