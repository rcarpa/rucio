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
from urllib.parse import urlparse

from rucio.common import exception
from rucio.rse.protocols.protocol import RSEProtocol
from rucio.rse import rsemanager

if getattr(rsemanager, 'CLIENT_MODE', None):
    from rucio.client.didclient import DIDClient

    def _bt_meta(scope: str, name: str):
        pieces_root, pieces_layers, piece_length = bittorrent_v2_merkle_sha256(os.path.join(file['dirname'], file['basename']))
        bittorrent_meta = {
            'bittorrent_pieces_root': base64.b64encode(pieces_root).decode(),
            'bittorrent_pieces_layers': base64.b64encode(pieces_layers).decode(),
            'bittorrent_piece_length': piece_length,
        }
        DIDClient().get_metadata_bulk(scope=file['did_scope'], name=file['did_name'], meta=bittorrent_meta)
        pass

else:
    from rucio.common.types import InternalScope
    from rucio.core.did import get_metadata_bulk
    from rucio.core.rse import get_rse_vo

    def _bt_meta(scope: str, name: str):
        vo = get_rse_vo(rse['id'])  # pylint: disable=E0601
        scope = InternalScope(scope, vo=vo)  # pylint: disable=E0601

        meta = get_metadata(scope=scope, name=name, plugin='all')
        pieces_root = base64.b64decode(meta.get('bittorrent_pieces_root', ''))
        pieces_layers = base64.b64decode(meta.get('bittorrent_pieces_layers', ''))
        piece_length = meta.get('bittorrent_piece_length', 0)
        return pieces_root, pieces_layers, piece_length


class Default(RSEProtocol):

    def __init__(self, protocol_attr, rse_settings, logger=logging.log):
        """ Initializes the object with information about the referred RSE.

            :param props: Properties of the requested protocol
        """
        super(Default, self).__init__(protocol_attr, rse_settings, logger=logger)
        self.logger = logger

    @staticmethod
    def pieces_root(scope, name):
        return _pieces_root(scope, name)

    def lfns2pfns(self, lfns):
        """
            Retruns a fully qualified PFN for the file referred by path.

            :param path: The path to the file.

            :returns: Fully qualified PFN.
        """
        pfns = {}
        prefix = self.attributes['prefix']

        if not prefix.startswith('/'):
            prefix = ''.join(['/', prefix])
        if not prefix.endswith('/'):
            prefix = ''.join([prefix, '/'])

        lfns = [lfns] if isinstance(lfns, dict) else lfns
        for lfn in lfns:
            scope, name = lfn['scope'], lfn['name']

            if 'path' in lfn and lfn['path'] is not None:
                pfns['%s:%s' % (scope, name)] = ''.join([prefix, lfn['path'] if not lfn['path'].startswith('/') else lfn['path'][1:]])
            else:
                pfns['%s:%s' % (scope, name)] = ''.join([prefix, self._get_path(scope=scope, name=name)])
        return pfns

    def parse_pfns(self, pfns):
        """
            Splits the given PFN into the parts known by the protocol. It is also checked if the provided protocol supportes the given PFNs.

            :param pfns: a list of a fully qualified PFNs

            :returns: dic with PFN as key and a dict with path and name as value

            :raises RSEFileNameNotSupported: if the provided PFN doesn't match with the protocol settings
        """
        ret = dict()
        pfns = [pfns] if isinstance(pfns, str) else pfns

        for pfn in pfns:
            parsed = urlparse(pfn)
            scheme = parsed.scheme
            hostname = parsed.netloc.partition(':')[0]
            port = int(parsed.netloc.partition(':')[2]) if parsed.netloc.partition(':')[2] != '' else 0
            while '//' in parsed.path:
                parsed = parsed._replace(path=parsed.path.replace('//', '/'))
            path = parsed.path

            # Protect against 'lazy' defined prefixes for RSEs in the repository
            if not self.attributes['prefix'].startswith('/'):
                self.attributes['prefix'] = '/' + self.attributes['prefix']
            if not self.attributes['prefix'].endswith('/'):
                self.attributes['prefix'] += '/'

            if self.attributes['hostname'] != hostname:
                if self.attributes['hostname'] != 'localhost':  # In the database empty hostnames are replaced with localhost but for some URIs (e.g. file) a hostname is not included
                    raise exception.RSEFileNameNotSupported('Invalid hostname: provided \'%s\', expected \'%s\'' % (hostname, self.attributes['hostname']))

            if self.attributes['port'] != port:
                raise exception.RSEFileNameNotSupported('Invalid port: provided \'%s\', expected \'%s\'' % (port, self.attributes['port']))

            if not path.startswith(self.attributes['prefix']):
                raise exception.RSEFileNameNotSupported('Invalid prefix: provided \'%s\', expected \'%s\'' % ('/'.join(path.split('/')[0:len(self.attributes['prefix'].split('/')) - 1]),
                                                                                                              self.attributes['prefix']))  # len(...)-1 due to the leading '/

            # Spliting parsed.path into prefix, path, filename
            prefix = self.attributes['prefix']
            path = path.partition(self.attributes['prefix'])[2]
            name = path.split('/')[-1]
            path = '/'.join(path.split('/')[:-1])
            if not path.startswith('/'):
                path = '/' + path
            if path != '/' and not path.endswith('/'):
                path = path + '/'
            ret[pfn] = {'path': path, 'name': name, 'scheme': scheme, 'prefix': prefix, 'port': port, 'hostname': hostname, }

        return ret
