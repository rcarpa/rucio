import logging
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rucio.core.rse import RseData

class BittorrentDriver(metaclass=ABCMeta):
    external_name = ''
    required_rse_attrs = tuple()

    @classmethod
    @abstractmethod
    def make_driver(cls, rse: "RseData", logger=logging.log):
        pass

    @abstractmethod
    def listen_addr(self) -> tuple[str, int]:
        pass

    @abstractmethod
    def management_addr(self) -> tuple[str, int]:
        pass

    @abstractmethod
    def add_torrent(self, file_name: str, file_content: bytes, download_location: str, seed_mode: bool = False):
        pass

    @abstractmethod
    def add_peer(self, torrent_id: str, ip: str, port: int):
        pass

    @abstractmethod
    def set_ssl_certificate(self, torrent_id: str, certificate: str, private_key: str, dh_params: str):
        pass

    @abstractmethod
    def get_status(self, request_id: str, torrent_id: str):
        pass
