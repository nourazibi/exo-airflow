"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url.rstrip("/")
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        path = path if path.startswith("/") else f"/{path}"
        query_params = {"op": op, "user.name": self.user, **params}
        return f"{self.base_url}{path}?{urlencode(query_params)}"

    def mkdirs(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "MKDIRS")
        resp = requests.put(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return bool(data.get("boolean", False))

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        init_url = self._url(hdfs_path, "CREATE", overwrite="true")
        init_resp = requests.put(init_url, allow_redirects=False, timeout=30)

        if init_resp.status_code not in (307, 201):
            raise RuntimeError(
                f"Échec init upload WebHDFS ({init_resp.status_code}): {init_resp.text}"
            )

        redirect_url = init_resp.headers.get("Location")
        if not redirect_url:
            if init_resp.status_code == 201:
                return hdfs_path
            raise RuntimeError("Aucune URL de redirection fournie par WebHDFS.")

        with open(local_file_path, "rb") as f:
            upload_resp = requests.put(
                redirect_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=300,
            )

        if upload_resp.status_code != 201:
            raise RuntimeError(
                f"Échec upload DataNode ({upload_resp.status_code}): {upload_resp.text}"
            )

        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        url = self._url(hdfs_path, "OPEN")
        resp = requests.get(url, allow_redirects=True, timeout=300)
        resp.raise_for_status()
        return resp.content

    def exists(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "GETFILESTATUS")
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return False

    def list_status(self, hdfs_path: str) -> list:
        url = self._url(hdfs_path, "LISTSTATUS")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("FileStatuses", {}).get("FileStatus", [])