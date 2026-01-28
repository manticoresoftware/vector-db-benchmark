import multiprocessing as mp
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    get_table_name,
)

from dataset_reader.base_reader import Record
from engine.base_client.upload import BaseUploader
import json

class ClosableSession(requests.Session):
    def __del__(self):
        self.close()

class ManticoreSearchUploader(BaseUploader):
    api_url = None
    session: requests.Session = None
    upload_params = {}

    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        port = connection_params.get("port", MANTICORESEARCH_PORT)
        cls.api_url = f"http://{host}:{port}"
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        cls.host = host
        cls.session = ClosableSession()
        adapter = HTTPAdapter(max_retries=retries)
        cls.session.mount("http://", adapter)
        cls.session.headers.update({"Content-type": "application/x-ndjson"})
        cls.connection_params = {
            k: v for k, v in connection_params.items() if k != "port"
        }
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(cls, batch: List[Record]):
        docs = []
        for record in batch:
            if record.vector is None:
                raise ValueError("ManticoreSearch does not support sparse vectors.")
            doc = {"vector": record.vector}
            if record.metadata:
                doc.update(record.metadata)

            data = {
                "index": get_table_name(),
                "id": record.id + 1,  # we do not support id=0
                "doc": doc,
            }
            docs.append({"insert": data})
        data = '\n'.join([json.dumps(item) for item in docs])
        response = cls.session.post(
            f"{cls.api_url}/bulk", data, **cls.connection_params
        )
        response.raise_for_status()

    @classmethod
    def post_upload(cls, _distance):
        optimize_cutoff = cls.upload_params.get("optimize_cutoff", 1)
        response = cls.session.post(
            f"{cls.api_url}/sql?mode=raw",
            data=f"query=FLUSH%20RAMCHUNK%60{get_table_name()}%60",
            **cls.connection_params,
        )
        response.raise_for_status()

        request_params = dict(cls.connection_params)
        request_params.pop("timeout", None)
        request_params.pop("optimize_timeout", None)
        optimize_timeout = cls.upload_params.get(
            "optimize_timeout", cls.connection_params.get("optimize_timeout")
        )
        if optimize_timeout is not None:
            request_params["timeout"] = optimize_timeout
        response = cls.session.post(
            f"{cls.api_url}/sql?mode=raw", 
            data=f"query=OPTIMIZE%20TABLE%20%60{get_table_name()}%60%20OPTION%20sync%3D1%20%2Ccutoff%3D{optimize_cutoff}",
            **request_params,
        )
        response.raise_for_status()

        response = cls.session.post(
            f"{cls.api_url}/sql?mode=raw",
            data=f"query=SHOW%20TABLE%20%60{get_table_name()}%60%20STATUS",
            **request_params,
        )
        response.raise_for_status()
        try:
            payload = response.json()
            disk_chunks = None
            if isinstance(payload, list) and payload:
                rows = payload[0].get("data") or []
                for row in rows:
                    if row.get("Variable_name") == "disk_chunks":
                        disk_chunks = row.get("Value")
                        break
            elif isinstance(payload, dict):
                rows = payload.get("data") or []
                if rows and isinstance(rows[0], dict):
                    disk_chunks = rows[0].get("disk_chunks")
            if disk_chunks is not None and int(disk_chunks) != int(optimize_cutoff):
                raise ValueError(
                    f"disk_chunks={disk_chunks} does not match optimize_cutoff={optimize_cutoff}"
                )
        except Exception as exc:
            raise ValueError(
                f"Failed to validate disk_chunks for {get_table_name()}"
            ) from exc
        return {}
