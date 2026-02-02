import multiprocessing as mp
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    get_table_name,
    set_table_name,
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
        table_name = connection_params.get("table")
        if table_name:
            set_table_name(table_name)
        port = connection_params.get("port", MANTICORESEARCH_PORT)
        cls.api_url = f"http://{host}:{port}"
        # Do not retry on HTTP 5xx here: Manticore /bulk often returns a JSON
        # body with the actual error and line number; retrying hides it behind
        # "too many 500 error responses".
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            status=0,
            backoff_factor=0.1,
            allowed_methods=None,
            raise_on_status=False,
        )
        cls.host = host
        cls.session = ClosableSession()
        adapter = HTTPAdapter(max_retries=retries)
        cls.session.mount("http://", adapter)
        cls.session.headers.update({"Content-Type": "application/x-ndjson"})
        request_params = {
            k: v for k, v in connection_params.items() if k not in {"port", "table"}
        }
        # Manticore bulk uploads can take longer than the default request timeout,
        # especially with large batch sizes and parallel workers.
        #
        # Important: urllib3/http.client uses the same underlying socket timeout
        # for writing the request body too. So a (connect, read) tuple can still
        # fail during send if connect stays low. Use a single larger timeout.
        timeout = request_params.get("timeout")
        if isinstance(timeout, (int, float)):
            request_params["timeout"] = max(float(timeout), 300.0)
        cls.connection_params = request_params
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(cls, batch: List[Record]):
        docs = []
        record_ids = []
        for record in batch:
            if record.vector is None:
                raise ValueError("ManticoreSearch does not support sparse vectors.")
            doc = {"vector": record.vector}
            if record.metadata:
                doc.update(record.metadata)

            record_id = record.id + 1  # we do not support id=0
            data = {
                "table": get_table_name(),
                "id": record_id,
                "doc": doc,
            }
            docs.append({"insert": data})
            record_ids.append(record_id)

        payload = "\n".join(json.dumps(item) for item in docs) + "\n"
        response = cls.session.post(
            f"{cls.api_url}/bulk", data=payload, **cls.connection_params
        )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            details = response.text
            try:
                body = response.json()
                if isinstance(body, dict):
                    current_line = body.get("current_line")
                    if isinstance(current_line, int) and 1 <= current_line <= len(record_ids):
                        details = (
                            f"{details}\n"
                            f"bulk current_line={current_line}, record_id={record_ids[current_line-1]}, "
                            f"table={get_table_name()}"
                        )
            except Exception:
                pass
            print(f"Manticore bulk error {response.status_code}: {details}")
            raise

    @classmethod
    def post_upload(cls, _distance):
        optimize_cutoff = cls.upload_params.get("optimize_cutoff", 1)
        response = cls.session.post(
            f"{cls.api_url}/sql?mode=raw",
            data=f"query=FLUSH%20RAMCHUNK%20%60{get_table_name()}%60",
            **cls.connection_params,
        )
        response.raise_for_status()
        if cls.upload_params.get("skip_optimize", False):
            return {}

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
            # optimize_cutoff is a threshold/limit, not an exact expected value.
            # Having fewer disk chunks than the cutoff is fine (it means chunks
            # were merged further).
            if disk_chunks is not None and int(disk_chunks) > int(optimize_cutoff):
                raise ValueError(
                    f"disk_chunks={disk_chunks} is greater than optimize_cutoff={optimize_cutoff}"
                )
        except Exception as exc:
            raise ValueError(
                f"Failed to validate disk_chunks for {get_table_name()}"
            ) from exc
        return {}
