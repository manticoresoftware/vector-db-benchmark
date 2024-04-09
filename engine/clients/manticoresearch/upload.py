import multiprocessing as mp
import uuid
from typing import List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    MANTICORESEARCH_TABLE,
)

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
        cls.api_url = f"http://{host}:{MANTICORESEARCH_PORT}" 
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        cls.host = host
        cls.session = ClosableSession()
        adapter = HTTPAdapter(max_retries=retries)
        cls.session.mount("http://", adapter)
        cls.session.headers.update({"Content-type": "application/x-ndjson"})
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]):
        if metadata is None:
            metadata = [{}] * len(vectors)

        docs = []
        for id, vector, payload in zip(ids, vectors, metadata):
            data = {
                "index": MANTICORESEARCH_TABLE,
                "id": id, 
                "doc": {
                    "vector": vector
                }            
            }
            #  data.update(payload)
            docs.append({"insert": data})
        data = '\n'.join([json.dumps(item) for item in docs])
        response = cls.session.post(f"{cls.api_url}/bulk", data)
        response.raise_for_status()

    @classmethod
    def post_upload(cls, _distance):
        response = cls.session.post(f"{cls.api_url}/sql?mode=raw", data=f"query=FLUSH%20RAMCHUNK%60{MANTICORESEARCH_TABLE}%60")
        response.raise_for_status()
        return {}
