import multiprocessing as mp
from typing import List, Tuple
from urllib.parse import urljoin

from dataset_reader.base_reader import Query
from engine.base_client.search import BaseSearcher
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    get_table_name,
)
from engine.clients.manticoresearch.parser import ManticoreSearchConditionParser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class ManticoreSearchSearcher(BaseSearcher):
    connection_params = {}
    search_params = {}
    parser = ManticoreSearchConditionParser()

    def __init__(self, host, connection_params, search_params):
        super().__init__(host, connection_params, search_params)
        options = self.search_params.get("options", {})
        if isinstance(options, dict) and "ef" in options:
            self.search_params.setdefault("num_candidates", options["ef"])

    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=None,
        )
        adapter = HTTPAdapter(max_retries=retries)
        cls.session.mount("http://", adapter)
        cls.session.headers.update({"Connection": "keep-alive"})
        cls.session.headers.update({"Content-Type": "application/json"})
        port = connection_params.get("port", MANTICORESEARCH_PORT)
        cls.base_url = urljoin(f"http://{host}:{port}", "/search")
        cls.search_params = search_params
        cls.connection_params = {
            k: v for k, v in connection_params.items() if k != "port"
        }

    @classmethod
    def search_one(cls, query: Query, top: int) -> List[Tuple[int, float]]:
        if query.vector is None:
            raise ValueError("ManticoreSearch does not support sparse queries.")

        knn = {
            "index": get_table_name(),
            "_source": "id",
            "knn": {
                "field": "vector",
                "query_vector": query.vector,
                "k": top,
#                "rescore": True,
#                "oversampling": 3.0,
                **cls.search_params.get("options", {}),
            },
            "limit": top,
        }

        meta_conditions = cls.parser.parse(query.meta_conditions)
        if meta_conditions:
            knn["query"] = meta_conditions
        res = cls.session.post(cls.base_url, json=knn, **cls.connection_params).json()
        return [(int(hit["_id"]) - 1, hit["_knn_dist"]) for hit in res["hits"]["hits"]]
