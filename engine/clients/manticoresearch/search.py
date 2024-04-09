import multiprocessing as mp
import uuid
from typing import List, Tuple
from urllib.parse import urljoin

from engine.base_client.search import BaseSearcher
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    MANTICORESEARCH_TABLE,
)
from engine.clients.manticoresearch.parser import ManticoreSearchConditionParser
import requests

class ManticoreSearchSearcher(BaseSearcher):
    search_params = {}
    parser = ManticoreSearchConditionParser()

    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.session = requests.Session()
        cls.session.headers.update({"Content-Type": "application/json"})
        cls.base_url = urljoin(f"http://{host}:{MANTICORESEARCH_PORT}", "/search")
        cls.search_params = search_params
        print(search_params)

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:
        knn = {
            "index": MANTICORESEARCH_TABLE,
            "knn": {
                "field": "vector",
                "query_vector": vector,
                "k": top,
            },
            **{"options": {"cutoff": 100}, **cls.search_params}
        }

        meta_conditions = cls.parser.parse(meta_conditions)
        if meta_conditions:
            knn.update(meta_conditions)

        res = cls.session.post(cls.base_url, json=knn).json()
        return [(int(hit["_id"]), hit["_score"]) for hit in res["hits"]["hits"]]

