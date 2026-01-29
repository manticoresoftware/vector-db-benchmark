from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    get_table_name,
    set_table_name,
)

import time
import requests

class ManticoreSearchConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: "L2",
        Distance.COSINE: "COSINE",
    }


    INDEX_TYPE_MAPPING = {
        "int": "uint",
        "keyword": "string",
        "text": "text",
        "float": "float",
        "geo": "json",  # Manticore typically handles geo as JSON
    }
    
    def __init__(self, host, collection_params, connection_params):
        self.host = host
        self.collection_params = collection_params
        self.connection_params = connection_params

    def clean(self):
        port = self.connection_params.get("port", MANTICORESEARCH_PORT)
        request_params = {
            k: v for k, v in self.connection_params.items() if k != "port"
        }
        url = f'http://{self.host}:{port}/sql?mode=raw'
        query = f"DROP TABLE IF EXISTS `{get_table_name()}`"
        data = 'query=' + requests.utils.quote(query, safe='')
        response = requests.post(url, data, **request_params)
        if response.status_code != 200:
            print(f'Error cleaning table: {response.text}')

    def recreate(self, dataset, collection_params):
        if dataset.config.type == "sparse":
            raise IncompatibilityError
        if dataset.config.distance == Distance.DOT:
            raise IncompatibilityError
        knn_options = collection_params.get('knn_options', {})
        hnsw_options = ' '.join([f"{key}='{value}'" for key, value in knn_options.items()])
        engine = collection_params.get("engine", "columnar")
        if engine not in {"columnar", "rowwise"}:
            raise ValueError(f"Unsupported Manticore engine: {engine}")
        optimize_cutoff = collection_params.get("optimize_cutoff", 1)
        auto_optimize = collection_params.get("auto_optimize", None)

        vector_field = {
            'name': 'vector',
            'type': f"float_vector knn_type='hnsw' knn_dims='{dataset.config.vector_size}' hnsw_similarity='{self.DISTANCE_MAPPING[dataset.config.distance]}' {hnsw_options}",
        }

        fields = [vector_field] + [
            {
                'name': field_name,
                'type': self.INDEX_TYPE_MAPPING.get(field_type, field_type),
            }
            for field_name, field_type in dataset.config.schema.items()
        ]

        field_definitions = ', '.join([f"`{field['name']}` {field['type']}" for field in fields])

        query = f"""
        CREATE TABLE IF NOT EXISTS `{get_table_name()}` (
            {field_definitions}
        ) optimize_cutoff='{optimize_cutoff}' engine='{engine}'
        """
        port = self.connection_params.get("port", MANTICORESEARCH_PORT)
        request_params = {
            k: v for k, v in self.connection_params.items() if k != "port"
        }
        url = f'http://{self.host}:{port}/sql?mode=raw'
        data = 'query=' + requests.utils.quote(query, safe='')
        response = requests.post(url, data, **request_params)

        if response.status_code != 200:
            error_text = response.text
            if "directory is not empty" in error_text:
                fallback_name = f"{get_table_name()}_{int(time.time())}"
                set_table_name(fallback_name)
                query = f"""
        CREATE TABLE IF NOT EXISTS `{get_table_name()}` (
            {field_definitions}
        ) optimize_cutoff='{optimize_cutoff}' engine='{engine}'
        """
                data = 'query=' + requests.utils.quote(query, safe='')
                response = requests.post(url, data, **request_params)
            if response.status_code != 200:
                print(f'Error creating table: {response.text}')

        if auto_optimize is not None:
            auto_optimize_query = f"SET GLOBAL auto_optimize={auto_optimize}"
            data = 'query=' + requests.utils.quote(auto_optimize_query, safe='')
            response = requests.post(url, data, **request_params)
            if response.status_code != 200:
                print(f'Error setting auto_optimize: {response.text}')
