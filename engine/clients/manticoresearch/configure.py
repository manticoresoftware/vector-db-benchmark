from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.manticoresearch.config import (
    MANTICORESEARCH_PORT,
    MANTICORESEARCH_TABLE,
)

import requests
import json 
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
        url = f'http://{self.host}:{MANTICORESEARCH_PORT}/sql?mode=raw'
        query = f"DROP TABLE IF EXISTS `{MANTICORESEARCH_TABLE}`"
        data = 'query=' + requests.utils.quote(query, safe='')
        response = requests.post(url, data, **self.connection_params)
        if response.status_code != 200:
            print(f'Error cleaning table: {response.text}')

    def recreate(self, dataset, collection_params):
        if dataset.config.distance == Distance.DOT:
            raise IncompatibilityError
        knn_options = collection_params.get('knn_options', {})
        hnsw_options = ' '.join([f"{key}='{value}'" for key, value in knn_options.items()])

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
        CREATE TABLE IF NOT EXISTS `{MANTICORESEARCH_TABLE}` (
            {field_definitions}
        ) optimize_cutoff='1' engine='columnar'
        """
        url = f'http://{self.host}:{MANTICORESEARCH_PORT}/sql?mode=raw'
        data = 'query=' + requests.utils.quote(query, safe='')
        response = requests.post(url, data, **self.connection_params)

        if response.status_code != 200:
            print(f'Error creating table: {response.text}')

#        query = f"SET GLOBAL auto_optimize=0"
#        url = f'http://{self.host}:{MANTICORESEARCH_PORT}/sql?mode=raw'
#        data = 'query=' + requests.utils.quote(query, safe='')
#        response = requests.post(url, data, **self.connection_params)

#        if response.status_code != 200:
#            print(f'Error running SET GLOBAL: {response.text}')