#!/bin/bash

cd "$(dirname "$0")/../../"


ENGINE_NAME="weaviate-1.14.1"
DATASET_NAME="random-100"
SERVER_BACKEND="remote"
CLIENT_BACKEND="remote"
SERVER_HOST="49.12.245.80"
DOCKER_HOST="ssh://benchmark"
EF_CONSTRUCTION=100
MAX_LINKS=50

venv/bin/python3 main.py run-client $ENGINE_NAME configure $DATASET_NAME \
      --backend-type $CLIENT_BACKEND \
      --server-host $SERVER_HOST \
      --docker-host $DOCKER_HOST \
      --ef-construction $EF_CONSTRUCTION \
      --max-connections $MAX_LINKS