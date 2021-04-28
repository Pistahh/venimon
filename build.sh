#!/bin/bash

set -e

docker build -t collector -f Dockerfile.collector src
docker build -t dbstore -f Dockerfile.dbstore src
