#!/bin/bash

mvn clean
mvn package

VERSION=`cat VERSION`

echo "*** Updating ConsumerGraph version in Dockerfile and run.sh ***"
sed -e "s/CONSUMERGRAPH_VERSION/${VERSION}/g" Dockerfile.tmpl > Dockerfile
sed -e "s/CONSUMERGRAPH_VERSION/${VERSION}/g" scripts/run.tmpl > scripts/run.sh
chmod +x scripts/run.sh

echo "*** Building ConsumerGraph Docker image ***"
docker build -t "consumergraph" .

echo "*** Tagging ConsumerGraph Docker image ***"
docker tag consumergraph consumergraph:latest
docker tag consumergraph consumergraph:"${VERSION}"

