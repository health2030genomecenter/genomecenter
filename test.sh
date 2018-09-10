#!/usr/bin/env bash

IMAGENAME=docker-centos7-slurm-sbt:18.08.8-1.2.1

cd docker/slurm-sbt-base && docker build -t $IMAGENAME .  && cd ../..

NAME=testcontainer

docker run --detach --name $NAME  -h ernie $IMAGENAME tail -f /dev/null 

docker cp . $NAME:/opt/

docker exec $NAME /bin/bash -c "cd /opt && sbt test"

docker rm -f -v $NAME