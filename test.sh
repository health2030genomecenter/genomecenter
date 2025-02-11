#!/usr/bin/env bash

IMAGENAME=docker-centos7-slurm-sbt:18.08.8-1.2.1

cd docker/slurm-sbt-base && docker build -t $IMAGENAME .  && cd ../..

GC_ABS_TESTFOLDER=`pwd`/$GC_TESTFOLDER

NAME=`docker run --detach -v $HOME/.ivy2:/root/.ivy2  -v $GC_ABS_TESTFOLDER:/opt/testdata:ro  -h ernie $IMAGENAME tail -f /dev/null`

echo "Container started $NAME"
echo "Copying source tree.."

docker cp -a . $NAME:/opt/

echo "Execute sbt in container.."

docker exec -it $NAME /bin/bash -c "cd /opt && GC_TESTFOLDER=/opt/testdata/ sbt -J-Dsbt.io.jdktimestamps=true 'it:test' ; bash"

docker rm -f -v $NAME