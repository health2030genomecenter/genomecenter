FROM giovtorres/docker-centos7-slurm:18.08.0

RUN yum -y install java unzip

ENV SBT_VERSION 1.2.1

RUN \
  curl -L -o sbt-$SBT_VERSION.rpm https://dl.bintray.com/sbt/rpm/sbt-$SBT_VERSION.rpm && \
  rpm -i sbt-$SBT_VERSION.rpm && \
  rm sbt-$SBT_VERSION.rpm && \
  sbt sbtVersion