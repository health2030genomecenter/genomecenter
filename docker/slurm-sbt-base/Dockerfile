FROM giovtorres/docker-centos7-slurm:18.08.0

RUN yum -y install java unzip gpg

ENV SBT_VERSION 1.2.1

COPY sbt-public.key.asc .

RUN gpg --import sbt-public.key.asc

RUN \
  curl -L -o sbt-$SBT_VERSION.rpm https://dl.bintray.com/sbt/rpm/sbt-$SBT_VERSION.rpm && \
  curl -L -o sbt-$SBT_VERSION.rpm.asc https://dl.bintray.com/sbt/rpm/sbt-$SBT_VERSION.rpm.asc && \  
  gpg --verify sbt-$SBT_VERSION.rpm.asc sbt-$SBT_VERSION.rpm && \
  rpm --checksig sbt-$SBT_VERSION.rpm && \
  rpm -i sbt-$SBT_VERSION.rpm && \
  rm sbt-$SBT_VERSION.rpm && \
  mkdir -p src/main/scala/ && \
  touch src/main/scala/triggerCompilation.scala && \
  sbt 'set scalaVersion := "2.12.6"' 'compile' && \
  rm -R src 