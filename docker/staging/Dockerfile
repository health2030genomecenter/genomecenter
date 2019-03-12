FROM docker.staging/centos7-slurm-sbt:18.08.8-1.2.1

RUN mkdir -p /data/output

COPY ./ /opt/

ENTRYPOINT ["/opt/bin/pipeline-executor", "-J-Xmx4G"]

