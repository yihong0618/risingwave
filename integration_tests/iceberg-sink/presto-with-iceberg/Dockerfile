# Docker hub ahanaio account is discontinued.
# use ghcr.io/popsql/prestodb-sandbox instead of ahanaio/prestodb-sandbox
FROM ghcr.io/popsql/prestodb-sandbox

COPY ./iceberg.properties /opt/presto-server/etc/catalog
COPY ./log.properties /opt/presto-server/etc
COPY ./hadoop-catalog.xml /etc/iceberg/conf/hadoop-catalog.xml
