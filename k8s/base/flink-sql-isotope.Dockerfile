# Custom cp-flink image for the CMF SHARED compute pool that runs the isotope
# report statements.
#
# WHY A CUSTOM IMAGE
# A CMF ComputePool's `spec.clusterSpec` supports only flinkVersion / image /
# flinkConfiguration / jobManager / taskManager — there is NO podTemplate, so the
# initContainer trick the raw flink-basic FlinkDeployment used to fetch SQL
# connectors at pod-start is unavailable. Instead we bake the two SQL connectors
# into /opt/flink/lib and pre-enable the S3 filesystem plugin at build time.
#
# Contents added on top of the stock cp-flink image:
#   * flink-sql-connector-kafka         — read source topics / write sink topics
#   * flink-sql-avro-confluent-registry — the avro-confluent sink format (SR-framed)
#   * s3-fs-hadoop plugin (enabled)     — lets the cluster pull the cmf:// PTF
#                                         artifact JAR from MinIO at CREATE FUNCTION time
#
# The PTF/UDF JAR itself is NOT baked in — it is uploaded to CMF as a cmf://
# artifact and referenced by CREATE FUNCTION ... USING JAR.
#
# Build (see `make flink-image-build`), then `minikube image load` it so the
# Kubelet uses it without a registry pull:
#   docker build --build-arg FLINK_IMAGE=<cp-flink tag> \
#     -t isotope-cp-flink-sql:local -f k8s/base/flink-sql-isotope.Dockerfile k8s/base
ARG FLINK_IMAGE=confluentinc/cp-flink:2.1.2-cp1-java21

# --- fetch stage: download the connector JARs (stock image has no curl) --------
FROM curlimages/curl:8.13.0 AS fetch
ARG KAFKA_CONNECTOR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
ARG AVRO_CONFLUENT_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.1.2/flink-sql-avro-confluent-registry-2.1.2.jar
RUN curl -fSL -o /tmp/flink-sql-connector-kafka.jar "$KAFKA_CONNECTOR_URL" \
 && curl -fSL -o /tmp/flink-sql-avro-confluent-registry.jar "$AVRO_CONFLUENT_URL"

# --- final image ---------------------------------------------------------------
FROM ${FLINK_IMAGE}
COPY --from=fetch /tmp/flink-sql-connector-kafka.jar         /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar
COPY --from=fetch /tmp/flink-sql-avro-confluent-registry.jar /opt/flink/lib/flink-sql-avro-confluent-registry-2.1.2.jar
# Enable the bundled S3 filesystem plugin (present in /opt/flink/opt of the base
# image) by copying it into the active plugins dir.
USER root
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && chown -R flink:flink /opt/flink/lib /opt/flink/plugins/s3-fs-hadoop
USER flink
