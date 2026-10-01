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
#                                         artifact JAR from RustFS at CREATE FUNCTION time
#   * iceberg-flink-runtime-2.1         — the opt-in lake's Iceberg sink + REST catalog
#   * lake-jars.txt                     — the AWS SDK S3 client S3FileIO needs, and the
#                                         slice of Hadoop that Iceberg's Flink catalog
#                                         factory needs to build a Configuration (the
#                                         s3-fs-hadoop plugin's copy is classloader-isolated
#                                         from /opt/flink/lib). A pinned minimal list, not
#                                         iceberg-aws-bundle + hadoop-client-runtime: see
#                                         the file's header for the CVEs that avoids.
#
# The base image's own RHEL packages (openssl, util-linux, libxml2, ...) carry
# HIGH CVEs this file cannot patch: the image is a stripped UBI with no rpm,
# dnf or microdnf. Only a newer base tag fixes them, and the newest 2.1 tag
# (2.1.3-cp2) trades 4 of them for 22 Go stdlib findings — see docs/lake.md 5.0.
#
# The Iceberg/Hadoop JARs are baked in unconditionally: this image is built by
# `make cp-flink-up`, before anyone chooses ENABLE_LAKE at `cp-flink-reports-up`
# time. Nothing on the reports path loads them — they only matter to the lake
# application (IsotopeLakeJob, docs/lake.md).
#
# The PTF/UDF JAR itself is NOT baked in — it is uploaded to CMF as a cmf://
# artifact and referenced by CREATE FUNCTION ... USING JAR.
#
# Built inside the minikube node (BuildKit → containerd), so the Kubelet uses it
# without a registry pull and no host Docker daemon is needed. See
# `make flink-image-build`:
#   minikube image build -t isotope-cp-flink-sql:local \
#     --build-opt=opt=build-arg:FLINK_IMAGE=<cp-flink tag> \
#     -f flink-sql-isotope.containerfile k8s/base/
ARG FLINK_IMAGE=confluentinc/cp-flink:2.1.2-cp1-java21

# --- fetch stage: download the connector JARs (stock image has no curl) --------
FROM curlimages/curl:8.13.0 AS fetch
ARG KAFKA_CONNECTOR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
ARG AVRO_CONFLUENT_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.1.2/flink-sql-avro-confluent-registry-2.1.2.jar
# Iceberg 1.11.0 is the first release with a Flink 2.1 runtime.
ARG ICEBERG_VER=1.11.0
ARG MAVEN=https://repo1.maven.org/maven2
COPY lake-jars.txt /tmp/lake-jars.txt
RUN curl -fSL -o /tmp/flink-sql-connector-kafka.jar "$KAFKA_CONNECTOR_URL" \
 && curl -fSL -o /tmp/flink-sql-avro-confluent-registry.jar "$AVRO_CONFLUENT_URL" \
 && curl -fSL -o /tmp/iceberg-flink-runtime.jar "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-2.1/$ICEBERG_VER/iceberg-flink-runtime-2.1-$ICEBERG_VER.jar" \
 && mkdir -p /tmp/lake \
 && grep -v -e '^#' -e '^$' /tmp/lake-jars.txt | while read -r path; do \
        curl -fSL -o "/tmp/lake/${path##*/}" "$MAVEN/$path" || exit 1; \
    done

# --- final image ---------------------------------------------------------------
FROM ${FLINK_IMAGE}
COPY --from=fetch /tmp/flink-sql-connector-kafka.jar         /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar
COPY --from=fetch /tmp/flink-sql-avro-confluent-registry.jar /opt/flink/lib/flink-sql-avro-confluent-registry-2.1.2.jar
COPY --from=fetch /tmp/iceberg-flink-runtime.jar             /opt/flink/lib/iceberg-flink-runtime-2.1.jar
COPY --from=fetch /tmp/lake/                                 /opt/flink/lib/
USER root
# Enable the bundled S3 filesystem plugin (present in /opt/flink/opt of the base
# image) by copying it into the active plugins dir.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && chown -R flink:flink /opt/flink/lib /opt/flink/plugins/s3-fs-hadoop
USER flink
