FROM dontan001/spark:v3.3-plugins

USER root
RUN set -ex && \
    mkdir -p /opt/spark/listeners && \
    mkdir -p /opt/spark/listeners/lib
COPY target/scala-2.12/spark-measure_2.12-0.19-SNAPSHOT.jar /opt/spark/listeners
COPY docker/lib /opt/spark/listeners/lib
