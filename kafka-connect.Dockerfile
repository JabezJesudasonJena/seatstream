# --- Stage 1: Extractor ---
# We use Alpine simply because it has 'tar' built-in
FROM alpine:latest AS extractor

# Copy your local tar.gz file into the extractor
COPY kafka-connect-cassandra-*.tar.gz /tmp/cassandra.tar.gz

# Extract it
RUN mkdir -p /downloads/cassandra-plugin && \
    tar -xzf /tmp/cassandra.tar.gz -C /downloads/cassandra-plugin/ --strip-components=1


# --- Stage 2: Final Image ---
FROM confluentinc/cp-kafka-connect:7.4.0

USER root

# 1. Install the Elasticsearch Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest

# 2. Copy the unzipped plugin from Stage 1
COPY --from=extractor /downloads/cassandra-plugin/ /usr/share/java/kafka-connect-cassandra/

# 3. Fix permissions
RUN chown -R appuser:appuser /usr/share/java/kafka-connect-cassandra/

USER appuser