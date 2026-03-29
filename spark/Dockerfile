FROM apache/spark:3.5.3

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# gcs + bigquery connectors for spark
RUN uv pip install --system --no-cache \
    google-cloud-storage==2.18.2 \
    google-cloud-bigquery==3.25.0

USER spark
