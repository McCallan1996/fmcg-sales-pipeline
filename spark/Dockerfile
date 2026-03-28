FROM bitnami/spark:3.5

USER root

# gcs + bigquery connectors for spark
RUN pip install --no-cache-dir \
    google-cloud-storage==2.18.2 \
    google-cloud-bigquery==3.25.0

USER 1001
