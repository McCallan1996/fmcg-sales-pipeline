FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --python /home/airflow/.local/bin/python --no-cache -r /tmp/requirements.txt
