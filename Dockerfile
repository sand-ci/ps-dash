FROM ubuntu:noble

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

RUN apt-get update -y

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --allow-unauthenticated \
    curl wget \
    build-essential \
    git \
    python3-pip \
    python3-full \
    rsync \
    software-properties-common \
    unzip \
    zip \
    vim 

COPY src /src/

RUN pip3 install --no-cache-dir --break-system-packages -r /src/requirements.txt

EXPOSE 8050

ENTRYPOINT ["python3", "/src/app.py"]
