FROM ubuntu:bionic

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

RUN apt-get update -y

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --allow-unauthenticated \
    curl wget \
    build-essential \
    git \
    python3 \
    python3-pip \
    rsync \
    software-properties-common \
    unzip \
    zip \
    vim 


RUN pip3 install --upgrade pip

COPY src /src/

RUN pip3 install --no-cache-dir -r /src/requirements.txt

EXPOSE 5000

ENTRYPOINT ["python", "/src/app.py"]
