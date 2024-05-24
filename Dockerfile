FROM ubuntu

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

RUN apt-get update -y

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --allow-unauthenticated \
    curl wget \
    build-essential \
    git \
    python3 \
    python3-full \
    rsync \
    software-properties-common \
    unzip \
    zip \
    vim 

RUN python -m pip install --upgrade pip

COPY src /src/

RUN python -m pip install --no-cache-dir -r /src/requirements.txt

EXPOSE 8050

ENTRYPOINT ["python3", "/src/app.py"]
