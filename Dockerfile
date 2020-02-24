FROM ubuntu:bionic

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install curl wget -y

COPY src /src/

RUN pip install --no-cache-dir -r /src/requirements.txt

EXPOSE 5000

ENTRYPOINT ["python", "/src/app.py"]