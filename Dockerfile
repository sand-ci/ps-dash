FROM python:alpine 

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

# RUN apt-get update
# RUN apt-get install cython3

# RUN yum -y update
# RUN yum install -y \
#     curl \
#     wget \
#     cython3

COPY src /src/

RUN pip install --no-cache-dir numpy

RUN pip install --no-cache-dir -r /src/requirements.txt

EXPOSE 5000

ENTRYPOINT ["python", "/src/app.py"]