FROM python:alpine 

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

COPY src /src/
RUN pip install --no-cache-dir -r /src/requirements.txt

EXPOSE 5000

ENTRYPOINT ["python", "/src/app.py"]