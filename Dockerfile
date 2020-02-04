FROM python:alpine 

LABEL maintainer="Ilija Vukotic"

ENV PYTHONUNBUFFERED 1

RUN pip install --no-cache-dir -r requirements.txt

COPY src /src/

EXPOSE 5000

ENTRYPOINT ["python", "/src/app.py"]