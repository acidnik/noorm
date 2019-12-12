FROM python:3.8.0-alpine

RUN apk add --no-cache mysql-dev gcc musl-dev python3-dev ca-certificates libffi-dev
RUN mkdir /app
COPY requirements.txt /app
WORKDIR /app
RUN pip3 install -r requirements.txt
COPY . /app
