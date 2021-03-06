FROM python:3.7.4

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . /app
WORKDIR /app

RUN pip install -e .
