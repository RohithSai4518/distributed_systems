# Multi-stage container build for Aegis Distributed Systems Engine
FROM python:3.11-slim as builder

WORKDIR /app

COPY . /app

RUN python -m compileall aegis/ web/

EXPOSE 8080 9001 9002 9003 8001 8002 8003

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "main.py"]
CMD ["cluster", "--nodes", "3", "--http-port", "8080"]
