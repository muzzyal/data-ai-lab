FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

COPY ./.devcontainer/requirements.txt /tmp/pip/

RUN pip3 --no-cache-dir install -r /tmp/pip/requirements.txt \
    && rm -rf /tmp/pip/
