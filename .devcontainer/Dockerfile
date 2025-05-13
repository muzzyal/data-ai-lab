FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

COPY ./.devcontainer/requirements.txt /tmp/pip/

RUN pip3 --no-cache-dir install -r /tmp/pip/requirements.txt \
    && rm -rf /tmp/pip/

RUN curl -fsSL https://apt.releases.hashicorp.com/gpg | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg \
&& echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(. /etc/os-release; echo "$VERSION_CODENAME") main" \
| tee /etc/apt/sources.list.d/hashicorp.list \
&& apt-get update && apt-get install -y terraform

RUN git config --global --add safe.directory /workspaces
