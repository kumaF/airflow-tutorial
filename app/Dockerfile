FROM python:3.7

ENV DEPENDENCY_VERSION=v0.1

RUN python3.7 -m pip install --upgrade pip

ADD requirements.txt requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    python-dev-is-python3 \
    libpq-dev

RUN python3.7 -m pip install --no-cache-dir -r requirements.txt \
    && rm -rf ~/.cache/pip

ENV CODE_VERSION=v0.5

COPY main.py main.py

ENTRYPOINT [ "python", "main.py" ]