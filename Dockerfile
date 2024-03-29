FROM docker.io/python:3.10

# Create dirs for:
# - Injecting config.yml: /root/.DANE
# - Mount point for input & output files: /mnt/dane-fs
# - Storing the source code: /src
# - Storing the input file to be used while testing: /src/data
RUN mkdir /root/.DANE /mnt/dane-fs /src /data

ENV DANE_HOME=/root/.DANE

WORKDIR /src

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==1.8.2

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

# Write provenance info about software versions to file
RUN echo "dane-example-worker;https://github.com/beeldengeluid/dane-example-worker/commit/$(git rev-parse HEAD)" >> /software_provenance.txt

COPY . /src

ENTRYPOINT ["./docker-entrypoint.sh"]