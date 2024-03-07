from docker.io/python:3.10

WORKDIR /src

RUN adduser --system --no-create-home nonroot

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==1.8.2

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

COPY . .

RUN chown nonroot: docker-entrypoint.sh
USER nonroot
ENTRYPOINT ["./docker-entrypoint.sh"]