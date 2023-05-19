FROM python:3.10-slim as poetry
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN python -c 'from urllib.request import urlopen; print(urlopen("https://install.python-poetry.org").read().decode())' | python -
COPY ./pyproject.toml ./
COPY ./poetry.lock ./
RUN poetry install --no-interaction --no-ansi -vvv

FROM python:3.10-slim as runtime
ENV PYTHONUNBUFFERED=true
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
COPY --from=poetry /.venv /app/.venv
COPY ./ /app
CMD ["python", "main.py"]