ARG PYTHON_VERSION=3.13.4
FROM python:${PYTHON_VERSION}-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN groupadd --system biblioteca \
    && useradd --system --gid biblioteca --home-dir /app biblioteca

COPY --chown=biblioteca:biblioteca . .

USER biblioteca

EXPOSE 9999

CMD ["python", "-m", "network.servidor"]

