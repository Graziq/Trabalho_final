FROM python:3.9-slim as base

WORKDIR /app

# Primeiro copia apenas o requirements.txt para aproveitar cache
COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Depois copia o resto
COPY ./src ./src
COPY ./simulacao_resultados ./simulacao_resultados