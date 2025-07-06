FROM prefecthq/prefect:2.16.4-python3.10

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["prefect", "agent", "start", "--pool", "artigo-pool", "--work-queue", "queue-pool"]