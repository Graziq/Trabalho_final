# Use uma única imagem base (Python 3.10 ou 3.12 - escolha uma)
FROM prefecthq/prefect:2.16.4-python3.10

# Define o diretório de trabalho
WORKDIR /app

# Copia o conteúdo do projeto
COPY . /app

# Instala todas as dependências de uma vez (incluindo psycopg2)
RUN pip install --no-cache-dir -r requirements.txt prefect-docker psycopg2-binary

# Comando para iniciar o agente
CMD ["prefect", "agent", "start", "-p", "default-agent-pool"]