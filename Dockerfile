# Dockerfile
# Use uma imagem base Python com a mesma versão que você usa
FROM python:3.12.5-slim-bookworm 

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo requirements.txt para o diretório de trabalho
COPY requirements.txt .

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o seu código-fonte para o contêiner
COPY src/ /app/src/

# Define a variável de ambiente para o caminho compartilhado de dados
ENV PREFECT_SHARED_DATA_PATH=/app/Trabalho_final/simulacao_resultados

# Comando padrão para iniciar a aplicação Dash (não é necessário CMD se você usar 'command' no docker-compose)
# CMD ["python", "src/flows/visualizacao_impacto.py"] 
# ^^^ Comentado porque você já tem 'command' no docker-compose.yml