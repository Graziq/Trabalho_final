FROM prefecthq/prefect:2.16.4-python3.12

# Instalar pandapower
RUN pip install pandapower

# Definir diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos do projeto (opcional, pois no docker-compose você já mapeia volumes)
COPY . /app
