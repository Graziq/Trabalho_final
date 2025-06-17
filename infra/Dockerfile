FROM prefecthq/prefect:2.16.4-python3.12

# Instalar dependências
RUN pip install pandapower pandas

# Definir diretório de trabalho
WORKDIR /app

# Copiar todos os arquivos necessários
COPY . /app
