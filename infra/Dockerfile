# Dockerfile (dentro de Trabalho_final/infra/)
FROM prefecthq/prefect:2.16.4-python3.12

# Instalar dependências Python
RUN pip install psycopg2-binary SQLAlchemy pandas pandapower networkx prefect

# Define o diretório de trabalho dentro do contêiner.
# É uma boa prática que este seja o diretório onde seu código principal estará.
WORKDIR /app

# Copia o conteúdo da pasta 'Trabalho_final' (que é o contexto do build, definido no docker-compose)
# para o diretório /app dentro do contêiner.
# O '..' no 'COPY' não é mais necessário aqui porque o 'context' no docker-compose já resolve isso.
# Apenas a pasta 'src/flows/' será copiada para /app se você quiser apenas o código do flow,
# mas geralmente é melhor copiar todo o projeto para que importações relativas funcionem.
COPY ../. /app