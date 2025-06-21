# Use uma imagem base do Prefect com Python 3.10, compatível com suas libs (Numba/Pandapower)
FROM prefecthq/prefect:2.16.4-python3.10

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia todo o conteúdo do seu projeto (Trabalho_final) para /app dentro do contêiner.
# O 'context: ../' no docker-compose.yml fará com que a pasta 'Trabalho_final' seja o contexto.
# Então, copiar '.' copia tudo desse contexto.
COPY . /app

# Instalar dependências Python a partir do seu requirements.txt
# Isso garante que as mesmas versões que você testou localmente sejam usadas no contêiner.
# Assumimos que requirements.txt está na raiz do seu projeto (Trabalho_final)
RUN pip install --no-cache-dir -r requirements.txt

# Comando para iniciar o agente Prefect
# Este CMD é o padrão para iniciar um agente que observa um work pool específico
CMD ["prefect", "agent", "start", "-p", "default-agent-pool"]