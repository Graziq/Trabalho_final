FROM python:3.9-slim as base

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY simulacao_resultados ./simulacao_resultados

# Estágio para Prefect
FROM base as prefect
RUN pip install prefect==2.16.4 pandapower
EXPOSE 4200
CMD ["prefect", "agent", "start", "--pool", "default-agent-pool"]

# Estágio para Visualização
FROM base as visualization
RUN pip install dash plotly pandas
EXPOSE 8050
CMD ["python", "src/flows/visualizacao_impacto.py"]