from prefect import flow
import os
import sys

# Garante que o diretório raiz do projeto esteja no Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.flows.resultados2 import simulacao_contingencia_flow

@flow(name="simulacao-e-visualizacao-orchestrator", log_prints=True)
def simulacao_e_visualizacao_orchestrator(
    n_cenarios: int = 2, vmax: float = 1.093, vmin: float = 0.94, line_loading_max: float = 120
):
    print("Iniciando o flow orquestrador...")

    # Executa a simulação
    simulacao_contingencia_flow(
        n_cenarios=n_cenarios,
        vmax=vmax,
        vmin=vmin,
        line_loading_max=line_loading_max
    )
    print("Simulação concluída.")

    print("O Dash (rodando em outro container) vai buscar os novos dados automaticamente no Postgres.")

if __name__ == "__main__":
    print("Executando orchestrator_flow.py diretamente para teste...")
    simulacao_e_visualizacao_orchestrator(n_cenarios=1)
    print("Teste direto do orchestrador concluído.")