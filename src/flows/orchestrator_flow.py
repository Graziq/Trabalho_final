from prefect import flow, task
import os
import sys

# Garante que o diretório raiz do projeto esteja no Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

# Importa o flow de simulação
from src.flows.resultados2 import simulacao_contingencia_flow

# REMOVIDO: A importação de run_dash_server_as_subprocess não é mais necessária aqui.
# from src.flows.visualizacao_impacto import run_dash_server_as_subprocess

# REMOVIDO: A task launch_dash_app_orchestrator_task não é mais necessária aqui.
# @task(name="Lançar Aplicação Dash", log_prints=True)
# def launch_dash_app_orchestrator_task():
#     """
#     Task Prefect que invocava a função para lançar a aplicação Dash em subprocesso.
#     Esta task foi removida do flow principal e não deve ser executada pelo schedule.
#     """
#     print("Esta task de lançamento do Dash foi removida do flow orquestrador agendado.")
#     # run_dash_server_as_subprocess() # Esta linha também foi removida ou a função não existe mais aqui

@flow(name="simulacao-e-visualizacao-orchestrator", log_prints=True)
def simulacao_e_visualizacao_orchestrator(
    n_cenarios: int = 2, vmax: float = 1.093, vmin: float = 0.94, line_loading_max: float = 120
):
    """
    FLOW: Orquestra APENAS a simulação de contingências.
    A aplicação de visualização Dash é esperada para ser lançada
    como um processo separado e persistente, e não por este flow.
    """
    print("Iniciando o flow orquestrador 'simulacao-e-visualizacao-orchestrator'...")

    # 1. Executa o flow de simulação de contingências
    print("Executando o flow 'simulacao_contingencia_flow'...")
    simulacao_contingencia_flow(
        n_cenarios=n_cenarios,
        vmax=vmax,
        vmin=vmin,
        line_loading_max=line_loading_max
    )
    print("Flow de simulação concluído. Dados salvos no PostgreSQL.")
    print("O Dash (se estiver rodando) buscará automaticamente os novos dados.")

    # REMOVIDO: A chamada para a task de lançamento do Dash foi removida daqui.
    # print("Iniciando task para lançar a aplicação de visualização Dash...")
    # launch_dash_app_orchestrator_task()

    print("Flow orquestrador concluído.")

# Para executar este orchestrator flow diretamente para teste (opcional)
if __name__ == "__main__":
    print("Executando orchestrator_flow.py diretamente para teste...")
    simulacao_e_visualizacao_orchestrator(n_cenarios=1) # Exemplo com 1 cenário para teste rápido
    print("Teste direto do orchestrator_flow concluído.")