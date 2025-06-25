from prefect import flow, task
import time

@task
def minha_task_simples():
    print("Executando minha task simples...")
    time.sleep(2)
    print("Task simples concluída.")

@flow(name="meu-flow-de-teste", log_prints=True)
def meu_flow_de_teste(mensagem: str = "Olá do Prefect!"):
    print(f"Iniciando o flow de teste com a mensagem: {mensagem}")
    minha_task_simples()
    print("Flow de teste concluído.")

# Não inclua o bloco if __name__ == "__main__": aqui!