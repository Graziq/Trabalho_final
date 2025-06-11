import pandapower as pp
import pandapower.networks as pn
import random
from pandapower.topology import create_nxgraph
import networkx as nx
import pandas as pd
import numpy as np

from prefect import flow, task # Importa os decoradores flow e task do Prefect
from prefect.artifacts import create_markdown_artifact # Opcional: para criar relatórios na UI do Prefect
from prefect.context import get_run_context # Opcional: para obter informações do run do Prefect

# --- Tarefas (Tasks) ---

@task
def criar_rede_ieee30_slack_bar_task():
    """
    TASK: Cria a rede IEEE 30 barras e configura a barra slack,
    garantindo que os limites de reativos dos geradores estejam bem definidos.
    Retorna o objeto da rede.
    """
    net = pn.case30()

    if 'q_mvar_min' not in net.gen.columns:
        net.gen['q_mvar_min'] = 0.0
    if 'q_mvar_max' not in net.gen.columns:
        net.gen['q_mvar_max'] = 0.0

    for idx in net.gen.index:
        q_min = net.gen.at[idx, 'q_mvar_min']
        q_max = net.gen.at[idx, 'q_mvar_max']

        if pd.isna(q_min) or q_min == 0.0:
            net.gen.at[idx, 'q_mvar_min'] = -50.0
        
        if pd.isna(q_max) or q_max == 0.0:
            net.gen.at[idx, 'q_mvar_max'] = 50.0
            
    net.gen['slack'] = False
    gen_idx_at_bus0 = net.gen[net.gen['bus'] == 0].index
    if not gen_idx_at_bus0.empty:
        net.gen.at[gen_idx_at_bus0[0], 'slack'] = True
    else:
        pp.create_gen(net, bus=0, p_mw=0, vm_pu=1.0, slack=True, q_mvar_min=-50, q_mvar_max=50) 
    
    return net

@task
def gerar_dados_cenario_task(net_base, cenario_id):
    """
    TASK: Gera um conjunto de dados aleatórios para um cenário específico,
    introduzindo variações nas cargas e geradores da rede.
    Retorna um dicionário com os dados do cenário.
    """
    dados = {}
    
    for idx in net_base.load.index:
        p_original = net_base.load.at[idx, 'p_mw']
        q_original = net_base.load.at[idx, 'q_mvar']
        dados[f'carga_p_mw_{idx}'] = p_original * random.uniform(0.95, 1.05)
        dados[f'carga_q_mvar_{idx}'] = q_original * random.uniform(0.90, 1.10)

    for idx in net_base.gen.index:
        p_original = net_base.gen.at[idx, 'p_mw']
        vm_original = net_base.gen.at[idx, 'vm_pu']
        q_mvar_min_original = net_base.gen.at[idx, 'q_mvar_min']
        q_mvar_max_original = net_base.gen.at[idx, 'q_mvar_max']
        
        dados[f'gen_p_mw_{idx}'] = p_original * random.uniform(0.95, 1.05)
        if net_base.gen.at[idx, 'slack']:
            dados[f'gen_vm_pu_{idx}'] = vm_original
        else:
            dados[f'gen_vm_pu_{idx}'] = vm_original * random.uniform(0.995, 1.005)

        new_q_mvar_min = q_mvar_min_original * random.uniform(0.90, 1.10)
        new_q_mvar_max = q_mvar_max_original * random.uniform(0.90, 1.10)
        dados[f'gen_q_mvar_min_{idx}'] = min(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_q_mvar_max_{idx}'] = max(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_slack_{idx}'] = net_base.gen.at[idx, 'slack']

    if not net_base.shunt.empty:
        for idx in net_base.shunt.index:
            q_original = net_base.shunt.at[idx, 'q_mvar']
            dados[f'shunt_q_mvar_{idx}'] = q_original * random.uniform(0.90, 1.10)

    dados['cenario'] = cenario_id
    return dados

@task
def aplicar_dados_ao_net_task(net, dados):
    """
    TASK: Aplica os dados de um cenário específico à rede pandapower.
    Retorna o objeto da rede modificado.
    """
    # Cria uma cópia profunda da rede para evitar modificações indesejadas
    # em outras tasks que possam estar usando a mesma rede base.
    net_copy = pp.copy_net(net) 

    for idx in net_copy.load.index:
        net_copy.load.at[idx, 'p_mw'] = dados[f'carga_p_mw_{idx}']
        net_copy.load.at[idx, 'q_mvar'] = dados[f'carga_q_mvar_{idx}']

    for idx in net_copy.gen.index:
        net_copy.gen.at[idx, 'p_mw'] = dados[f'gen_p_mw_{idx}']
        net_copy.gen.at[idx, 'vm_pu'] = dados[f'gen_vm_pu_{idx}']
        net_copy.gen.at[idx, 'q_mvar_min'] = dados[f'gen_q_mvar_min_{idx}']
        net_copy.gen.at[idx, 'q_mvar_max'] = dados[f'gen_q_mvar_max_{idx}']
        net_copy.gen.at[idx, 'slack'] = dados[f'gen_slack_{idx}']

    if not net_copy.shunt.empty:
        for idx in net_copy.shunt.index:
            net_copy.shunt.at[idx, 'q_mvar'] = dados[f'shunt_q_mvar_{idx}']
    return net_copy


@task(retries=3, retry_delay_seconds=10) # Tenta rodar 3 vezes com 10s de atraso em caso de falha
def rodar_fluxo_potencia_task(net):
    """
    TASK: Roda o fluxo de potência para a rede.
    Retorna as tensões das barras (vm_pu) como um dicionário,
    ou um dicionário de NaNs se não convergir.
    """
    try:
        pp.runpp(net, numba=False, init='flat')
        return net.res_bus.vm_pu.to_dict(), True # Retorna o resultado e um flag de sucesso
    except pp.LoadflowNotConverged:
        print("Fluxo de potência NÃO convergiu.")
        # Retorna NaN para todas as barras se não convergir
        return {bus: float('nan') for bus in net.bus.index}, False


@task
def desligar_linha_e_verificar_ilhamento_task(net_base, linha, dados_cenario):
    """
    TASK: Prepara uma rede para uma contingência de linha, desliga a linha
    e verifica se houve ilhamento.
    Retorna a rede modificada, o status de ilhamento e os dados da linha.
    """
    net = criar_rede_ieee30_slack_bar_task.run() # Cria uma nova rede para a contingência
    net = aplicar_dados_ao_net_task.run(net, dados_cenario) # Aplica os dados do cenário
    
    # Desliga a linha
    original_line_status = net.line.at[linha, 'in_service']
    net.line.at[linha, 'in_service'] = False

    ilhamento_detectado = False
    status_contingencia = 'normal'

    graph = create_nxgraph(net, respect_switches=True)
    slack_buses = net.gen[net.gen['slack']].bus.values
    componentes = list(nx.connected_components(graph))

    for component in componentes:
        component_set = set(component)
        slack_present_in_component = any(bus in component_set for bus in slack_buses)
        if not slack_present_in_component:
            ilhamento_detectado = True
            status_contingencia = 'ilhamento'
            break

    # Reverte o status da linha na cópia para que a task subsequente receba uma rede consistente
    # net.line.at[linha, 'in_service'] = original_line_status # Isso não é necessário se criamos uma cópia limpa a cada iteração

    return net, ilhamento_detectado, status_contingencia, {
        'from_bus': net_base.line.at[linha, 'from_bus'],
        'to_bus': net_base.line.at[linha, 'to_bus']
    }

@task
def verificar_criticidade_task(net, vmax, vmin, line_loading_max):
    """
    TASK: Verifica criticidade de tensão e carregamento da rede após um fluxo de potência.
    Retorna o status da criticidade.
    """
    status_contingencia = 'normal'
    try:
        vm_min = float(net.res_bus.vm_pu.min())
        vm_max = float(net.res_bus.vm_pu.max())
        loading_max = float(net.res_line.loading_percent.max())

        if vm_min < vmin:
            status_contingencia = 'crítica'
        elif vm_max > vmax:
            status_contingencia = 'crítica'
        elif loading_max > line_loading_max:
            status_contingencia = 'crítica'
    except Exception: # Caso os resultados não existam (ex: fluxo não convergiu)
        status_contingencia = 'crítica (dados ausentes)'
        
    return status_contingencia

@task
def processar_cenario_nao_critico_task(cenario_id, linha_desligada, line_info, tensao_antes, tensao_depois):
    """
    TASK: Formata os dados de tensão para um cenário não crítico em um dicionário.
    Retorna o dicionário com os dados formatados.
    """
    row_data = {
        'cenario': cenario_id,
        'linha_desligada': linha_desligada,
        'from_bus': line_info['from_bus'],
        'to_bus': line_info['to_bus']
    }
    for bus_idx, vm_pu in tensao_antes.items():
        row_data[f'vm_pu_antes_bus_{bus_idx}'] = vm_pu
    for bus_idx, vm_pu in tensao_depois.items():
        row_data[f'vm_pu_depois_bus_{bus_idx}'] = vm_pu
    return row_data

@task
def salvar_resultados_globais_task(resultados, output_path='resultados_simulacao_ieee30_detalhado.csv'):
    """
    TASK: Salva os resultados globais de todas as contingências em um CSV.
    """
    df_resultados_finais = pd.DataFrame(resultados)
    df_resultados_finais.to_csv(output_path, index=False)
    print(f"\nResultados detalhados (todas as contingências) salvos em '{output_path}'.")

@task
def salvar_tensao_nao_criticos_task(tensao_data, output_path='tensao_barras_nao_criticos_ieee30.csv'):
    """
    TASK: Salva os dados de tensão para contingências NÃO CRÍTICAS em um CSV.
    """
    if tensao_data:
        df_tensao_nao_criticos = pd.DataFrame(tensao_data)
        # Reordenar colunas para melhor visualização (opcional)
        cols = ['cenario', 'linha_desligada', 'from_bus', 'to_bus'] + \
               sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_antes_bus_')]) + \
               sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_depois_bus_')])
        df_tensao_nao_criticos = df_tensao_nao_criticos[cols]
        df_tensao_nao_criticos.to_csv(output_path, index=False)
        print(f"Dados de tensão para contingências NÃO CRÍTICAS salvos em '{output_path}'.")
        return df_tensao_nao_criticos
    else:
        print("Nenhuma contingência não crítica foi encontrada para salvar dados de tensão.")
        return pd.DataFrame() # Retorna um DataFrame vazio


@task
def analisar_impacto_tensao_final_task(input_df_tensao, output_csv='impacto_tensao_barras_completo_ieee30.csv', num_barras=30):
    """
    TASK: Analisa o impacto do desligamento de linhas nas tensões das barras
    e gera um CSV onde cada barra tem sua própria coluna com o valor do impacto.
    """
    if input_df_tensao.empty:
        print("DataFrame de entrada para análise de impacto está vazio. Nenhuma análise será realizada.")
        return

    resultados_impacto_novo_formato = []
    colunas_barras = [f'barra_{i}' for i in range(num_barras)]

    for index, row in input_df_tensao.iterrows():
        cenario_id = row['cenario']
        linha_desligada = row['linha_desligada']
        
        linha_resultado = {
            'cenario': cenario_id,
            'linha_desligada': linha_desligada
        }

        for i in range(num_barras):
            col_antes = f'vm_pu_antes_bus_{i}'
            col_depois = f'vm_pu_depois_bus_{i}'

            tensao_antes = row.get(col_antes)
            tensao_depois = row.get(col_depois)

            if pd.isna(tensao_antes) or pd.isna(tensao_depois):
                diferenca = np.nan
            else:
                diferenca = abs(tensao_depois - tensao_antes)
            
            linha_resultado[f'barra_{i}'] = diferenca
        
        resultados_impacto_novo_formato.append(linha_resultado)

    df_impacto_novo_formato = pd.DataFrame(resultados_impacto_novo_formato)
    
    if not df_impacto_novo_formato.empty:
        cols_ordered = ['cenario', 'linha_desligada'] + colunas_barras
        df_impacto_novo_formato = df_impacto_novo_formato[cols_ordered]

        df_impacto_novo_formato.to_csv(output_csv, index=False)
        print(f"\nAnálise de impacto concluída. Dados de impacto por barra salvos em '{output_csv}'.")
        
        # Opcional: Criar um artefato de markdown no Prefect UI
        run_name = get_run_context().flow_run.name
        markdown_report = f"""
### Relatório de Impacto de Tensão
**Flow Run**: `{run_name}`

Arquivo de impacto salvo em: `{output_csv}`
Número de registros de impacto: {len(df_impacto_novo_formato)}
"""
        create_markdown_artifact(markdown_report, key="impacto-tensao-relatorio", description="Relatório do impacto de tensão nas barras.")
    else:
        print("\nNão foram encontrados dados de impacto de tensão para salvar no novo formato.")


# --- Flow (Fluxo Principal) ---

@flow(name="Simulacao de Contingencia N-1 IEEE 30 Barras", log_prints=True)
def simulacao_contingencia_flow(n_cenarios: int = 2, vmax: float = 1.093, vmin: float = 0.94, line_loading_max: float = 120):
    """
    FLOW: Orquestra a simulação de contingências N-1 na rede IEEE 30 barras.
    """
    print(f"Iniciando simulação com {n_cenarios} cenários para IEEE 30 barras...")

    # 1. Cria a rede base
    net_base = criar_rede_ieee30_slack_bar_task.submit() # `.submit()` faz a task rodar assincronamente e retorna um Future

    # 2. Roda o fluxo de potência inicial da rede base (para logs)
    tensao_base_dict, _ = rodar_fluxo_potencia_task.submit(net_base).wait() # `.wait()` espera o resultado do Future

    # Opcional: Log inicial da rede base
    if tensao_base_dict:
        print("Rede base carregada:")
        print(f" - Tensão mínima: {min(tensao_base_dict.values()):.4f} pu")
        print(f" - Tensão máxima: {max(tensao_base_dict.values()):.4f} pu")
        # Não temos o loading da rede base diretamente aqui, precisaria de outra task.
        # print(f" - Carregamento máximo de linha: {net_base.res_line.loading_percent.max():.2f} %")
    print("------")

    resultados_globais = []
    tensao_cenarios_nao_criticos_para_csv = []

    linhas_para_testar = list(net_base.result().line.index) # Pega o objeto net do Future para acessar as linhas

    # Loop principal para simular cada cenário
    for cenario_id in range(n_cenarios):
        print(f"\nSimulando Cenário {cenario_id}...")
        
        # Gera e aplica dados de cenário
        dados_cenario = gerar_dados_cenario_task.submit(net_base.result(), cenario_id)
        net_cenario_inicial = aplicar_dados_ao_net_task.submit(net_base.result(), dados_cenario)
        
        # Roda o fluxo de potência para o cenário base ANTES de qualquer contingência
        tensao_antes_contingencia, convergiu_antes = rodar_fluxo_potencia_task.submit(net_cenario_inicial).wait()

        if not convergiu_antes:
            print(f"Cenário {cenario_id}: Fluxo de potência inicial NÃO convergiu, ignorando contingências para este cenário.")
            # Se o cenário inicial não convergir, registramos isso e pulamos as contingências para ele.
            resultados_globais.append({
                'cenario': cenario_id,
                'linha_desligada': 'N/A',
                'status': 'cenário inicial não convergiu',
                'ilhamento': False,
                'num_componentes_conectados': None,
                'convergencia': False
            })
            continue # Pula para o próximo cenario_id
        
        linhas_criticas_cenario_resumo = []

        # Loop para testar o desligamento de cada linha como contingência N-1
        for linha in linhas_para_testar:
            # Desliga a linha e verifica ilhamento
            net_contingencia_future, ilhamento_detectado, status_contingencia_ilhamento, line_info = \
                desligar_linha_e_verificar_ilhamento_task.submit(net_base, linha, dados_cenario).wait()

            tensao_apos_contingencia = {bus: float('nan') for bus in net_contingencia_future.bus.index}
            convergencia_pos_contingencia = False

            if ilhamento_detectado:
                print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Ilhamento detectado")
                linhas_criticas_cenario_resumo.append(linha)
                status_final = status_contingencia_ilhamento
            else:
                # Roda o fluxo de potência pós-contingência
                tensao_apos_contingencia, convergiu_pos_contingencia = rodar_fluxo_potencia_task.submit(net_contingencia_future).wait()
                
                if convergiu_pos_contingencia:
                    # Verifica criticidade (tensão/carregamento)
                    status_criticidade = verificar_criticidade_task.submit(net_contingencia_future, vmax, vmin, line_loading_max).wait()
                    status_final = status_criticidade

                    if status_final == 'normal':
                        # Processa e coleta dados para cenários não críticos
                        processed_data = processar_cenario_nao_critico_task.submit(
                            cenario_id, linha, line_info, tensao_antes_contingencia, tensao_apos_contingencia
                        ).wait()
                        tensao_cenarios_nao_criticos_para_csv.append(processed_data)
                    else:
                        print(f"Cenário {cenario_id}, linha {linha}: {status_final}")
                        linhas_criticas_cenario_resumo.append(linha)
                else:
                    status_final = 'crítica (não convergiu)'
                    print(f"Cenário {cenario_id}, linha {linha}: ❌ Fluxo não convergiu")
                    linhas_criticas_cenario_resumo.append(linha)
            
            resultados_globais.append({
                'cenario': cenario_id,
                'linha_desligada': linha,
                'status': status_final,
                'ilhamento': ilhamento_detectado,
                'num_componentes_conectados': len(list(nx.connected_components(create_nxgraph(net_contingencia_future, respect_switches=True)))) if not ilhamento_detectado else None,
                'convergencia': convergiu_pos_contingencia # Representa se o fluxo pós-contingência convergiu
            })
        
        if linhas_criticas_cenario_resumo:
            print(f"\n--- Resumo Cenário {cenario_id}: Linhas que causaram Criticidade/Ilhamento: {linhas_criticas_cenario_resumo} ---")
        else:
            print(f"\n--- Resumo Cenário {cenario_id}: Nenhuma criticidade ou ilhamento detectado. ---")

    # Salva os resultados globais (todas as contingências)
    salvar_resultados_globais_task.submit(resultados_globais)

    # Salva e retorna o DataFrame de tensões não críticas para a análise final
    df_tensao_nao_criticos = salvar_tensao_nao_criticos_task.submit(tensao_cenarios_nao_criticos_para_csv).wait()

    # Analisa e salva o impacto final de tensão por barra
    analisar_impacto_tensao_final_task.submit(df_tensao_nao_criticos)

# --- Como Executar ---
# Para rodar este fluxo, salve o código como um arquivo Python (ex: `simulacao_prefect.py`)
# e execute no seu terminal:
# prefect deploy ./simulacao_prefect.py simulacao_contingencia_flow -n "Simulacao de Contingencia de Rede"
# Isso criará um deployment. Depois, você pode rodar:
# prefect run deployment "Simulacao de Contingencia de Rede/simulacao-contingencia-n-1-ieee-30-barras"

# Ou, para um teste local simples:
if __name__ == "__main__":
    simulacao_contingencia_flow(n_cenarios=2) # Execute com 2 cenários para teste