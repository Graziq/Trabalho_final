import pandapower as pp
import pandapower.networks as pn
import random
from pandapower.topology import create_nxgraph
import networkx as nx
import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context
import io
import sys
import os

# --- 1. Defina um diretório base para todos os arquivos de saída ---
# Este é o caminho ABSOLUTO no seu sistema onde você quer que os arquivos sejam salvos.
# Substitua pelo caminho real no seu computador.
# Ex: r"C:\Users\graz1\Documents\MeusProjetos\Trabalho_Final_Resultados"
BASE_OUTPUT_DIR = r"C:\Users\graz1\UFF\mestrado\Trabalho_final\simulacao_resultados"

# Crie o diretório se ele não existir
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- Adicione esta linha AQUI ---
print(f"DEBUG: pandapower version in worker: {pp.__version__}")
# --- Fim da linha a ser adicionada ---

# --- Funções/Tasks Auxiliares Inalteradas ---

@task
def criar_rede_ieee30_slack_bar():
    """
    Cria a rede IEEE 30 barras e configura a barra slack,
    garantindo que os limites de reativos dos geradores estejam bem definidos.
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
def gerar_dados_cenario(net, cenario_id=None):
    """
    Gera um conjunto de dados aleatórios para um cenário específico,
    introduzindo variações nas cargas e geradores da rede.
    """
    dados = {}
    for idx in net.load.index:
        p_original = net.load.at[idx, 'p_mw']
        q_original = net.load.at[idx, 'q_mvar']
        dados[f'carga_p_mw_{idx}'] = p_original * random.uniform(0.95, 1.05)
        dados[f'carga_q_mvar_{idx}'] = q_original * random.uniform(0.90, 1.10)

    for idx in net.gen.index:
        p_original = net.gen.at[idx, 'p_mw']
        vm_original = net.gen.at[idx, 'vm_pu']
        q_mvar_min_original = net.gen.at[idx, 'q_mvar_min']
        q_mvar_max_original = net.gen.at[idx, 'q_mvar_max']

        dados[f'gen_p_mw_{idx}'] = p_original * random.uniform(0.95, 1.05)
        if net.gen.at[idx, 'slack']:
            dados[f'gen_vm_pu_{idx}'] = vm_original
        else:
            dados[f'gen_vm_pu_{idx}'] = vm_original * random.uniform(0.995, 1.005)

        new_q_mvar_min = q_mvar_min_original * random.uniform(0.90, 1.10)
        new_q_mvar_max = q_mvar_max_original * random.uniform(0.90, 1.10)
        dados[f'gen_q_mvar_min_{idx}'] = min(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_q_mvar_max_{idx}'] = max(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_slack_{idx}'] = net.gen.at[idx, 'slack']

    if not net.shunt.empty:
        for idx in net.shunt.index:
            q_original = net.shunt.at[idx, 'q_mvar']
            dados[f'shunt_q_mvar_{idx}'] = q_original * random.uniform(0.90, 1.10)

    if cenario_id is not None:
        dados['cenario'] = cenario_id
    return dados

@task
def aplicar_dados_ao_net(net, dados):
    """
    Aplica os dados de um cenário específico (gerados por gerar_dados_cenario)
    à rede pandapower.
    """
    # É importante copiar a rede ANTES de aplicar os dados se você quiser manter a rede original intacta
    # para múltiplos cenários ou operações futuras.
    
    net_copy = pp.from_json_string(pp.to_json(net))

    
    for idx in net_copy.load.index:
        net_copy.load.at[idx, 'p_mw'] = dados[f'carga_p_mw_{idx}']
    for idx in net_copy.load.index: # Loop separado para q_mvar para evitar erros de indexação
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
            
    return net_copy # Retorna a rede copiada e modificada

@task(retries=3, retry_delay_seconds=10)
def rodar_fluxo_potencia(net):
    """
    Executa o fluxo de potência usando pandapower.
    Retorna o objeto da rede com os resultados e um booleano de convergência.
    """
    try:
        pp.runpp(net, numba=False, init='flat')
        return net, True
    except pp.LoadflowNotConverged:
        print("Fluxo de potência NÃO convergiu.")
        # Se não convergir, preenche os resultados de tensão com NaN para manter a estrutura
        for bus_idx in net.bus.index:
            if bus_idx not in net.res_bus.index: # Garante que o índice existe
                net.res_bus.loc[bus_idx, 'vm_pu'] = np.nan
        return net, False

@task
def simular_desligamento_e_verificar_ilhamento(net_copy, linha):
    """
    Prepara uma rede para uma contingência de linha, desliga a linha
    e verifica se houve ilhamento. Retorna a rede modificada e o status de ilhamento.
    """
    net_contingencia_copy = pp.from_json_string(pp.to_json(net_copy)) # Garante que a contingência não afete o net_copy original
    net_contingencia_copy.line.at[linha, 'in_service'] = False

    ilhamento_detectado = False
    graph = create_nxgraph(net_contingencia_copy, respect_switches=True)
    slack_buses = net_contingencia_copy.gen[net_contingencia_copy.gen['slack']].bus.values
    componentes = list(nx.connected_components(graph))

    for component in componentes:
        component_set = set(component)
        slack_present_in_component = any(bus in component_set for bus in slack_buses)
        if not slack_present_in_component:
            ilhamento_detectado = True
            break

    return net_contingencia_copy, ilhamento_detectado

@task
def salvar_resultados_globais(resultados, output_filename='resultados_simulacao_ieee30_detalhado.csv'):
    """
    Salva os resultados globais de todas as contingências em um CSV.
    """
    output_path = os.path.join(BASE_OUTPUT_DIR, output_filename) # Usa o BASE_OUTPUT_DIR
    df_resultados_finais = pd.DataFrame(resultados)
    df_resultados_finais.to_csv(output_path, index=False)
    print(f"\nResultados detalhados (todas as contingências) salvos em '{output_path}'.")
    run_context = get_run_context()
    if run_context:
        create_markdown_artifact(
            f"Resultados da simulação salvos em: `{output_path}`",
            key="resultados-simulacao",
            description="Sumário dos resultados de todas as contingências."
        )


@task
def salvar_tensao_nao_criticos(tensao_data, output_filename='tensao_barras_nao_criticos_ieee30.csv'):
    """
    Salva os dados de tensão para contingências NÃO CRÍTICAS em um CSV.
    """
    output_path = os.path.join(BASE_OUTPUT_DIR, output_filename) # Usa o BASE_OUTPUT_DIR
    if tensao_data:
        df_tensao_nao_criticos = pd.DataFrame(tensao_data)
        # Reordena as colunas para melhor visualização
        cols = ['cenario', 'linha_desligada', 'from_bus', 'to_bus'] + \
               sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_antes_bus_')]) + \
               sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_depois_bus_')])
        df_tensao_nao_criticos = df_tensao_nao_criticos[cols]
        df_tensao_nao_criticos.to_csv(output_path, index=False)
        print(f"Dados de tensão para contingências NÃO CRÍTICAS salvos em '{output_path}'.")
        run_context = get_run_context()
        if run_context:
            create_markdown_artifact(
                f"Dados de tensão para cenários não críticos salvos em: `{output_path}`",
                key="tensao-nao-criticos",
                description="Tensões antes e depois para contingências sem criticidade."
            )
        return df_tensao_nao_criticos # Retorna o DataFrame para ser usado no próximo flow (se aplicável, mas não diretamente neste caso)
    else:
        print("Nenhuma contingência não crítica foi encontrada para salvar dados de tensão.")
        return pd.DataFrame() # Retorna um DataFrame vazio


@task
def analisar_impacto_tensao_formato_novo(input_df_tensao, output_filename='impacto_tensao_barras_completo_ieee30.csv', num_barras=30):
    """
    Analisa o impacto do desligamento de linhas nas tensões das barras
    e gera um CSV onde cada barra tem sua própria coluna com o valor da diferença de tensão (impacto).
    """
    output_csv_path = os.path.join(BASE_OUTPUT_DIR, output_filename) # Usa o BASE_OUTPUT_DIR

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

        df_impacto_novo_formato.to_csv(output_csv_path, index=False) # Usa o novo caminho
        print(f"\nAnálise de impacto concluída. Dados de impacto por barra salvos em '{output_csv_path}'.")

        run_context = get_run_context()
        if run_context:
            run_name = run_context.flow_run.name
            markdown_report = f"""
### Relatório de Impacto de Tensão
**Flow Run**: `{run_name}`

Arquivo de impacto salvo em: `{output_csv_path}`
Número de registros de impacto: {len(df_impacto_novo_formato)}
"""
            create_markdown_artifact(markdown_report, key="impacto-tensao-relatorio", description="Relatório do impacto de tensão nas barras.")
    else:
        print("\nNão foram encontrados dados de impacto de tensão para salvar no novo formato.")

# --- FLOW 1: Simulação de Contingências ---
@flow(name="Simulacao de Contingencia N-1 IEEE 30 Barras", log_prints=True)
def simulacao_contingencia_flow(n_cenarios: int = 2, vmax: float = 1.093, vmin: float = 0.94, line_loading_max: float = 120):
    """
    FLOW: Orquestra a simulação de contingências N-1 na rede IEEE 30 barras,
    salvando os resultados e os dados de tensão para posterior análise de impacto.
    """
    print(f"Iniciando simulação com {n_cenarios} cenários para IEEE 30 barras...")

    # 1. Carrega a rede base
    net_base = criar_rede_ieee30_slack_bar()

    # 2. Roda o fluxo de potência inicial da rede base
    net_base_result, convergencia_base = rodar_fluxo_potencia(net_base)

    if convergencia_base:
        print("Rede base carregada:")
        print(f" - Tensão mínima: {net_base_result.res_bus.vm_pu.min():.4f} pu")
        print(f" - Tensão máxima: {net_base_result.res_bus.vm_pu.max():.4f} pu")
        if not net_base_result.res_line.empty:
            print(f" - Carregamento máximo de linha: {net_base_result.res_line.loading_percent.max():.2f} %")
        print("------")
    else:
        print("A rede base não convergiu. A simulação não pode continuar.")
        return # Encerra o flow se a base não convergir

    resultados_globais = []
    tensao_cenarios_nao_criticos_para_csv = []

    linhas_para_testar = list(net_base_result.line.index)

    for cenario_id in range(n_cenarios):
        print(f"\nSimulando Cenário {cenario_id}...")
        dados_cenario = gerar_dados_cenario(net_base_result, cenario_id)

        # 3. Aplica dados de cenário a uma cópia da rede base
        net_cenario_inicial = aplicar_dados_ao_net(net_base_result, dados_cenario)

        # 4. Roda o fluxo de potência para o cenário ANTES de qualquer contingência
        net_cenario_result, convergencia_inicial = rodar_fluxo_potencia(net_cenario_inicial)

        if not convergencia_inicial:
            print(f"Cenário {cenario_id}: Fluxo de potência inicial NÃO convergiu, ignorando contingências para este cenário.")
            resultados_globais.append({
                'cenario': cenario_id,
                'linha_desligada': 'N/A',
                'status': 'cenário inicial não convergiu',
                'ilhamento': False,
                'num_componentes_conectados': None,
                'convergencia': False
            })
            continue

        tensao_antes_contingencia = net_cenario_result.res_bus.vm_pu.to_dict()

        linhas_criticas_cenario_resumo = []

        for linha in linhas_para_testar:
            # 5. Simula desligamento e verifica ilhamento
            net_pos_desligamento, ilhamento_detectado = simular_desligamento_e_verificar_ilhamento(net_cenario_result, linha)

            tensao_apos_contingencia = {bus: np.nan for bus in net_pos_desligamento.bus.index}
            convergencia_pos_contingencia = False
            status_contingencia = 'normal' # Default para 'normal'

            if ilhamento_detectado:
                print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Ilhamento detectado")
                linhas_criticas_cenario_resumo.append(linha)
                status_contingencia = 'ilhamento'
            else:
                # 6. Roda o fluxo de potência pós-contingência
                net_final_contingencia, convergencia_pos = rodar_fluxo_potencia(net_pos_desligamento)
                convergencia_pos_contingencia = convergencia_pos

                if convergencia_pos_contingencia:
                    # 7. Verifica criticidade (tensão e carregamento)
                    vm_min = float(net_final_contingencia.res_bus.vm_pu.min())
                    vm_max = float(net_final_contingencia.res_bus.vm_pu.max())
                    loading_max = float(net_final_contingencia.res_line.loading_percent.max())

                    if vm_min < vmin:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão mínima ({vm_min:.4f} pu) abaixo do limite ({vmin:.4f} pu)")
                        status_contingencia = 'crítica'
                    elif vm_max > vmax:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão máxima ({vm_max:.4f} pu) acima do limite ({vmax:.4f} pu)")
                        status_contingencia = 'crítica'
                    elif loading_max > line_loading_max:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Carregamento de linha ({loading_max:.2f} %) excedido ({line_loading_max:.2f} %)")
                        status_contingencia = 'crítica'
                    
                    # Se não for crítica, coleta os dados de tensão para análise de impacto
                    if status_contingencia == 'normal':
                        tensao_apos_contingencia = net_final_contingencia.res_bus.vm_pu.to_dict()
                        row_data = {
                            'cenario': cenario_id,
                            'linha_desligada': linha,
                            'from_bus': net_base_result.line.at[linha, 'from_bus'],
                            'to_bus': net_base_result.line.at[linha, 'to_bus']
                        }
                        for bus_idx, vm_pu in tensao_antes_contingencia.items():
                            row_data[f'vm_pu_antes_bus_{bus_idx}'] = vm_pu
                        for bus_idx, vm_pu in tensao_apos_contingencia.items():
                            row_data[f'vm_pu_depois_bus_{bus_idx}'] = vm_pu
                        tensao_cenarios_nao_criticos_para_csv.append(row_data)
                    else:
                        linhas_criticas_cenario_resumo.append(linha) # Adiciona a linha à lista de críticas se a contingência for crítica

                else: # Não convergiu
                    status_contingencia = 'crítica (não convergiu)'
                    print(f"Cenário {cenario_id}, linha {linha}: ❌ Fluxo não convergiu")
                    linhas_criticas_cenario_resumo.append(linha)

            resultados_globais.append({
                'cenario': cenario_id,
                'linha_desligada': linha,
                'status': status_contingencia,
                'ilhamento': ilhamento_detectado,
                'num_componentes_conectados': len(list(nx.connected_components(create_nxgraph(net_pos_desligamento, respect_switches=True)))) if not ilhamento_detectado else None,
                'convergencia': convergencia_pos_contingencia
            })

        if linhas_criticas_cenario_resumo:
            print(f"\n--- Resumo Cenário {cenario_id}: Linhas que causaram Criticidade/Ilhamento: {linhas_criticas_cenario_resumo} ---")
        else:
            print(f"\n--- Resumo Cenário {cenario_id}: Nenhuma criticidade ou ilhamento detectado. ---")

    # 8. Salva os resultados globais (todas as contingências)
    salvar_resultados_globais(resultados_globais, output_filename='resultados_simulacao_ieee30_detalhado.csv')

    # 9. Salva os dados de tensão para contingências NÃO CRÍTICAS
    salvar_tensao_nao_criticos(tensao_cenarios_nao_criticos_para_csv, output_filename='tensao_barras_nao_criticos_ieee30.csv')


# --- FLOW 2: Análise de Impacto (Separado)
@flow(name="Analise de Impacto de Tensao IEEE 30 Barras", log_prints=True)
def analise_impacto_flow(input_csv_filename: str = 'tensao_barras_nao_criticos_ieee30.csv', num_barras: int = 30):
    """
    FLOW: Carrega os dados de tensão de cenários não críticos de um CSV e
    realiza a análise de impacto, salvando os resultados em um novo CSV.
    """
    input_csv_path = os.path.join(BASE_OUTPUT_DIR, input_csv_filename) # Usa o BASE_OUTPUT_DIR para ler

    print(f"Iniciando análise de impacto de tensão a partir de '{input_csv_path}'...")

    try:
        df_tensao = pd.read_csv(input_csv_path)
        print(f"Dados carregados com sucesso de '{input_csv_path}'.")
    except FileNotFoundError:
        print(f"Erro: Arquivo '{input_csv_path}' não encontrado. Certifique-se de que o 'simulacao_contingencia_flow' foi executado primeiro e que o caminho está correto.")
        return
    except Exception as e:
        print(f"Erro ao carregar o arquivo CSV: {e}")
        return

    # Chama a task para realizar a análise de impacto
    analisar_impacto_tensao_formato_novo(df_tensao, num_barras=num_barras)

    print("Análise de impacto de tensão concluída.")

# --- Execução Principal do Script ---
if __name__ == "__main__":
    n_cenarios_simulacao = 2 # Definir o número de cenários para a simulação

    print(f"--- Executando o Flow de Simulação de Contingências ---")
    # Para rodar o flow de simulação (que gera os CSVs de saída)
    simulacao_contingencia_flow(n_cenarios=n_cenarios_simulacao)

    print(f"\n--- Executando o Flow de Análise de Impacto ---")
    # Para rodar o flow de análise de impacto, usando os dados gerados pelo flow anterior
    analise_impacto_flow(input_csv_filename='tensao_barras_nao_criticos_ieee30.csv', num_barras=30)

    print("\nProcesso completo (simulação e análise) concluído.")