"""
ANÁLISE DE CONTINGÊNCIAS EM SISTEMAS DE POTÊNCIA - IEEE 30 BARRAS

Este script realiza:
1. Criação da rede IEEE 30 barras com configuração adequada da barra slack
2. Geração de múltiplos cenários com variações randômicas nas cargas e geradores
3. Simulação de contingências N-1 (desligamento de linhas)
4. Identificação de condições críticas (tensão fora dos limites, sobrecarga, ilhamento)
5. Análise do impacto nas tensões das barras para contingências não críticas
6. Geração de relatórios em CSV para análise posterior
"""


import pandapower as pp # Importa a biblioteca pandapower para modelagem e análise de sistemas de potência
import pandapower.networks as pn # Importa redes padrão do pandapower, como o IEEE 30 barras
import random # Importa para gerar números aleatórios para as variações de cenário
from pandapower.topology import create_nxgraph # Importa para criar um grafo da rede
import networkx as nx # Importa para análise de grafos (conectividade, ilhamento)
import pandas as pd # Importa para manipulação de dados em formato de tabelas (DataFrames)
import numpy as np # Importa para operações numéricas, como NaN (Not a Number)
from random import gauss

def criar_rede_ieee30_slack_bar():
    """Cria a rede IEEE 30 barras com limites exatos do caso original"""
    net = pn.case30()
    
    # --- Configuração dos limites de tensão ---
    net.bus["max_vm_pu"] = 1.06  # Vmax para todas as barras
    net.bus["min_vm_pu"] = 0.94   # Vmin para todas as barras
    
    # --- Configuração dos limites dos geradores ---
    limites_geradores = {
        1: {'p_mw': (0, 360.2), 'q_mvar': (0, 10)},    # Barra 1 (SLACK)
        2: {'p_mw': (0, 140), 'q_mvar': (-40, 50)},     # Barra 2
        5: {'p_mw': (0, 100), 'q_mvar': (-40, 40)},     # Barra 5
        8: {'p_mw': (0, 100), 'q_mvar': (-10, 40)},     # Barra 8
        11: {'p_mw': (0, 100), 'q_mvar': (-6, 24)},     # Barra 11
        13: {'p_mw': (0, 100), 'q_mvar': (-6, 24)}      # Barra 13
    }
    
    for idx in net.gen.index:
        bus = net.gen.at[idx, 'bus']
        if bus in limites_geradores:
            net.gen.at[idx, 'max_q_mvar'] = limites_geradores[bus]['q_mvar'][1]
            net.gen.at[idx, 'min_q_mvar'] = limites_geradores[bus]['q_mvar'][0]
            net.gen.at[idx, 'max_p_mw'] = limites_geradores[bus]['p_mw'][1]
            net.gen.at[idx, 'min_p_mw'] = limites_geradores[bus]['p_mw'][0]
    
    # --- Configuração da barra slack ---
    # Remove a lógica da barra 0 e define a barra 1 como slack (caso original)
    net.gen['slack'] = False
    net.gen.loc[net.gen['bus'] == 1, 'slack'] = True  # Barra 1 é a slack
    
    return net

def gerar_dados_cenario(net, cenario_id=None):
    dados = {}
    
    # --- Cargas ---
    for idx in net.load.index:
        p_original = net.load.at[idx, 'p_mw']
        q_original = net.load.at[idx, 'q_mvar']
        
        # Opção 1: Variações independentes (ajustadas)
        dados[f'carga_p_mw_{idx}'] = p_original * random.uniform(0.98, 1.02)  # ±2% P
        dados[f'carga_q_mvar_{idx}'] = q_original * random.uniform(0.98, 1.02)  # ±2% Q (igual a P)
        
        # Ou Opção 2: Manter fator de potência constante
        # variacao = random.uniform(0.98, 1.02)
        # dados[f'carga_p_mw_{idx}'] = p_original * variacao
        # dados[f'carga_q_mvar_{idx}'] = q_original * variacao

    # --- Geradores ---
    for idx in net.gen.index:
        p_original = net.gen.at[idx, 'p_mw']
        vm_original = net.gen.at[idx, 'vm_pu']
        
        # P ativa: ±5% (adequado para redespacho)
        dados[f'gen_p_mw_{idx}'] = p_original * random.uniform(0.95, 1.05)
        
        # Tensão: ±1% para geradores PV
        if net.gen.at[idx, 'slack']:
            dados[f'gen_vm_pu_{idx}'] = vm_original
        else:
            dados[f'gen_vm_pu_{idx}'] = vm_original * random.uniform(0.99, 1.01)

        # Limites de reativo: ±10% (bom para testar limites)
        #calcula o centro entre qmax e qmin para manter a assimetria entre Q e P
        q_range = net.gen.at[idx, 'q_mvar_max'] - net.gen.at[idx, 'q_mvar_min']
        new_center = (net.gen.at[idx, 'q_mvar_min'] + net.gen.at[idx, 'q_mvar_max'])/2 * random.uniform(0.95,1.05)
        new_q_mvar_min = new_center - q_range/2 * random.uniform(0.9,1.1)
        new_q_mvar_max = new_center + q_range/2 * random.uniform(0.9,1.1)
        dados[f'gen_q_mvar_min_{idx}'] = min(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_q_mvar_max_{idx}'] = max(new_q_mvar_min, new_q_mvar_max)
        
        # Garante que o novo limite mínimo não seja maior que o novo limite máximo.
        dados[f'gen_q_mvar_min_{idx}'] = min(new_q_mvar_min, new_q_mvar_max)
        dados[f'gen_q_mvar_max_{idx}'] = max(new_q_mvar_min, new_q_mvar_max)
        
        # Mantém o status 'slack' do gerador.
        dados[f'gen_slack_{idx}'] = net.gen.at[idx, 'slack']

    # --- Variação para Shunts (Potência Reativa 'Q') --- 
    # if not net.shunt.empty:
    #     for idx in net.shunt.index:
    #         q_original = net.shunt.at[idx, 'q_mvar'] # Potência reativa original do shunt
    #         # Aplica uma variação aleatória de +/- 10% na potência reativa do shunt.
    #         dados[f'shunt_q_mvar_{idx}'] = q_original * random.uniform(0.90, 1.10)

    # Adiciona o ID do cenário aos dados gerados.
    if cenario_id is not None:
        dados['cenario'] = cenario_id
    return dados 

def aplicar_dados_ao_net(net, dados):
    """
    Aplica os dados de um cenário específico (gerados por gerar_dados_cenario)
    à rede pandapower.
    """
    # Aplica dados para Cargas (P_mw e Q_mvar)
    for idx in net.load.index:
        net.load.at[idx, 'p_mw'] = dados[f'carga_p_mw_{idx}']
        net.load.at[idx, 'q_mvar'] = dados[f'carga_q_mvar_{idx}']

    # Aplica dados para Geradores (P_mw, Vm_pu, Q_mvar_min, Q_mvar_max e slack)
    for idx in net.gen.index:
        net.gen.at[idx, 'p_mw'] = dados[f'gen_p_mw_{idx}']
        net.gen.at[idx, 'vm_pu'] = dados[f'gen_vm_pu_{idx}']
        net.gen.at[idx, 'q_mvar_min'] = dados[f'gen_q_mvar_min_{idx}']
        net.gen.at[idx, 'q_mvar_max'] = dados[f'gen_q_mvar_max_{idx}']
        net.gen.at[idx, 'slack'] = dados[f'gen_slack_{idx}']

    # Aplica dados para Shunts (Q_mvar) se existirem
    if not net.shunt.empty:
        for idx in net.shunt.index:
            net.shunt.at[idx, 'q_mvar'] = dados[f'shunt_q_mvar_{idx}']


def simular_cenarios(n_cenarios=1):
    """
    Simula múltiplos cenários de contingência (desligamento de linhas)
    na rede IEEE 30 barras, aplicando variações aleatórias.
    Verifica criticidades (tensão fora dos limites, carregamento de linha excedido, ilhamento)
    e coleta dados de tensão para contingências não críticas.

    Args:
        n_cenarios (int): O número de cenários de simulação a serem executados.

    Returns:
        tuple: Uma tupla contendo:
            - resultados_globais (list): Lista de dicionários com o status de cada contingência.
            - tensao_cenarios_nao_criticos_para_csv (list): Lista de dicionários
              com os dados de tensão antes e depois para cenários não críticos.
    """
    net_base = criar_rede_ieee30_slack_bar() # Cria a rede base (sem variações ou contingências)

    # Executa o fluxo de potência inicial para a rede base e imprime alguns resultados.
    pp.runpp(net_base, numba=True, init='flat')
    print("Rede base carregada:")
    print(f" - Tensão mínima: {net_base.res_bus.vm_pu.min():.4f} pu")
    print(f" - Tensão máxima: {net_base.res_bus.vm_pu.max():.4f} pu")
    print(f" - Carregamento máximo de linha: {net_base.res_line.loading_percent.max():.2f} %")
    print("------")

    # Define os limites de operação para avaliação de criticidade
    vmax = 1.06  # Tensão máxima do caso IEEE 30
    vmin = 0.94   # Tensão mínima do caso IEEE 30
    line_loading_max = 100 # Carregamento máximo de linha permitido em porcentagem, no caso IEEE não existe esse limite, estou colocando para tornar mais realista.
#Se eu usar float('inf'), todas as linhas passarão como "não críticas" mesmo sobrecarregadas (irrealista).
#o ideal é usar um limite para cada linha, estou generalizando para fins de estudo.

    resultados_globais = [] # Lista para armazenar todos os resultados de contingência (críticos e não críticos)
    tensao_cenarios_nao_criticos_para_csv = [] # Lista para armazenar dados de tensão APENAS de cenários não críticos

    # --- Loop principal para simular cada cenário ---
    for cenario_id in range(n_cenarios):
        print(f"\nSimulando Cenário {cenario_id}...")
        dados_cenario = gerar_dados_cenario(net_base, cenario_id) # Gera dados variados para o cenário atual
        
        # Cria uma cópia da rede base para este cenário e aplica os dados variados.
        # Esta é a rede "pré-contingência" para o cenário atual.
        net_cenario_inicial = criar_rede_ieee30_slack_bar()
        aplicar_dados_ao_net(net_cenario_inicial, dados_cenario)
        
        # --- Roda o fluxo de potência para o cenário base ANTES de qualquer contingência ---
        # Isso nos dá as tensões de referência para comparar com as tensões pós-contingência.
        try:
            pp.runpp(net_cenario_inicial, numba=False, init='flat')
            tensao_antes_contingencia = net_cenario_inicial.res_bus.vm_pu.to_dict()
        except pp.LoadflowNotConverged:
            # Se o fluxo inicial não convergir, registra NaN para todas as tensões.
            tensao_antes_contingencia = {bus: float('nan') for bus in net_cenario_inicial.bus.index}
            print(f"Cenário {cenario_id}: Fluxo de potência inicial NÃO convergiu.")

        linhas_para_testar = list(net_base.line.index) # Lista de todas as linhas para testar contingências
        
        linhas_criticas_cenario_resumo = [] # Lista para armazenar linhas que causaram criticidade/ilhamento neste cenário

        # --- Loop para testar o desligamento de cada linha como contingência N-1 ---
        for linha in linhas_para_testar:
            # Cria uma nova rede para cada contingência, garantindo que seja uma cópia limpa.
            net = criar_rede_ieee30_slack_bar()
            aplicar_dados_ao_net(net, dados_cenario) # Aplica os dados variados do cenário atual
            net.line.at[linha, 'in_service'] = False # Desliga a linha atual (simula a contingência)

            status_contingencia = 'normal' # Inicializa o status como normal
            ilhamento_detectado = False # Flag para ilhamento
            
            # --- Verificação de Ilhamento (Particionamento da Rede) ---
            # Cria um grafo da rede para verificar a conectividade.
            graph = create_nxgraph(net, respect_switches=True)
            slack_buses = net.gen[net.gen['slack']].bus.values # Identifica as barras slack

            # Encontra os componentes conectados (sub-redes isoladas).
            componentes = list(nx.connected_components(graph))
            for i, component in enumerate(componentes):
                component_set = set(component)
                # Verifica se a barra slack está presente em cada componente.
                slack_present_in_component = any(bus in component_set for bus in slack_buses)

                # Se um componente não contiver a barra slack, significa que parte da rede
                # foi ilhada (perdeu conexão com a fonte de referência).
                if not slack_present_in_component:
                    ilhamento_detectado = True
                    status_contingencia = 'ilhamento' # Define o status como ilhamento
                    break # Não precisa verificar outros componentes se já houve ilhamento

            # Inicializa as tensões após a contingência com NaN, caso o fluxo não converta.
            tensao_apos_contingencia = {bus: float('nan') for bus in net.bus.index}

            if ilhamento_detectado:
                print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Ilhamento detectado")
                linhas_criticas_cenario_resumo.append(linha) # Adiciona a linha à lista de críticas
            else:
                # --- Execução do Fluxo de Potência Pós-Contingência ---
                try:
                    pp.runpp(net, numba=True, init='flat') # Tenta rodar o fluxo de potência
                    
                    # Se convergiu, armazena as tensões resultantes.
                    tensao_apos_contingencia = net.res_bus.vm_pu.to_dict()

                    # Obtém os valores mínimos/máximos de tensão e carregamento de linha.
                    vm_min = float(net.res_bus.vm_pu.min())
                    vm_max = float(net.res_bus.vm_pu.max())
                    loading_max = float(net.res_line.loading_percent.max())

                    # --- Verificação de Criticidade (Tensão e Carregamento) ---
                    # Compara os resultados com os limites de operação definidos.
                    if vm_min < vmin:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão mínima ({vm_min:.4f} pu) abaixo do limite ({vmin:.4f} pu)")
                        status_contingencia = 'crítica'
                    elif vm_max > vmax:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão máxima ({vm_max:.4f} pu) acima do limite ({vmax:.4f} pu)")
                        status_contingencia = 'crítica'
                    elif loading_max > line_loading_max:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Carregamento de linha ({loading_max:.2f} %) excedido ({line_loading_max:.2f} %)")
                        status_contingencia = 'crítica'
                    else:
                        # === CASO NÃO CRÍTICO AQUI! ===
                        # Se não houve ilhamento E não houve criticidade de tensão/carregamento,
                        # esta contingência é considerada "não crítica".
                        # Coletamos as tensões antes e depois para análise de impacto detalhada.
                        row_data = {
                            'cenario': cenario_id,
                            'linha_desligada': linha,
                            'from_bus': net_base.line.at[linha, 'from_bus'],
                            'to_bus': net_base.line.at[linha, 'to_bus']
                        }
                        # Adiciona as tensões de cada barra ANTES do desligamento
                        for bus_idx, vm_pu in tensao_antes_contingencia.items():
                            row_data[f'vm_pu_antes_bus_{bus_idx}'] = vm_pu
                        # Adiciona as tensões de cada barra DEPOIS do desligamento
                        for bus_idx, vm_pu in tensao_apos_contingencia.items():
                            row_data[f'vm_pu_depois_bus_{bus_idx}'] = vm_pu
                        
                        tensao_cenarios_nao_criticos_para_csv.append(row_data)
                        pass # Não imprime "OK" para não poluir o console

                    if status_contingencia == 'crítica':
                        linhas_criticas_cenario_resumo.append(linha) # Adiciona a linha à lista de críticas
                
                # --- Tratamento de Fluxo de Potência Não Convergente ---
                except pp.LoadflowNotConverged:
                    status_contingencia = 'crítica (não convergiu)' # Define o status como não convergiu
                    print(f"Cenário {cenario_id}, linha {linha}: ❌ Fluxo não convergiu")
                    linhas_criticas_cenario_resumo.append(linha) # Adiciona a linha à lista de críticas

            # Armazena os resultados desta contingência (crítica ou não) para o CSV global.
            resultados_globais.append({
                'cenario': cenario_id,
                'linha_desligada': linha,
                'status': status_contingencia,
                'ilhamento': ilhamento_detectado,
                'num_componentes_conectados': len(componentes) if not ilhamento_detectado else None,
                'convergencia': status_contingencia != 'crítica (não convergiu)' # True se convergiu, False caso contrário
            })
        
        # --- Resumo por Cenário ---
        if linhas_criticas_cenario_resumo:
            print(f"\n--- Resumo Cenário {cenario_id}: Linhas que causaram Criticidade/Ilhamento: {linhas_criticas_cenario_resumo} ---")
        else:
            print(f"\n--- Resumo Cenário {cenario_id}: Nenhuma criticidade ou ilhamento detectado. ---")

    # Retorna os resultados globais e os dados de tensão para cenários não críticos.
    return resultados_globais, tensao_cenarios_nao_criticos_para_csv

# --- Função para Análise de Impacto de Tensão no formato de colunas de barras ---
def analisar_impacto_tensao_formato_novo(input_df_tensao, output_csv='impacto_tensao_barras_completo_ieee30.csv', num_barras=30):
    """
    Analisa o impacto do desligamento de linhas nas tensões das barras
    e gera um CSV onde cada barra tem sua própria coluna com o valor da diferença de tensão (impacto).

    Args:
        input_df_tensao (pd.DataFrame): DataFrame com as tensões antes e depois do desligamento.
        output_csv (str): Caminho para o arquivo CSV de saída para os resultados de impacto.
        num_barras (int): O número total de barras no sistema (padrão 30 para IEEE 30).
    """
    # Verifica se o DataFrame de entrada está vazio. Se sim, não há nada para analisar.
    if input_df_tensao.empty:
        print("DataFrame de entrada para análise de impacto está vazio. Nenhuma análise será realizada.")
        return

    resultados_impacto_novo_formato = [] # Lista para armazenar as linhas do novo DataFrame de impacto

    # Cria uma lista de nomes de colunas dinamicamente para cada barra (ex: 'barra_0', 'barra_1', ...)
    colunas_barras = [f'barra_{i}' for i in range(num_barras)]

    # Itera sobre cada linha do DataFrame de tensões não críticas.
    # Cada linha representa um cenário de contingência "não crítica".
    for index, row in input_df_tensao.iterrows():
        cenario_id = row['cenario'] # ID do cenário
        linha_desligada = row['linha_desligada'] # ID da linha desligada
        
        # Inicia um dicionário para a linha atual do resultado, contendo o cenário e a linha desligada.
        linha_resultado = {
            'cenario': cenario_id,
            'linha_desligada': linha_desligada
        }

        # Para cada barra no sistema (de 0 a num_barras-1), calcula o impacto na tensão.
        for i in range(num_barras):
            col_antes = f'vm_pu_antes_bus_{i}' # Nome da coluna da tensão 'antes' para a barra 'i'
            col_depois = f'vm_pu_depois_bus_{i}' # Nome da coluna da tensão 'depois' para a barra 'i'

            # Tenta obter os valores de tensão. Usamos .get() para segurança,
            # embora essas colunas devam existir se o df_tensao_nao_criticos for bem formado.
            tensao_antes = row.get(col_antes)
            tensao_depois = row.get(col_depois)

            # Se a tensão antes ou depois for NaN (indicando que o fluxo de potência não convergiu para essa barra),
            # a diferença também será NaN. Caso contrário, calcula a diferença absoluta.
            if pd.isna(tensao_antes) or pd.isna(tensao_depois):
                diferenca = np.nan
            else:
                diferenca = abs(tensao_depois - tensao_antes) # Calcula a magnitude da diferença (impacto)
            
            # Adiciona o impacto calculado para a barra 'i' ao dicionário da linha de resultado.
            linha_resultado[f'barra_{i}'] = diferenca
        
        # Adiciona o dicionário da linha de resultado à lista de resultados.
        resultados_impacto_novo_formato.append(linha_resultado)

    # Cria um DataFrame final a partir da lista de resultados.
    df_impacto_novo_formato = pd.DataFrame(resultados_impacto_novo_formato)
    
    # Se o DataFrame não estiver vazio, salva-o em um arquivo CSV.
    if not df_impacto_novo_formato.empty:
        # Define a ordem das colunas para garantir 'cenario' e 'linha_desligada' no início,
        # seguidas pelas colunas das barras em ordem numérica.
        cols_ordered = ['cenario', 'linha_desligada'] + colunas_barras
        df_impacto_novo_formato = df_impacto_novo_formato[cols_ordered]

        df_impacto_novo_formato.to_csv(output_csv, index=False) # Salva sem o índice do DataFrame
        print(f"\nAnálise de impacto concluída. Dados de impacto por barra salvos em '{output_csv}'.")
    else:
        print("\nNão foram encontrados dados de impacto de tensão para salvar no novo formato.")


# --- Execução Principal do Script ---
# Define o número de cenários de simulação que serão executados.
n_cenarios_simulacao = 5 

print(f"Iniciando simulação com {n_cenarios_simulacao} cenários para IEEE 30 barras...")

# Chama a função principal de simulação de cenários.
# Ela retorna uma lista de resultados globais e uma lista de dados de tensão para cenários não críticos.
resultados_globais_df, tensao_data_para_csv = simular_cenarios(n_cenarios=n_cenarios_simulacao)

# Converte os resultados globais em um DataFrame e os salva em um CSV.
df_resultados_finais = pd.DataFrame(resultados_globais_df)
df_resultados_finais.to_csv('resultados_simulacao_ieee30_detalhado.csv', index=False)
print(f"\nSimulação concluída. Resultados detalhados (todas as contingências) salvos em 'resultados_simulacao_ieee30_detalhado.csv'.")

# --- Processamento e salvamento de dados de tensão para contingências NÃO CRÍTICAS ---
if tensao_data_para_csv:
    # Converte a lista de dados de tensão em um DataFrame.
    df_tensao_nao_criticos = pd.DataFrame(tensao_data_para_csv)
    
    # Opcional: Reordena as colunas para melhor visualização no CSV de tensões.
    # Coloca 'cenario', 'linha_desligada', 'from_bus', 'to_bus' primeiro,
    # seguidos pelas tensões 'antes' e 'depois' em ordem crescente de barra.
    cols = ['cenario', 'linha_desligada', 'from_bus', 'to_bus'] + \
           sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_antes_bus_')]) + \
           sorted([col for col in df_tensao_nao_criticos.columns if col.startswith('vm_pu_depois_bus_')])
    df_tensao_nao_criticos = df_tensao_nao_criticos[cols]

    # Salva o DataFrame de tensões não críticas em um CSV.
    df_tensao_nao_criticos.to_csv('tensao_barras_nao_criticos_ieee30.csv', index=False)
    print(f"Dados de tensão para contingências NÃO CRÍTICAS salvos em 'tensao_barras_nao_criticos_ieee30.csv'.")

    # --- Chamada para a função de análise de impacto de tensão no formato desejado ---
    # É importante que esta chamada ocorra APÓS df_tensao_nao_criticos ser criado e preenchido.
    analisar_impacto_tensao_formato_novo(df_tensao_nao_criticos)
else:
    # Mensagens caso não haja contingências não críticas para processar.
    print("Nenhuma contingência não crítica foi encontrada para salvar dados de tensão.")
    print("Nenhuma análise de impacto de tensão será realizada, pois não há dados não críticos.")


#analisar se as margens dos valores randomicos fazem sentido#
