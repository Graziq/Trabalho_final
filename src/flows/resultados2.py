# Assuming the corrected `analise_impacto_flow`
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
import json # Para trabalhar com o tipo JSONB no PostgreSQL
from sqlalchemy import create_engine, text # Para conexão com o banco de dados e execução de comandos SQL



def get_db_url():
    """Retorna a URL de conexão do banco de dados, adaptando para o ambiente."""
    # Se você está executando o flow localmente para depuração, use 'localhost'.
    # Se a task for executada por um agente no Docker, 'DB_HOST' será 'postgres'.
    db_user = os.getenv('DB_USER', 'prefect')
    db_password = os.getenv('DB_PASSWORD', 'prefect')
    db_host = os.getenv('DB_HOST', 'localhost') # <<-- AQUI! Use 'localhost' como fallback padrão para scripts fora do Docker
    db_name = os.getenv('DB_NAME', 'prefect')
    db_port = os.getenv('DB_PORT', '5432')

    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return url

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

#teste pois estava travando (lembrar de rodar o agent antes)
print(f"DEBUG: pandapower version in worker: {pp.__version__}")


## Funções/Tasks Auxiliares Inalteradas

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


## Novas Tasks para Interagir com o PostgreSQL

@task
def criar_tabelas_postgres():
    """Versão definitiva com todos os tratamentos de erro"""
    import time
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import OperationalError

    DB_URL = get_db_url()
    print(f"🔄 Tentando conectar em: {DB_URL.replace('prefect:prefect@', '')}")

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                print("✅ Conexão bem-sucedida!")

                # Criação das tabelas com verificação
                tabelas = {
                    'resultados_simulacao': """
                        CREATE TABLE IF NOT EXISTS resultados_simulacao (
                            id SERIAL PRIMARY KEY,
                            cenario INTEGER,
                            linha_desligada INTEGER,
                            status TEXT,
                            ilhamento BOOLEAN,
                            num_componentes_conectados INTEGER,
                            convergencia BOOLEAN,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );""",
                    'tensao_barras_nao_criticos': """
                        CREATE TABLE IF NOT EXISTS tensao_barras_nao_criticos (
                            id SERIAL PRIMARY KEY,
                            cenario INTEGER,
                            linha_desligada INTEGER,
                            from_bus INTEGER,
                            to_bus INTEGER,
                            -- Geração dinâmica das colunas vm_pu_antes_bus_X
                            """ + ",\n".join([f"vm_pu_antes_bus_{i} NUMERIC" for i in range(30)]) + """,
                            -- Geração dinâmica das colunas vm_pu_depois_bus_X
                            """ + ",\n".join([f"vm_pu_depois_bus_{i} NUMERIC" for i in range(30)]) + """,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );""",
                    'impacto_tensao_barras': """
                        CREATE TABLE IF NOT EXISTS impacto_tensao_barras (
                            id SERIAL PRIMARY KEY,
                            cenario INTEGER,
                            linha_desligada INTEGER,
                            impacto_por_barra JSONB,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );"""
                }

                for nome, schema in tabelas.items():
                    print(f"Criando/Verificando tabela: {nome}")
                    conn.execute(text(schema))
                    # Verificação pós-criação
                    result = conn.execute(
                        text("SELECT to_regclass(:tabela)"),
                        {"tabela": nome}
                    ).scalar()
                    if not result:
                        raise RuntimeError(f"Tabela {nome} não foi criada")
                    print(f"✅ Tabela {nome} verificada")

                conn.commit()
                return True

        except OperationalError as e:
            wait_time = attempt * 2
            print(f"⚠️ Tentativa {attempt}/{max_attempts} falhou. Aguardando {wait_time}s... Erro: {str(e)}")
            time.sleep(wait_time)
            if attempt == max_attempts:
                print("❌ Todas as tentativas falharam. Verifique:")
                print(f"- Serviço PostgreSQL está rodando? (docker-compose ps)")
                print(f"- Credenciais corretas? (usuário: prefect, senha: prefect)")
                print(f"- Permissões adequadas? (docker-compose exec postgres psql -U prefect -c '\\du')")
            raise
    return False

@task
def salvar_resultados_globais_postgres(resultados, table_name='resultados_simulacao'):
    """
    Salva os resultados globais de todas as contingências no PostgreSQL.
    """
    if not resultados:
        print("Nenhum resultado global para salvar no PostgreSQL.")
        return

    df_resultados_finais = pd.DataFrame(resultados)

    DB_URL = get_db_url() # <-- Chama a função auxiliar aqui também!
    engine = create_engine(DB_URL) # <-- Cria o engine localmente para a task

    try:
        df_resultados_finais.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"\nResultados detalhados (todas as contingências) salvos na tabela '{table_name}' do PostgreSQL.")
        run_context = get_run_context()
        if run_context:
            create_markdown_artifact(
                f"Resultados da simulação salvos no PostgreSQL na tabela: `{table_name}`",
                key="resultados-simulacao-db",
                description="Sumário dos resultados de todas as contingências no banco de dados."
            )
    except Exception as e:
        print(f"Erro ao salvar resultados globais no PostgreSQL: {e}")

@task
def salvar_tensao_nao_criticos_postgres(tensao_data, table_name='tensao_barras_nao_criticos', num_barras=30):
    """
    Salva os dados de tensão para contingências NÃO CRÍTICAS no PostgreSQL no formato expandido.
    """
    if not tensao_data:
        print("Nenhuma contingência não crítica foi encontrada para salvar dados de tensão no PostgreSQL.")
        return

    # Processa os dados para o formato expandido (flat)
    processed_data_flat = []
    for row in tensao_data:
        flat_row = {
            'cenario': row['cenario'],
            'linha_desligada': row['linha_desligada'],
            'from_bus': row['from_bus'],
            'to_bus': row['to_bus']
        }
        
        # 'tensao_antes' e 'tensao_depois' já são dicionários aqui (vide simulacao_contingencia_flow)
        tensao_antes_dict = row['tensao_antes']
        tensao_depois_dict = row['tensao_depois']

        for i in range(num_barras): # Iterar sobre todas as possíveis barras
            flat_row[f'vm_pu_antes_bus_{i}'] = tensao_antes_dict.get(i) # Usar .get(i) para valores numéricos
            flat_row[f'vm_pu_depois_bus_{i}'] = tensao_depois_dict.get(i) # Usar .get(i) para valores numéricos
        processed_data_flat.append(flat_row)

    df_tensao_nao_criticos = pd.DataFrame(processed_data_flat)

    # Garante que as colunas de tensão são numéricas (podem vir como object se houver NaN ou se forem strings)
    for col in df_tensao_nao_criticos.columns:
        if col.startswith('vm_pu_antes_bus_') or col.startswith('vm_pu_depois_bus_'):
            df_tensao_nao_criticos[col] = pd.to_numeric(df_tensao_nao_criticos[col], errors='coerce')


    DB_URL = get_db_url()
    engine = create_engine(DB_URL)

    try:
        # if_exists='append' vai adicionar as linhas.
        # As colunas devem corresponder exatamente ao schema da tabela no PostgreSQL.
        df_tensao_nao_criticos.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Dados de tensão para contingências NÃO CRÍTICAS salvos no formato expandido na tabela '{table_name}' do PostgreSQL.")
        run_context = get_run_context()
        if run_context:
            create_markdown_artifact(
                f"Dados de tensão para cenários não críticos salvos no PostgreSQL na tabela: `{table_name}`",
                key="tensao-nao-criticos-db",
                description="Tensões antes e depois para contingências sem criticidade no banco de dados."
            )
    except Exception as e:
        print(f"Erro ao salvar dados de tensão no PostgreSQL: {e}")
        # Considerar logar o DataFrame para depuração em caso de erro
        # print(df_tensao_nao_criticos.head())
        # print(df_tensao_nao_criticos.dtypes)

@task
def analisar_impacto_tensao_postgres(table_name_input='tensao_barras_nao_criticos', num_barras=30, table_name_output='impacto_tensao_barras'):
    """
    Analisa o impacto do desligamento de linhas nas tensões das barras e
    salva os resultados no PostgreSQL.
    """
    print(f"Iniciando análise de impacto de tensão a partir do PostgreSQL da tabela '{table_name_input}'...")

    DB_URL = get_db_url()
    engine = create_engine(DB_URL)

    try:
        # A query agora seleciona todas as colunas de tensão explicitamente
        # Isso garante que o DataFrame retornado já venha no formato expandido
        columns_to_select = ['cenario', 'linha_desligada', 'from_bus', 'to_bus'] + \
                            [f'vm_pu_antes_bus_{i}' for i in range(num_barras)] + \
                            [f'vm_pu_depois_bus_{i}' for i in range(num_barras)]
        
        query = f"SELECT {', '.join(columns_to_select)} FROM {table_name_input};"
        
        with engine.connect() as connection:
            df_tensao = pd.read_sql(text(query), connection)
        print(f"Dados carregados com sucesso da tabela '{table_name_input}'.")
    except Exception as e:
        print(f"Erro ao carregar dados de tensão do PostgreSQL para análise de impacto: {e}")
        return

    if df_tensao.empty:
        print("DataFrame de entrada para análise de impacto está vazio. Nenhuma análise será realizada.")
        return

    resultados_impacto_novo_formato = []

    for index, row in df_tensao.iterrows(): # Iterar sobre o DataFrame diretamente
        cenario_id = row['cenario']
        linha_desligada = row['linha_desligada']

        linha_resultado = {
            'cenario': cenario_id,
            'linha_desligada': linha_desligada,
            'impacto_por_barra': {} # Ainda usaremos JSONB para o impacto aqui, como definido na sua tabela 'impacto_tensao_barras'
        }

        for i in range(num_barras):
            tensao_antes = row[f'vm_pu_antes_bus_{i}'] # Acessa diretamente a coluna
            tensao_depois = row[f'vm_pu_depois_bus_{i}'] # Acessa diretamente a coluna

            if pd.isna(tensao_antes) or pd.isna(tensao_depois):
                diferenca = np.nan
            else:
                diferenca = abs(tensao_depois - tensao_antes)

            linha_resultado['impacto_por_barra'][str(i)] = diferenca

        resultados_impacto_novo_formato.append(linha_resultado)

    df_impacto = pd.DataFrame(resultados_impacto_novo_formato)
    
    # Conversão para JSON para a coluna 'impacto_por_barra' (se a tabela for JSONB)
    df_impacto['impacto_por_barra'] = df_impacto['impacto_por_barra'].apply(lambda x: json.dumps(x))

    # Reordenar colunas antes de salvar, se necessário, para corresponder ao DB
    # (cenario, linha_desligada, impacto_por_barra, created_at)
    cols_order = ['cenario', 'linha_desligada', 'impacto_por_barra']
    # Adicione outras colunas se existirem na tabela 'impacto_tensao_barras', como 'created_at' se você a estiver inserindo automaticamente
    df_impacto = df_impacto[cols_order]

    DB_URL = get_db_url()
    engine = create_engine(DB_URL)

    try:
        df_impacto.to_sql(table_name_output, engine, if_exists='append', index=False)
        print(f"\nAnálise de impacto concluída. Dados de impacto por barra salvos na tabela '{table_name_output}' do PostgreSQL.")
        run_context = get_run_context()
        if run_context:
            create_markdown_artifact(
                f"Relatório de Impacto de Tensão salvo no PostgreSQL na tabela: `{table_name_output}`",
                key="impacto-tensao-db",
                description="Relatório do impacto de tensão nas barras no banco de dados."
            )
    except Exception as e:
        print(f"Erro ao salvar dados de impacto no PostgreSQL: {e}")

## FLOW 1: Simulação de Contingências

@flow(name="simulacao-contingencia-")
def simulacao_contingencia_flow(n_cenarios: int = 2, vmax: float = 1.093, vmin: float = 0.94, line_loading_max: float = 120):
    """
    FLOW: Orquestra a simulação de contingências N-1 na rede IEEE 30 barras,
    salvando os resultados e os dados de tensão para posterior análise de impacto no PostgreSQL.
    """
    print(f"Iniciando simulação com {n_cenarios} cenários para IEEE 30 barras...")
    print(f"DEBUG: Prefect API URL: {os.getenv('PREFECT_API_URL')}")
    print(f"DEBUG: DB_HOST env var for flow: {os.getenv('DB_HOST', 'fallback_flow')}")

    # Garante que as tabelas existem antes de começar a inserir dados
    criar_tabelas_postgres()

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
    # Renomeada para 'tensao_cenarios_nao_criticos_para_db'
    tensao_cenarios_nao_criticos_para_db = []

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
                            'to_bus': net_base_result.line.at[linha, 'to_bus'],
                            'tensao_antes': tensao_antes_contingencia, # Passa o dicionário direto
                            'tensao_depois': tensao_apos_contingencia # Passa o dicionário direto
                        }
                        tensao_cenarios_nao_criticos_para_db.append(row_data)
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

    # 8. Salva os resultados globais no PostgreSQL
    salvar_resultados_globais_postgres(resultados_globais)

    # 9. Salva os dados de tensão para contingências NÃO CRÍTICAS no PostgreSQL
    salvar_tensao_nao_criticos_postgres(tensao_cenarios_nao_criticos_para_db, num_barras=len(net_base_result.bus.index))

## FLOW 2: Análise de Impacto (Separado)

@flow(name="analise-impacto-ieee30", log_prints=True)
def analise_impacto_flow(num_barras: int = 30):
    """
    FLOW: Orquestra a análise de impacto de tensão a partir do PostgreSQL.
    """
    print(f"Iniciando análise de impacto de tensão a partir do PostgreSQL...")

    # A task analisar_impacto_tensao_postgres agora é responsável por carregar os dados
    # e salvar o resultado, então não há necessidade de um segundo pd.read_sql aqui.
    analisar_impacto_tensao_postgres(table_name_input='tensao_barras_nao_criticos', num_barras=num_barras)

    print("Análise de impacto de tensão concluída.")


## Execução Principal do Script


n_cenarios_simulacao = 3

if __name__ == "__main__":
    # Test this connection before deployment
    DB_URL = get_db_url() # Obtenha a URL dinamicamente para o teste local
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as conn:
            print("✅ PostgreSQL connection successful!")
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
    
    n_cenarios_simulacao = 2

    print(f"--- Executando o Flow de Simulação de Contingências ---")
    simulacao_contingencia_flow(n_cenarios=n_cenarios_simulacao)

    print(f"\n--- Executando o Flow de Análise de Impacto ---")
    analise_impacto_flow(num_barras=30)

    print("\nProcesso completo (simulação e análise) concluído e dados salvos no PostgreSQL.")