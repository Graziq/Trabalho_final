# src/flows/visualizacao_impacto.py

import pandas as pd
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output
import os
import io
import sys
from prefect import flow, task # Manter para consistência, mas não será um flow do Prefect principal
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context


# --- 1. Defina um diretório base para todos os arquivos de entrada ---
# Use a variável de ambiente que será definida no docker-compose para o serviço Dash.
INPUT_DATA_DIR = os.getenv("PREFECT_SHARED_DATA_PATH", "/app/Trabalho_final/simulacao_resultados")

print(f"DEBUG: INPUT_DATA_DIR set to: {INPUT_DATA_DIR}")

# Prefect Flow/Tasks (Opcional, se quiser registrar que o Dash está sendo 'preparado' para o Prefect)
# Este Flow/Task não iniciará o servidor Dash, apenas confirmará que os dados estão lá.
@flow(name="Verificar e Preparar Dados para Visualizacao Dash", log_prints=True)
def verificar_dados_para_dash_flow(input_csv_filename: str = 'tensao_barras_nao_criticos_ieee30.csv'):
    input_path = os.path.join(INPUT_DATA_DIR, input_csv_filename)
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Arquivo de dados Dash '{input_path}' não encontrado.")
        print(f"Arquivo de dados Dash '{input_path}' encontrado. Pronto para iniciar o servidor.")
        run_context = get_run_context()
        if run_context:
            create_markdown_artifact(
                f"Dados para a visualização Dash foram localizados em: `{input_path}`. "
                f"Você pode iniciar o servidor Dash.",
                key="dash-data-check",
                description="Verificação de dados para a aplicação Dash."
            )
    except Exception as e:
        print(f"Erro na verificação de dados para Dash: {e}")
        raise

# --- Aplicação Dash ---
app = Dash(__name__)

# Função auxiliar para carregar dados para o layout e callbacks
# Isso é importante porque o layout é avaliado uma vez na inicialização,
# e o callback é avaliado a cada interação.
def load_data_for_dash():
    csv_path = os.path.join(INPUT_DATA_DIR, 'tensao_barras_nao_criticos_ieee30.csv')
    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        print(f"ERRO: Arquivo '{csv_path}' não encontrado ao tentar carregar para o Dash. Assegure-se que o flow de simulação o gerou.")
        return pd.DataFrame() # Retorna um DataFrame vazio para evitar erros
    except Exception as e:
        print(f"ERRO ao carregar CSV para o Dash: {e}")
        return pd.DataFrame()

# Carregar dados inicialmente para o layout (para preencher o dropdown)
initial_df_tensao = load_data_for_dash()
initial_scenarios = sorted(initial_df_tensao['cenario'].unique()) if not initial_df_tensao.empty else []


app.layout = html.Div([
    html.H1("Análise das 10 Barras Mais Impactadas Pós-Desligamento de Linhas (IEEE 30 Barras)"),

    html.Div([
        html.Label("Selecione o Cenário:"),
        dcc.Dropdown(
            id='dropdown-cenario',
            options=[{'label': f'Cenário {i}', 'value': i} for i in initial_scenarios],
            value=initial_scenarios[0] if initial_scenarios else None,
            clearable=False
        ),
    ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),

    html.Hr(),

    html.Div(id='output-tables-container')
])


@app.callback(
    Output('output-tables-container', 'children'),
    Input('dropdown-cenario', 'value')
)
def update_output_tables(selected_cenario):
    if selected_cenario is None:
        return html.Div("Por favor, selecione um cenário para exibir as tabelas.")

    df_tensao_dash = load_data_for_dash() # Recarrega os dados a cada atualização do callback

    if df_tensao_dash.empty:
        return html.Div("Dados de tensão não disponíveis para análise. Execute a simulação primeiro.")

    # ... (o restante da lógica do callback permanece o mesmo) ...
    # Filtro, cálculo de variação, ordenação e seleção das top 10
    
    cols_vm_pu_antes = [col for col in df_tensao_dash.columns if col.startswith('vm_pu_antes_bus_')]
    bus_ids = [col.replace('vm_pu_antes_bus_', '') for col in cols_vm_pu_antes]

    df_cenario_filtered = df_tensao_dash[df_tensao_dash['cenario'] == selected_cenario].copy()
    linhas_desligadas_unicas = df_cenario_filtered['linha_desligada'].unique()
    
    all_tables = []

    for linha_desligada in sorted(linhas_desligadas_unicas):
        df_filtered_linha = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]

        data_for_single_table = []
        for bus_id in bus_ids:
            col_antes = f'vm_pu_antes_bus_{bus_id}'
            col_depois = f'vm_pu_depois_bus_{bus_id}'

            vm_pu_antes = df_filtered_linha[col_antes]
            vm_pu_depois = df_filtered_linha[col_depois]
            
            if pd.isna(vm_pu_antes) or pd.isna(vm_pu_depois):
                 variation = None
            else:
                variation = vm_pu_depois - vm_pu_antes

            data_for_single_table.append({
                'Barra': f'Barra {bus_id}',
                'Tensao_Antes_pu': vm_pu_antes,
                'Variacao_Tensao_pu': variation,
                'Abs_Variacao_Tensao_pu': abs(variation) if variation is not None else -1
            })
        
        df_table = pd.DataFrame(data_for_single_table)
        df_table = df_table.sort_values(by='Abs_Variacao_Tensao_pu', ascending=False)
        df_table = df_table.head(10)
        df_table = df_table.drop(columns=['Abs_Variacao_Tensao_pu'])

        linha_info = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]
        from_bus = int(linha_info['from_bus'])
        to_bus = int(linha_info['to_bus'])
        linha_title = f"Linha {linha_desligada} (Barras {from_bus}-{to_bus})"

        all_tables.append(
            html.Div([
                html.H3(f"Top 10 Barras Mais Impactadas para {linha_title}"),
                dash_table.DataTable(
                    id=f'dynamic-table-{linha_desligada}-{selected_cenario}',
                    columns=[
                        {"name": "Barra", "id": "Barra"},
                        {"name": "Tensão Antes (p.u.)", "id": "Tensao_Antes_pu", "type": "numeric", "format": dash_table.Format.Format(precision=4, scheme=dash_table.Format.Scheme.fixed)},
                        {"name": "Variação de Tensão (p.u.)", "id": "Variacao_Tensao_pu", "type": "numeric", "format": dash_table.Format.Format(precision=4, scheme=dash_table.Format.Scheme.fixed)},
                    ],
                    data=df_table.to_dict('records'),
                    style_table={'overflowX': 'auto', 'marginBottom': '20px', 'border': '1px solid #ddd'},
                    style_cell={
                        'height': 'auto', 'minWidth': '120px', 'width': '120px', 'maxWidth': '180px',
                        'whiteSpace': 'normal', 'textAlign': 'left'
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'
                    },
                    sort_action="native",
                    filter_action="native"
                )
            ], style={'marginBottom': 30, 'padding': 15, 'border': '1px solid #eee', 'borderRadius': 5})
        )
    
    if not all_tables:
        return html.Div("Nenhuma linha desligada não-crítica encontrada para este cenário.")

    return all_tables


if __name__ == "__main__":
    # Este é o ponto de entrada quando o Docker service do Dash for iniciado
    print("\n--- Iniciando o Servidor Dash ---")
    print(f"Servidor Dash esperando dados em: {INPUT_DATA_DIR}")
    print("Acesse a aplicação no navegador: http://0.0.0.0:8050/ (ou o IP do seu host)")
    app.run(debug=True, host='0.0.0.0', port=8050)


# import pandas as pd
# from dash import Dash, dcc, html, dash_table
# from dash.dependencies import Input, Output

# # --- 1. Carregar os Dados ---
# try:
#     # Carregamos apenas este arquivo, pois já está pré-filtrado
#     df_tensao = pd.read_csv('../../simulacao_resultados/tensao_barras_nao_criticos_ieee30.csv')
# except FileNotFoundError:
#     print("Certifique-se de que 'tensao_barras_nao_criticos_ieee30.csv' está no mesmo diretório do script.")
#     exit()

# # --- 2. Pré-processar os Dados ---

# # Identificar as colunas de tensão dinamicamente
# cols_vm_pu_antes = [col for col in df_tensao.columns if col.startswith('vm_pu_antes_bus_')]

# # Mapear IDs das barras para os nomes das colunas
# bus_ids = [col.replace('vm_pu_antes_bus_', '') for col in cols_vm_pu_antes]

# # --- 3. Criar a Aplicação Dash ---
# app = Dash(__name__)

# app.layout = html.Div([
#     html.H1("Análise das 10 Barras Mais Impactadas Pós-Desligamento de Linhas (IEEE 30 Barras)"),

#     html.Div([
#         html.Label("Selecione o Cenário:"),
#         dcc.Dropdown(
#             id='dropdown-cenario',
#             options=[{'label': f'Cenário {i}', 'value': i} for i in sorted(df_tensao['cenario'].unique())],
#             value=sorted(df_tensao['cenario'].unique())[0] if not df_tensao['cenario'].empty else None,
#             clearable=False
#         ),
#     ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),

#     html.Hr(), # Linha divisória

#     html.Div(id='output-tables-container') # Contêiner para as tabelas geradas
# ])

# # --- 4. Implementar Callbacks ---

# # Callback para gerar todas as tabelas com base no cenário selecionado
# @app.callback(
#     Output('output-tables-container', 'children'),
#     Input('dropdown-cenario', 'value')
# )
# def update_output_tables(selected_cenario):
#     if selected_cenario is None:
#         return html.Div("Por favor, selecione um cenário para exibir as tabelas.")

#     # Filtrar os dados para o cenário selecionado
#     df_cenario_filtered = df_tensao[df_tensao['cenario'] == selected_cenario].copy()

#     # Garantir que temos todas as linhas desligadas únicas para este cenário
#     linhas_desligadas_unicas = df_cenario_filtered['linha_desligada'].unique()
    
#     all_tables = []

#     # Iterar por cada linha desligada única no cenário selecionado
#     for linha_desligada in sorted(linhas_desligadas_unicas):
#         # Filtrar a linha específica
#         df_filtered_linha = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]

#         data_for_table = []

#         for bus_id in bus_ids:
#             col_antes = f'vm_pu_antes_bus_{bus_id}'
#             col_depois = f'vm_pu_depois_bus_{bus_id}'

#             vm_pu_antes = df_filtered_linha[col_antes]
#             vm_pu_depois = df_filtered_linha[col_depois]
            
#             # Cuidado para evitar NaN se a coluna não existir por algum motivo ou for NaN
#             if pd.isna(vm_pu_antes) or pd.isna(vm_pu_depois):
#                  variation = None
#             else:
#                 variation = vm_pu_depois - vm_pu_antes

#             data_for_table.append({
#                 'Barra': f'Barra {bus_id}',
#                 'Tensao_Antes_pu': vm_pu_antes,
#                 'Variacao_Tensao_pu': variation,
#                 'Abs_Variacao_Tensao_pu': abs(variation) if variation is not None else -1 # Usado para ordenação
#             })
        
#         # Criar um DataFrame temporário para facilitar a ordenação
#         df_table = pd.DataFrame(data_for_table)
        
#         # Ordenar pela maior variação absoluta
#         df_table = df_table.sort_values(by='Abs_Variacao_Tensao_pu', ascending=False)
        
#         # --- NOVO: Selecionar apenas as 10 primeiras linhas (barras mais impactadas) ---
#         df_table = df_table.head(10)

#         # Remover a coluna auxiliar de ordenação antes de exibir
#         df_table = df_table.drop(columns=['Abs_Variacao_Tensao_pu'])

#         # Título da tabela - Obter from_bus e to_bus
#         linha_info = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]
#         from_bus = int(linha_info['from_bus'])
#         to_bus = int(linha_info['to_bus'])
#         linha_title = f"Linha {linha_desligada} (Barras {from_bus}-{to_bus})"

#         all_tables.append(
#             html.Div([
#                 html.H3(f"Top 10 Barras Mais Impactadas para {linha_title}"),
#                 dash_table.DataTable(
#                     id=f'dynamic-table-{linha_desligada}', # ID único para cada tabela
#                     columns=[
#                         {"name": "Barra", "id": "Barra"},
#                         {"name": "Tensão Antes (p.u.)", "id": "Tensao_Antes_pu", "type": "numeric", "format": dash_table.Format.Format(precision=4, scheme=dash_table.Format.Scheme.fixed)},
#                         {"name": "Variação de Tensão (p.u.)", "id": "Variacao_Tensao_pu", "type": "numeric", "format": dash_table.Format.Format(precision=4, scheme=dash_table.Format.Scheme.fixed)},
#                     ],
#                     data=df_table.to_dict('records'),
#                     style_table={'overflowX': 'auto', 'marginBottom': '20px', 'border': '1px solid #ddd'}, # Margem inferior e borda
#                     style_cell={
#                         'height': 'auto',
#                         'minWidth': '120px', 'width': '120px', 'maxWidth': '180px',
#                         'whiteSpace': 'normal',
#                         'textAlign': 'left'
#                     },
#                     style_header={
#                         'backgroundColor': 'rgb(230, 230, 230)',
#                         'fontWeight': 'bold'
#                     },
#                     sort_action="native",
#                     filter_action="native"
#                 )
#             ], style={'marginBottom': '30px', 'padding': '15px', 'border': '1px solid #eee', 'borderRadius': '5px'}) # Estilo para o contêiner de cada tabela
#         )
    
#     if not all_tables:
#         return html.Div("Nenhuma linha desligada não-crítica encontrada para este cenário.")

#     return all_tables

# if __name__ == '__main__':
#     app.run(debug=True)