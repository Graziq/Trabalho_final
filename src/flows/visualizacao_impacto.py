import pandas as pd
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
import os
from sqlalchemy import create_engine, text
import webbrowser
import time
import sys
import threading

# Função para obter a URL de conexão do banco de dados ---
def get_db_url():
    """Retorna a URL de conexão do banco de dados, adaptando para o ambiente."""
    db_user = os.getenv('DB_USER', 'prefect')
    db_password = os.getenv('DB_PASSWORD', 'prefect')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_name = os.getenv('DB_NAME', 'prefect')
    db_port = os.getenv('DB_PORT', '5432')
    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return url

# Função para Carregar os Dados do PostgreSQL
def load_all_data_from_postgres():
    """
    Carrega TODOS os dados da tabela 'tensao_barras_nao_criticos' do PostgreSQL.
    """
    DB_URL = get_db_url()
    table_name = 'tensao_barras_nao_criticos'

    try:
        engine = create_engine(DB_URL)
        print(f"DEBUG: Tentando carregar TODOS os dados da tabela '{table_name}' do PostgreSQL...")
        query = f"SELECT * FROM {table_name} ORDER BY created_at DESC;" # Ordena por created_at para facilitar a identificação dos mais recentes

        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        print(f"DEBUG: Dados carregados com sucesso da tabela '{table_name}'. Total de {len(df)} registros.")
        return df
    except Exception as e:
        print(f"ERRO: ao carregar todos os dados do PostgreSQL para visualização Dash: {e}")
        return pd.DataFrame()

# Criação da Aplicação Dash
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Análise das 10 Barras Mais Impactadas Pós-Desligamento de Linhas (IEEE 30 Barras)"),

    html.Div([
        html.Label("Selecione o Cenário:"),
        dcc.Dropdown(
            id='dropdown-cenario',
            options=[], # Preenchido dinamicamente
            value=None,  # Valor inicial nulo
            clearable=False
        ),
    ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),

    html.Hr(),

    html.Div(id='output-tables-container'),
    
    # Componente para disparar atualizações periódicas
    dcc.Interval(
        id='interval-component',
        interval=60 * 1000,  # Atualiza a cada 60 segundos (60000 ms)
        n_intervals=0
    ),
    html.Div(id='last-updated-time', style={'fontSize': 'small', 'color': 'gray', 'textAlign': 'right', 'marginRight': '10px'})
])

# Callbacks

# Callback para atualizar o dropdown com os cenários da ÚLTIMA execução e a mensagem de última atualização
@app.callback(
    Output('dropdown-cenario', 'options'),
    Output('dropdown-cenario', 'value'),
    Output('last-updated-time', 'children'),
    Input('interval-component', 'n_intervals'),
    State('dropdown-cenario', 'value') # Pega o valor atual do dropdown para tentar manter a seleção
)
def update_dropdown_and_data_status(n_intervals, current_scenario_value):
    print(f"DEBUG: Callback update_dropdown_and_data_status acionado. n_intervals: {n_intervals}")
    df_all_data = load_all_data_from_postgres() # Carrega todos os dados

    if df_all_data.empty or 'cenario' not in df_all_data.columns or 'created_at' not in df_all_data.columns:
        return [], None, "Nenhum dado disponível para visualização."

    # Encontra o timestamp da última execução de simulação
    latest_timestamp = df_all_data['created_at'].max()
    
    # Filtra o DataFrame para incluir APENAS os dados da última execução
    df_latest_run = df_all_data[df_all_data['created_at'] == latest_timestamp].copy()
    
    if df_latest_run.empty:
        return [], None, "Nenhum dado válido da última execução de simulação."

    # Gera as opções do dropdown com base nos cenários da última execução
    cenario_options = [{'label': f'Cenário {i}', 'value': i} for i in sorted(df_latest_run['cenario'].unique())]
    

    selected_value = current_scenario_value
    if selected_value not in df_latest_run['cenario'].unique():
        # Se o cenário atual não existe na última execução ou é nulo, selecione o primeiro novo cenário disponível
        selected_value = cenario_options[0]['value'] if cenario_options else None
            
    last_update_time_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return cenario_options, selected_value, f"Dados da última simulação (atualizado em: {last_update_time_str})"


# Callback para atualizar as tabelas com base na seleção do cenário (da última execução)
@app.callback(
    Output('output-tables-container', 'children'),
    Input('dropdown-cenario', 'value')
)
def update_output_tables(selected_cenario):
    print(f"DEBUG: Callback update_output_tables acionado. Cenário selecionado: {selected_cenario}")
    df_all_data = load_all_data_from_postgres() # Carrega todos os dados novamente para garantir que é a versão mais recente
    
    if selected_cenario is None or df_all_data.empty or 'created_at' not in df_all_data.columns:
        return html.Div("Por favor, selecione um cenário para exibir as tabelas ou os dados não foram carregados.")

    # Encontra o timestamp da última execução de simulação
    latest_timestamp = df_all_data['created_at'].max()
    
    # Filtra o DataFrame para incluir APENAS os dados da última execução
    df_latest_run = df_all_data[df_all_data['created_at'] == latest_timestamp].copy()

    if df_latest_run.empty:
        return html.Div("Nenhum dado válido da última execução de simulação para o cenário selecionado.")

    df_cenario_filtered = df_latest_run[df_latest_run['cenario'] == selected_cenario].copy()
    
    if df_cenario_filtered.empty:
        return html.Div(f"Nenhum dado para o Cenário {selected_cenario} na última simulação.")

    linhas_desligadas_unicas = df_cenario_filtered['linha_desligada'].unique()
    
    all_tables = []

    # Re-extrair bus_ids com base nos dados atuais (melhor prática)
    cols_vm_pu_antes = [col for col in df_cenario_filtered.columns if col.startswith('vm_pu_antes_bus_')]
    bus_ids_numeric = sorted([int(col.replace('vm_pu_antes_bus_', '')) for col in cols_vm_pu_antes])


    for linha_desligada in sorted(linhas_desligadas_unicas):
        df_filtered_linha = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]
        data_for_table = []

        for bus_id in bus_ids_numeric:
            col_antes = f'vm_pu_antes_bus_{bus_id}'
            col_depois = f'vm_pu_depois_bus_{bus_id}'

            vm_pu_antes = df_filtered_linha.get(col_antes)
            vm_pu_depois = df_filtered_linha.get(col_depois)
            
            if pd.isna(vm_pu_antes) or pd.isna(vm_pu_depois) or vm_pu_antes is None or vm_pu_depois is None:
                variation = None
            else:
                variation = vm_pu_depois - vm_pu_antes

            data_for_table.append({
                'Barra': f'Barra {bus_id}',
                'Tensao_Antes_pu': vm_pu_antes,
                'Variacao_Tensao_pu': variation,
                'Abs_Variacao_Tensao_pu': abs(variation) if variation is not None else -1
            })
        
        df_table = pd.DataFrame(data_for_table)
        df_table = df_table.sort_values(by='Abs_Variacao_Tensao_pu', ascending=False)
        df_table = df_table.head(10) # Top 10 barras mais impactadas
        df_table = df_table.drop(columns=['Abs_Variacao_Tensao_pu'])

        linha_info = df_cenario_filtered[df_cenario_filtered['linha_desligada'] == linha_desligada].iloc[0]
        from_bus = int(linha_info['from_bus']) if pd.notna(linha_info.get('from_bus')) else 'N/A'
        to_bus = int(linha_info['to_bus']) if pd.notna(linha_info.get('to_bus')) else 'N/A'
        linha_title = f"Linha {linha_desligada} (Barras {from_bus}-{to_bus})"

        all_tables.append(
            html.Div([
                html.H3(f"Top 10 Barras Mais Impactadas para {linha_title}"),
                dash_table.DataTable(
                    id=f'dynamic-table-{linha_desligada}',
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
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    sort_action="native", filter_action="native"
                )
            ], style={'marginBottom': '30px', 'padding': '15px', 'border': '1px solid #eee', 'borderRadius': '5px'})
        )
    
    if not all_tables:
        return html.Div("Nenhuma linha desligada não-crítica encontrada para este cenário.")

    return all_tables

# --- Função para iniciar o servidor Dash (apenas quando o script é executado diretamente) ---
def run_dash_server_as_subprocess():
    """
    Inicia o servidor Dash em uma thread para não bloquear o processo chamador.
    Esta função foi mantida por compatibilidade, mas o Dash é iniciado
    principalmente via o bloco if __name__ == '__main__'.
    """
    def run_server():
        app.run_server(debug=False, host='0.0.0.0', port=8050)

    server_thread = threading.Thread(target=run_server)
    server_thread.daemon = True
    server_thread.start()
    print(f"DEBUG: Dash server iniciado em segundo plano na URL: http://127.0.0.1:8050/")



if __name__ == '__main__':
    # Quando este script é executado diretamente (e.g., para desenvolvimento ou inicialização manual),
    # ele inicia o servidor Dash e abre o navegador.
    print(f"Iniciando a aplicação Dash na URL: http://127.0.0.1:8050/")
    webbrowser.open_new_tab("http://127.0.0.1:8050/")
    app.run_server(debug=True, host='0.0.0.0', port=8050)