import pandas as pd
from dash import Dash, html, dcc, dash_table, Input, Output
from pathlib import Path

# Configuração de caminhos
BASE_DIR = Path(__file__).parent.parent.parent
DATA_PATH = BASE_DIR / 'simulacao_resultados' / 'tensao_barras_nao_criticos_ieee30.csv'

app = Dash(__name__)

def calcular_impacto(df, cenario):
    """Calcula o impacto para todas as barras em um cenário específico"""
    resultados = []
    df_cenario = df[df['cenario'].astype(str) == str(cenario)]
    
    for _, row in df_cenario.iterrows():
        linha = row['linha_desligada']
        from_bus = row['from_bus']
        to_bus = row['to_bus']
        
        for col in [c for c in df.columns if c.startswith('vm_pu_antes_bus_')]:
            bus = col.split('_')[-1]
            tensao_antes = row[col]
            tensao_depois = row.get(f'vm_pu_depois_bus_{bus}')
            
            if pd.notna(tensao_antes) and pd.notna(tensao_depois):
                impacto = tensao_depois - tensao_antes
                resultados.append({
                    'Linha': f"Linha {linha} (Barras {from_bus}-{to_bus})",
                    'Barra': f"Barra {bus}",
                    'Tensão Inicial (p.u.)': f"{tensao_antes:.4f}",
                    'Variação (p.u.)': f"{impacto:.4f}",
                    'Tensão Final (p.u.)': f"{tensao_depois:.4f}",
                    '|Variação|': abs(impacto)
                })
    
    return pd.DataFrame(resultados)

def criar_tabelas(df_impacto):
    """Cria uma tabela Dash para cada linha desligada"""
    tabelas = []
    linhas = df_impacto['Linha'].unique()
    
    for linha in linhas:
        df_linha = df_impacto[df_impacto['Linha'] == linha]
        top_10 = df_linha.nlargest(10, '|Variação|')
        
        tabelas.append(html.Div([
            html.H3(linha, style={'color': '#2c3e50', 'marginBottom': '10px'}),
            dash_table.DataTable(
                id=f'tabela-{linha}',
                columns=[
                    {'name': 'Barra', 'id': 'Barra'},
                    {'name': 'Tensão Inicial (p.u.)', 'id': 'Tensão Inicial (p.u.)'},
                    {'name': 'Variação (p.u.)', 'id': 'Variação (p.u.)'},
                    {'name': 'Tensão Final (p.u.)', 'id': 'Tensão Final (p.u.)'}
                ],
                data=top_10.to_dict('records'),
                style_table={
                    'width': '90%',
                    'margin': '0 auto',
                    'boxShadow': '0 4px 8px 0 rgba(0,0,0,0.2)',
                    'borderRadius': '10px'
                },
                style_cell={
                    'textAlign': 'center',
                    'padding': '12px',
                    'fontFamily': 'Arial, sans-serif'
                },
                style_header={
                    'backgroundColor': '#3498db',
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    }
                ]
            )
        ], style={
            'marginBottom': '40px',
            'padding': '20px',
            'backgroundColor': '#f8f9fa',
            'borderRadius': '10px'
        }))
    
    return html.Div(tabelas)

app.layout = html.Div([
    html.Div([
        html.H1("Análise de Impacto nas Barras", style={
            'textAlign': 'center',
            'color': '#2c3e50',
            'padding': '20px',
            'backgroundColor': '#f8f9fa'
        }),
        
        html.Div([
            dcc.Dropdown(
                id='dropdown-cenario',
                placeholder="Selecione um cenário...",
                style={
                    'width': '50%',
                    'margin': '0 auto',
                    'fontSize': '16px'
                }
            )
        ], style={'padding': '20px', 'textAlign': 'center'}),
        
        html.Div(id='tabelas-container')
    ], style={'maxWidth': '1200px', 'margin': '0 auto'})
])

@app.callback(
    Output('dropdown-cenario', 'options'),
    Input('dropdown-cenario', 'id')
)
def carregar_cenarios(_):
    try:
        df = pd.read_csv(DATA_PATH)
        return [{'label': f'Cenário {int(c)}', 'value': str(c)} 
                for c in sorted(df['cenario'].unique())]
    except Exception as e:
        print(f"Erro ao carregar cenários: {e}")
        return []

@app.callback(
    Output('tabelas-container', 'children'),
    Input('dropdown-cenario', 'value')
)
def atualizar_tabelas(cenario_selecionado):
    if not cenario_selecionado:
        return html.Div(
            "Selecione um cenário para visualizar os dados de impacto",
            style={
                'textAlign': 'center',
                'fontSize': '18px',
                'padding': '40px',
                'color': '#7f8c8d'
            }
        )
    
    try:
        df = pd.read_csv(DATA_PATH)
        df_impacto = calcular_impacto(df, cenario_selecionado)
        
        if df_impacto.empty:
            return html.Div(
                f"Nenhum dado encontrado para o Cenário {cenario_selecionado}",
                style={
                    'textAlign': 'center',
                    'fontSize': '18px',
                    'padding': '40px',
                    'color': '#e74c3c'
                }
            )
        
        return criar_tabelas(df_impacto)
    except Exception as e:
        return html.Div(
            f"Erro ao processar dados: {str(e)}",
            style={
                'textAlign': 'center',
                'color': '#e74c3c',
                'padding': '40px'
            }
        )

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=8050,
        debug=True,
        dev_tools_hot_reload=False
    )