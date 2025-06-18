# analise_barras.py
import pandas as pd
import os

def analisar_top_barras(csv_path, output_md="resultados.md"):
    df_tensao = pd.read_csv(csv_path)
    
    with open(output_md, 'w') as f:
        f.write("# Relatório das 10 Barras Mais Impactadas\n\n")
        
        for cenario in df_tensao['cenario'].unique():
            f.write(f"## Cenário {cenario}\n\n")
            df_cenario = df_tensao[df_tensao['cenario'] == cenario]
            
            for _, row in df_cenario.iterrows():
                impactos = []
                for col in row.index:
                    if col.startswith('vm_pu_antes_bus_'):
                        bus = col.replace('vm_pu_antes_bus_', '')
                        antes = row[col]
                        depois = row[f'vm_pu_depois_bus_{bus}']
                        if pd.notna(antes) and pd.notna(depois):
                            impacto = abs(depois - antes)
                            impactos.append((bus, impacto))
                
                top_10 = sorted(impactos, key=lambda x: x[1], reverse=True)[:10]
                
                f.write(f"### Linha {row['linha_desligada']} (Barras {row['from_bus']}-{row['to_bus']})\n")
                f.write("| Barra | Variação (p.u.) |\n")
                f.write("|-------|-----------------|\n")
                for bus, impacto in top_10:
                    f.write(f"| {bus} | {impacto:.4f} |\n")
                f.write("\n")

if __name__ == "__main__":
    input_csv = os.getenv('INPUT_CSV', 'tensao_barras_nao_criticos_ieee30.csv')
    analisar_top_barras(input_csv)