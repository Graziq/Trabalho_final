import pandapower as pp
import pandapower.networks as pn # Para carregar o exemplo case30

def obter_barras_conectadas_por_linha(net, lista_de_linhas_problema):
    """
    Retorna um dicionário mostrando quais barras (from_bus, to_bus)
    estão conectadas a cada linha em uma lista de linhas de interesse.
    """
    barras_por_linha = {}
    print("\n--- Barras Conectadas às Linhas Problema ---")
    for linha_id in lista_de_linhas_problema:
        if linha_id in net.line.index:
            line_data = net.line.loc[linha_id]
            from_bus = line_data.from_bus
            to_bus = line_data.to_bus
            barras_por_linha[linha_id] = {'from_bus': from_bus, 'to_bus': to_bus}
            print(f"Linha {linha_id}: Conecta a Barra {from_bus} e Barra {to_bus}")
        else:
            print(f"Aviso: Linha {linha_id} não encontrada na rede.")
    return barras_por_linha

# --- Exemplo de Uso ---
if __name__ == "__main__":
    # Carregue sua rede aqui. Se você estiver usando o IEEE 30-bus, pode ser assim:
    net_exemplo = pn.case30() 
    
    # Se você tem sua própria rede em um arquivo, carregue-a:
    # net_exemplo = pp.from_pickle("caminho/para/sua_rede.p")
    # net_exemplo = pp.from_json("caminho/para/sua_rede.json")

    # Linhas que causam Ilhamento
    linhas_ilhamento = [15, 12, 33]
    print("Análise para Cenários de Ilhamento:")
    barras_ilhamento = obter_barras_conectadas_por_linha(net_exemplo, linhas_ilhamento)
    # print("Dicionário de ilhamento:", barras_ilhamento) # Opcional: ver o dicionário completo

    print("\n" + "="*40 + "\n")

    # Linhas que causam Crítica
    linhas_criticas = [9, 24, 36, 25, 37, 39, 23, 35, 8, 27]
    print("Análise para Cenários Críticos:")
    barras_criticas = obter_barras_conectadas_por_linha(net_exemplo, linhas_criticas)
    # print("Dicionário de críticas:", barras_criticas) # Opcional: ver o dicionário completo