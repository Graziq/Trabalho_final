import pandapower as pp
import pandapower.networks as pn
import random
from pandapower.topology import create_nxgraph
import networkx as nx
import pandas as pd


def criar_rede_ieee30_slack_bar():
    net = pn.case30()
    net.gen['slack'] = False
    gen_idx_at_bus0 = net.gen[net.gen['bus'] == 0].index
    if not gen_idx_at_bus0.empty:
        net.gen.at[gen_idx_at_bus0[0], 'slack'] = True
    else:
        pp.create_gen(net, bus=0, p_mw=0, vm_pu=1.0, slack=True)
    return net

def gerar_dados_cenario(net, cenario_id=None):
    dados = {}
    for idx in net.load.index:
        p = net.load.at[idx, 'p_mw'] * random.uniform(0.95, 1.05)
        dados[f'carga_p_mw_{idx}'] = p

    for idx in net.gen.index:
        p = net.gen.at[idx, 'p_mw'] * random.uniform(0.95, 1.05)
        
        if net.gen.at[idx, 'slack']:
            vm = net.gen.at[idx, 'vm_pu'] 
        else:
            vm = net.gen.at[idx, 'vm_pu'] * random.uniform(0.995, 1.005)

        slack = net.gen.at[idx, 'slack']
        dados[f'gen_p_mw_{idx}'] = p
        dados[f'gen_vm_pu_{idx}'] = vm
        dados[f'gen_slack_{idx}'] = slack

    if cenario_id is not None:
        dados['cenario'] = cenario_id
    return dados

def aplicar_dados_ao_net(net, dados):
    for idx in net.load.index:
        net.load.at[idx, 'p_mw'] = dados[f'carga_p_mw_{idx}']
    for idx in net.gen.index:
        net.gen.at[idx, 'p_mw'] = dados[f'gen_p_mw_{idx}']
        net.gen.at[idx, 'vm_pu'] = dados[f'gen_vm_pu_{idx}']
        net.gen.at[idx, 'slack'] = dados[f'gen_slack_{idx}']

def simular_cenarios():
    net_base = criar_rede_ieee30_slack_bar()

    pp.runpp(net_base, numba=False, init='flat')
    print("Rede base carregada:")
    print(f" - Tensão mínima: {net_base.res_bus.vm_pu.min():.4f} pu")
    print(f" - Tensão máxima: {net_base.res_bus.vm_pu.max():.4f} pu")
    print(f" - Carregamento máximo de linha: {net_base.res_line.loading_percent.max():.2f} %")
    print("------")

    vmax = 1.093
    vmin = 0.94
    line_loading_max = 120

    resultados = []

    
    n_cenarios = 50 # para 50 cenários
    

    for cenario_id in range(n_cenarios):
        dados_cenario = gerar_dados_cenario(net_base, cenario_id)

        linhas_para_testar = list(net_base.line.index)

        for linha in linhas_para_testar:
            net = criar_rede_ieee30_slack_bar()
            aplicar_dados_ao_net(net, dados_cenario)
            net.line.at[linha, 'in_service'] = False

            # Comentei os prints detalhados dentro do loop de linhas para não poluir demais a saída
            # quando houver muitos cenários. Descomente se precisar depurar novamente.
            # print(f"\n--- Cenário {cenario_id}, Desligando linha {linha} (de bus {net.line.at[linha, 'from_bus']} para bus {net.line.at[linha, 'to_bus']}) ---")

            graph = create_nxgraph(net, respect_switches=True)
            
            slack_buses = net.gen[net.gen['slack']].bus.values
            # print(f"Barras Slack identificadas: {slack_buses}")

            ilhamento = False
            componentes = list(nx.connected_components(graph))
            # print(f"Número de componentes conectados: {len(componentes)}")
            for i, component in enumerate(componentes):
                component_set = set(component)
                slack_present_in_component = any(bus in component_set for bus in slack_buses)
                # print(f"  Componente {i} (Tamanho: {len(component_set)}): Contém barra slack? {slack_present_in_component}")

                if not slack_present_in_component:
                    ilhamento = True
                    # print(f"  ⚠️ Ilhamento detectado: Componente {i} não contém nenhuma barra slack.")
                    break

            if ilhamento:
                status = 'ilhamento'
                print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Ilhamento detectado")
            else:
                try:
                    pp.runpp(net, numba=False, init='flat')

                    vm_min = float(net.res_bus.vm_pu.min())
                    vm_max = float(net.res_bus.vm_pu.max())
                    loading_max = float(net.res_line.loading_percent.max())

                    status = 'normal'
                    if vm_min < vmin:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão mínima ({vm_min:.4f} pu) abaixo do limite ({vmin:.4f} pu)")
                        status = 'crítica'
                    elif vm_max > vmax:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Tensão máxima ({vm_max:.4f} pu) acima do limite ({vmax:.4f} pu)")
                        status = 'crítica'
                    elif loading_max > line_loading_max:
                        print(f"Cenário {cenario_id}, linha {linha}: ⚠️ Carregamento de linha ({loading_max:.2f} %) excedido ({line_loading_max:.2f} %)")
                        status = 'crítica'
                    else:
                        pass # Não precisa imprimir nada para o caso OK, para manter a saída mais limpa
                        # print(f"Cenário {cenario_id}, linha {linha}: ✅ OK - Sem criticidade de tensão ou carregamento.")

                except pp.LoadflowNotConverged:
                    status = 'crítica (não convergiu)'
                    print(f"Cenário {cenario_id}, linha {linha}: ❌ Fluxo não convergiu")

            resultados.append({
                'cenario': cenario_id,
                'linha_desligada': linha,
                'status': status,
                'ilhamento': ilhamento,
                'num_componentes_conectados': len(componentes) if not ilhamento else None,
                'convergencia': status != 'crítica (não convergiu)'
            })

    return resultados

resultados = simular_cenarios()

df = pd.DataFrame(resultados)
df.to_csv('resultados_simulacao_ieee30.csv', index=False)
print("Simulação concluída. Resultados salvos em 'resultados_simulacao_ieee30.csv'.")