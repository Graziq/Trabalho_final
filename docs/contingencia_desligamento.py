import pandas as pd
import pandapower as pp
import pandapower.networks as ppnets
import pandapower.plotting as plt
import matplotlib.pyplot as mplt

# Carrega o sistema de teste IEEE 14 barras
net = ppnets.case14()

# Modificações para demonstração (ajustes nos dados originais)
net.ext_grid['in_service'] = False  # Desliga a fonte externa (ext_grid), o objetivo é que a rede fique autônoma para que eu consiga sobrecarregar mais ela
net.load.scaling = 1.5  # Aumentei a carga em 50%
net.gen['vm_pu'] = 1.045  # Tensão inicial dos geradores (0,95pu a 1,05 pu)

# Ajusta manual da potência ativa (p_mw) dos geradores
net.gen.loc[0, 'p_mw'] = 120
net.gen.loc[1, 'p_mw'] = 100
net.gen.loc[2, 'p_mw'] = 100
net.gen.loc[3, 'slack'] = True  # Define o gerador 3 referente à barra 7 como slack

# Executa fluxo de potência inicial
pp.runpp(net, numba=False)

# Mostra um resumo da geração e carga
gen_mw_total = net.res_gen['p_mw'].sum() #total gerado
imports_mw_total = net.res_ext_grid['p_mw'].sum() # total da potência importada (rede externa)

print('total gen MW:', gen_mw_total + imports_mw_total) #geração total (importada + local)
print('total imported gen MW:', imports_mw_total) 
print('total local gen MW:', gen_mw_total)
print('total load MW:', net.res_load['p_mw'].sum()) #carga consumida

# Exibe parâmetros e resultados dos geradores e barras
print("\nGenerator Parameters:\n", net.gen)
print("\nGenerator Results:\n", net.res_gen) 
print("\nBus Voltages:\n", net.res_bus)

######################################################################
# Listagem das linhas para ajudar na escolha manual
print("\nLista das linhas disponíveis para contingência:")
for idx, row in net.line.iterrows():
    print(f"Linha {idx}: de bus {row['from_bus']} para bus {row['to_bus']}")

######################################################################
# Simulação de contingência - desligamento manual de linha

# Escolha a linha que deseja desligar (altere este valor conforme necessidade)
linha_especifica = 3  # Exemplo: linha 3

# Verifica se a linha já está desligada
if not net.line.at[linha_especifica, 'in_service']:
    print(f"\nA linha {linha_especifica} já está desligada!")
else:
    # Tensões das barras antes da contingência para comparação
    original_vm = net.res_bus[['vm_pu', 'va_degree']].copy()
    original_vm.columns = ['vm_pu_orig', 'va_degree_orig']

    # Desliga a linha selecionada
    net.line.at[linha_especifica, 'in_service'] = False #Deixo ela fora de serviço
    print(f"\n>> Linha {linha_especifica} desligada: da barra {net.line.at[linha_especifica, 'from_bus']} para barra {net.line.at[linha_especifica, 'to_bus']}")

    # Executa fluxo de potência após desligamento da linha
    pp.runpp(net, numba=False)

    # Captura as tensões após a contingência, vm = magnitude da tensão, va = angulo
    after_vm = net.res_bus[['vm_pu', 'va_degree']].copy()
    after_vm.columns = ['vm_pu_post', 'va_degree_post']

    # Junta os dados antes e depois para comparação
    comparison = pd.concat([original_vm, after_vm], axis=1)
    comparison['delta_vm'] = comparison['vm_pu_post'] - comparison['vm_pu_orig']
    comparison['delta_va'] = comparison['va_degree_post'] - comparison['va_degree_orig']
    comparison = comparison.reset_index(drop=True)


    # Exibe as diferenças de tensão
    print("\nComparação das tensões nas barras antes e depois da contingência:")
    print(comparison.round(4))

    comparison['bus'] = net.bus.index  # ou .copy() para segurança


# ------------------------------------------------------
# Visualização do sistema com a linha desligada destacada

fig, ax = mplt.subplots()
fig.set_figheight(6)
fig.set_figwidth(8)

# Cria uma coleção de linhas para destacar a linha desligada em vermelho
critical_lc = plt.create_line_collection(net, [linha_especifica], color="r", zorder=2)
plt.draw_collections([critical_lc], ax=ax)

# Desenha o sistema elétrico com cargas e geradores
plt.simple_plot(net, plot_loads=True, plot_gens=True, ax=ax, show_plot=False)

mplt.show()
