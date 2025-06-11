# 🔌 Análise de Variação de Tensão com Pandapower e Prefect

Este projeto utiliza a biblioteca [**Pandapower**](https://www.pandapower.org/) para simular redes elétricas e o [**Prefect**](https://docs.prefect.io/) para orquestrar e monitorar os fluxos de trabalho (flows) de simulação.

## ⚡ Objetivo

Avaliar como o desligamento ou religamento de linhas de transmissão afeta os **níveis de tensão** em um sistema elétrico baseado no caso-padrão **IEEE 14 barras**, garantindo que **não ocorra ilhamento** na rede durante as manobras.

---

## 🧰 Tecnologias Utilizadas

- [Python 3.12.5](https://www.python.org/)
- [Pandapower](https://www.pandapower.org/)
- [Prefect 2.16.4](https://docs.prefect.io/)
- Docker + Docker Compose

---

## 🧪 Organização do Projeto

```bash
.
├── flows/                  # Fluxos Prefect (hello_flow, simulações)
│   └── hello_world.py
├── data-eee14/
│   └── ieee14.xlsx         # Dados do sistema elétrico (caso IEEE 14 barras)
├── requirements.txt
├── docker-compose.yml      # Ambiente Prefect Server
├── .github/workflows/
│   └── prefect-deploy.yml  # CI/CD com GitHub Actions
└── README.md
