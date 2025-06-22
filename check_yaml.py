import yaml
from pathlib import Path

file_path = Path("prefect.yaml")

if not file_path.exists():
    print(f"Erro: O arquivo {file_path} não foi encontrado.")
else:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        print("Conteúdo do YAML carregado com sucesso:")
        print(config)

        if 'deployments' in config and config['deployments'] is not None:
            print("\nLista de deployments encontrada:")
            for dep in config['deployments']:
                print(f"- {dep.get('name', 'Nome não encontrado')}")
        else:
            print("\nERRO: 'deployments' não encontrado ou é None no arquivo YAML.")

    except yaml.YAMLError as e:
        print(f"Erro ao analisar o YAML: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")