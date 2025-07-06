import subprocess
import os
import sys

def run_dash_server_as_subprocess():
    """
    Executa o visualizacao_impacto.py como subprocesso.
    """
    script_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "flows"))
    dash_script_path = os.path.join(script_dir, "visualizacao_impacto.py")

    if not os.path.exists(dash_script_path):
        raise FileNotFoundError(f"visualizacao_impacto.py não encontrado em: {dash_script_path}")

    print(f"Iniciando Dash server via subprocess: {dash_script_path} ...")

    process = subprocess.Popen(
        [sys.executable, dash_script_path],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    print(f"Dash iniciado como subprocesso com PID {process.pid}")
    return process