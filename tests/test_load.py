import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json
from src import loading as load

def test_load_data(tmp_path):
    """Testa se o load grava corretamente o JSON no arquivo"""
    data = [
        {"user_id": 1, "completed": True},
        {"user_id": 2, "completed": False}
    ]
    output_path = tmp_path / "output.json"

    # força o uso do diretório temporário
    load.DATA_DIR = tmp_path
    load.save_data(data)  # você precisa ter uma função save_data() no load.py

    # verifica se arquivo foi criado
    with open(output_path) as f:
        result = json.load(f)

    assert result == data
