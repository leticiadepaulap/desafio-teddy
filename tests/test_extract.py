import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json
from src import extract

def test_extract_json(tmp_path):
    """Testa se o extract lê corretamente um arquivo JSON"""
    # cria arquivo de teste
    data = [
        {"userId": 1, "completed": True},
        {"userId": 2, "completed": False}
    ]
    file_path = tmp_path / "raw_data.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # força o caminho de teste
    extract.DATA_DIR = tmp_path
    result = extract.read_data()  # você precisa ter uma função read_data() no extract.py

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["userId"] == 1
