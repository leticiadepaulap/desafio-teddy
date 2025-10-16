import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json
from src import transform

def test_transform_and_save(tmp_path):
    """Testa filtragem de completed=True e renomeação de userId → user_id"""
    raw_data = [
        {"userId": 1, "completed": True, "title": "ok"},
        {"userId": 2, "completed": False, "title": "falha"}
    ]
    input_path = tmp_path / "raw_data.json"
    output_path = tmp_path / "filtered_data.json"

    # salva arquivo de entrada
    with open(input_path, "w") as f:
        json.dump(raw_data, f)

    # força o uso do diretório temporário
    transform.DATA_DIR = tmp_path
    transform.transform_and_save()

    # lê resultado
    with open(output_path) as f:
        result = json.load(f)

    assert len(result) == 1
    assert result[0]["user_id"] == 1
    assert "userId" not in result[0]
