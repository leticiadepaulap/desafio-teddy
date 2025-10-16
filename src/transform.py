import os
import json
import logging

# Configurar logging
logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data"

def transform_and_save():
    """Filtra apenas os registros com completed=True, renomeia userId→user_id e salva em novo arquivo"""
    try:
        input_path = os.path.join(DATA_DIR, "raw_data.json")
        output_path = os.path.join(DATA_DIR, "filtered_data.json")

        with open(input_path, "r") as f:
            data = json.load(f)

        # Filtra e renomeia campos
        filtered = []
        for item in data:
            if item.get("completed") is True:
                # renomear a chave "userId" para "user_id", se existir
                if "userId" in item:
                    item["user_id"] = item.pop("userId")
                filtered.append(item)

        with open(output_path, "w") as f:
            json.dump(filtered, f, indent=2)

        logger.info(f"{len(filtered)} registros filtrados e salvos em {output_path}")

    except Exception as e:
        logger.error(f"Erro na transformação dos dados: {str(e)}")
        raise
