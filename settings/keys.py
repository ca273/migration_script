import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_system_key(key_name: str):
    with open(f"{PROJECT_ROOT}/settings/keys.json") as keys_file:
        json_data = json.load(keys_file)
    return json_data.get(key_name, None)
