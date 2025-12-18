import json
from pathlib import Path


def load_settings(file_path=None):
    """
    Load settings JSON. If path is not provided, use config/settings.json relative to this module.
    """
    if file_path is None:
        file_path = Path(__file__).resolve().parent / "settings.json"
    else:
        file_path = Path(file_path)

    with open(file_path, "r") as f:
        return json.load(f)
