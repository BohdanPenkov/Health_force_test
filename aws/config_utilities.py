import configparser
import os
import yaml
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv


def load_config(config_file: str = "config.ini"):
    """
    Load config file
    Args:
        config_file: filename of config file

    Returns:

    """
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.parent / config_file)
    return config


def setup_paths(config):
    """
    Setup paths
    Args:
        config: filename of config file

    Returns:

    """
    path_dir_data = config["path"]["path_dir_data"]
    if "{date}" in path_dir_data:
        path_dir_data = path_dir_data.format(date=datetime.now().date().__str__())
    os.makedirs(path_dir_data, exist_ok=True)

    path_file_output_excel = os.path.expanduser(
        os.path.join(path_dir_data, config["path"]["filename_output"])
    )

    path_file_input = os.path.expanduser(
        os.path.join(path_dir_data, config["path"]["filename_input"])
    )

    return {
        "path_dir_data": path_dir_data,
        "path_file_output_excel": path_file_output_excel,
        "path_file_input": path_file_input,
    }


def load_yaml_config(file_yaml: str = "config.yaml"):
    """
    Load YAML config file
    Args:
        file_yaml: filename of YAML config file

    Returns:

    """
    with open(file_yaml, "r") as file:
        try:
            config_yaml = yaml.safe_load(file)
        except (yaml.YAMLError, FileNotFoundError) as e:
            raise ValueError(f"Error loading YAML file: {e}")
    return config_yaml


def fetch_env_vars():
    load_dotenv()
    return {
        "HEALTHFORCE_XWYZ_USERNAME": os.getenv("HEALTHFORCE_XWYZ_USERNAME"),
        "HEALTHFORCE_XWYZ_PASSWORD": os.getenv("HEALTHFORCE_XWYZ_PASSWORD"),
    }
