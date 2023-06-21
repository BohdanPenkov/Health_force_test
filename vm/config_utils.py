"""
This module contains utility functions for loading the configuration.
"""
import configparser


def load_config(file_path: str):
    """
    Loads the configuration from the specified file.

    Args:
        file_path (str): The path to the configuration file.

    Returns:
        A ConfigParser object with the loaded configuration.
    """
    config = configparser.ConfigParser()
    config.read(file_path)
    return config
