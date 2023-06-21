"""
Main module of the project.
"""
import argparse
import datetime
import os

from logging_utils import setup_logger
from config_utils import load_config

from data_utils import (
    create_df_from_excel,
    prepare_params,
)


def main(config_file_path: str):
    """
    Main function of the project.

    Args:
        config_file_path (str): The path to the configuration file.

    Returns:
        None
    """
    config = load_config(config_file_path)

    logger = setup_logger(config["logging"]["level"])

    date = datetime.datetime.now().date().strftime("%Y-%m-%d")
    path_dir_data = os.path.join(config["path"]["path_dir_data"], date)
    os.makedirs(path_dir_data, exist_ok=True)

    params = prepare_params(config, date, path_dir_data)

    create_df_from_excel(
        **params,
    )
    logger.info("Process completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Provide the path to the configuration file."
    )
    parser.add_argument(
        'config_file_path',
        type=str,
        help='Path to the configuration file.',
        default="config.ini",
    )

    args = parser.parse_args()

    main(args.config_file_path)
