"""
Main entry point for XWYZ batch script scrapping
For a given processed CSV file of patients, fetch all
associated data and interact with the XWYZ portal to submit them
into their database.

When the script ends, it produces a zip file containing :
  * An anonymized Excel sheet containing scrapped patient data with
    an associated comment if an issue has occured.
  * All available PDF for each patient.
    The name of each PDF is the PIC of the associated patient.

After the script has ended, send the email with the
zipped file back to the hospital.
"""
import os
import time

from common import catch_all_exceptions

from aws.process import ProcessPatients
from config_utilities import (
    load_config,
    setup_paths,
    load_yaml_config,
    fetch_env_vars,
)


@catch_all_exceptions
def main():
    """
    Main entry point. Read and process all parameters
    and env vars before calling the main script.
    """
    # Load config file
    start_time = time.time()

    config = load_config()

    # Setup paths
    paths = setup_paths(config)

    # Load YAML config
    config_yaml = load_yaml_config()
    env_variables = fetch_env_vars()

    process_patients_class = ProcessPatients(
        username=env_variables.get("HEALTHFORCE_XWYZ_USERNAME"),
        password=env_variables.get("HEALTHFORCE_XWYZ_PASSWORD"),
        filename_output=paths["path_file_output_excel"],
        webdriver_headless=config.getboolean("webdriver", "headless"),
        path_dir_output=paths["path_dir_data"],
        path_exec_firefox=config["path"]["path_exec_firefox"],
        zip_with_password=config.getboolean("path", "zip_with_password"),
        config_yaml=config_yaml,
        path_file_input=os.path.expanduser(
            os.path.join(
                paths["path_dir_data"], config["path"]["filename_input"])
        ),
    )
    process_patients_class.process_patients()
    # Stop the timer
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the execution time
    print(f"Elapsed time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()


