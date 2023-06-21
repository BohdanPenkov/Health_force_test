"""
Data utilities for the project.
This file contains functions to process data.
"""
import os
import re
import logging
from enum import Enum

import dateutil.parser
import pandas as pd


logger = logging.getLogger(__name__)


class Insurance(Enum):
    """
    Enum for the accepted insurances.
    """
    QUAS = "QUAS"
    QUAS_PENSIONATI = "QUAS-PENSIONATI"


OSR_INSTITUTE = 1
SRT_INSTITUTE = 8


DICT_PRESTAZIONNE = {
    1: "ALTRE PRESTAZIONI O NESSUNA DELL'ELENCO",
    2: "Visite specialistiche",
    3: "Prestazioni di fisioterapia (MAX 500E/ANNO)",
    4: "Chirurgia dermatologica",
    5: "Visite specialistiche in gravidanza (assistito)",
    6: "Ecografie in gravidanza",
    7: "Visite specialistiche pediatriche (tutela del figlio)",
    8: "Prevenzione",
    9: "Fisioterapia oltre 500E SOLO PER CONDIZIONI SPECIALI",
    10: "Visite specialistiche (tutela del figlio, non pediatriche)",
}


def create_df_from_excel(
    path_file_excel_next_appointments: str,
    path_file_second_pnr: str,
    path_cat_code: str,
    accepted_insurances: tuple,
    result_file: str,
):
    """
    Main function (temp) to process data and print the resulting DataFrame.

    Args:
        path_file_excel_next_appointments (str): The root
        directory path to the files patients excel
        accepted_insurances (tuple[str]): A tuple of accepted insurances.
        result_file (str): The path to the result file.
        path_cat_code (str): The path to the file containing the cat codes.
        path_file_second_pnr (str): The path to the file containing
                                    the second PNR.

    Returns:
        None
    """

    # Open the file and get the correct header
    try:
        xls = pd.ExcelFile(path_file_excel_next_appointments)
        df_patients = pd.read_excel(xls, Insurance.QUAS.value, header=None)
    except Exception as e:
        logger.error(
            f"Failed to read Excel file "
            f"at {path_file_excel_next_appointments}: {e}"
        )
        return

    if "Descrizione_BusinessPartner" not in df_patients.columns:
        try:
            df_header = pd.read_excel(xls, "Tabella", header=1)
        except Exception as e:
            logger.error(
                f"Failed to read Excel file at "
                f"{path_file_excel_next_appointments} in "
                f"'Tabella' sheet: {e}"
            )
            return
        df_patients.columns = df_header.columns
    nb_patient = len(df_patients.index)
    logger.debug(f"Excel read {nb_patient} detected")

    df_patients = filter_minor_from_df(df_patients)

    df_patients = filter_accepted_insurances(df_patients, accepted_insurances)

    df_patients = add_pnr_to_df(df_patients)

    df_patients = add_check_2nd_pnr(df_patients, path_file_second_pnr)

    df_patients = add_cat_code(df_patients, path_cat_code)

    df_patients = extract_scadenza_from_df(df_patients)

    df_patients.to_csv(result_file)


def add_cat_code(
        df_patients: pd.DataFrame, path_cat_code: str
) -> pd.DataFrame:
    """
    Add the cat code to the dataframe

    Args:
        df_patients (pd.DataFrame): The dataframe to add the cat code to.
        path_cat_code (str): The path to the cat code file.

    Returns:
        pd.DataFrame: The dataframe with the cat code added.
    """
    if not os.path.exists(path_cat_code):
        logger.error(f"File at path {path_cat_code} not found.")
        return df_patients

    nb_patient_before = len(df_patients.index)
    try:
        xls = pd.ExcelFile(path_cat_code)
        if 'Codice' not in xls.sheet_names:
            logger.error(
                f"Sheet 'Codice' not "
                f"found in the file at {path_cat_code}"
            )
            return df_patients
        df_cat_code = pd.read_excel(xls, "Codice")
    except Exception as e:
        logger.error(
            f"An error occurred while reading "
            f"from the file at path {path_cat_code}: {e}")
        return df_patients

    required_columns = ['Codice Esame SAP', 'ID prestazioni']
    if not all(column in df_cat_code.columns for column in required_columns):
        logger.error("Required columns are not present in the file.")
        return df_patients
    if 'Esame' not in df_patients.columns:
        logger.error("Column 'Esame' not present in df_patients DataFrame.")
        return df_patients

    df_cat_code.drop_duplicates(inplace=True)
    join_right = df_cat_code[["Codice Esame SAP", "ID prestazioni"]]
    joined = pd.merge(
        df_patients,
        join_right,
        left_on="Esame",
        right_on="Codice Esame SAP",
        how="left",
    )
    joined["type_prestazioni"] = joined["ID prestazioni"].map(
        DICT_PRESTAZIONNE
    )

    nb_patient_after = len(joined.index)

    if nb_patient_before != nb_patient_after:
        logger.error(
            f"{nb_patient_before - nb_patient_after} "
            f"patients were dropped because of their ESAME. "
            f"This REALLY should not happen. DATA WAS LOST ! "
        )
    return joined


def filter_accepted_insurances(
    df_patients: pd.DataFrame,
    accepted_insurances: tuple[str]
) -> pd.DataFrame:
    """
    Filter the DataFrame to select appointement
    that matches the accepted insurances.

    Args:
        df_patients (pd.DataFrame): The input DataFrame.
        accepted_insurances (tuple[str]): A tuple of accepted insurances.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """

    nb_patient_before = len(df_patients.index)

    # We could do with the column "BusinessPartner" that contains
    # an int that seems to be a indentifiant of the insurance
    result = df_patients.loc[
        df_patients["Descrizione_BusinessPartner"].isin(accepted_insurances)
    ]

    nb_patient_after = len(df_patients.index)
    if nb_patient_before != nb_patient_after:
        logger.warning(
            f"{nb_patient_before - nb_patient_after} "
            f"patients were dropped because they don't "
            f"have the correct insurance, this should not happen"
        )
    return result


def filter_minor_from_df(df_patients: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the DataFrame to include only patients who are 18 years or older.

    Args:
        df_patients (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    nb_patient_before = len(df_patients.index)

    df_patients["age"] = (
            pd.Timestamp("now") - df_patients["Data_Di_Nascita"]
    ).astype("<m8[Y]")

    result = df_patients[df_patients["age"] >= 18]

    nb_patient_after = len(result.index)

    nb_minor = nb_patient_before - nb_patient_after

    if nb_minor > 0:
        logger.debug(
            f"{nb_minor} patients were minor and "
            f"therefore dropped from the file"
        )
    else:
        logger.debug("No minor patient detected")

    return result


def add_check_2nd_pnr(
    df_patients: pd.DataFrame, path_file_second_pnr: str
) -> pd.DataFrame:
    """
    Add a column to the DataFrame to indicate if an
    appointement requires a second PNR.

    Args:
        df_patients (pd.DataFrame): The input DataFrame.
        path_file_second_pnr (str): Path to file with codes
                                    that require a second PNR data.

    Returns:
        pd.DataFrame: The DataFrame with the added column.
    """

    xls = pd.ExcelFile(path_file_second_pnr)
    df_2nd_pnr_osr = pd.read_excel(xls, "OSR")
    df_2nd_pnr_srt = pd.read_excel(xls, "SRT")

    list_2nd_pnr_osr = df_2nd_pnr_osr["Prestazione"].to_list()
    list_2nd_pnr_srt = df_2nd_pnr_srt["Prestazione"].to_list()

    df_patients["second_pnr"] = False
    filtered_osr = df_patients[df_patients["Istituto"] == OSR_INSTITUTE]
    filtered_srt = df_patients[df_patients["Istituto"] == SRT_INSTITUTE]
    df_patients.loc[
        filtered_osr[filtered_osr["Esame"].isin(list_2nd_pnr_osr)].index,
        "second_pnr"
    ] = True
    df_patients.loc[
        filtered_srt[filtered_srt["Esame"].isin(
            list_2nd_pnr_srt
        )].index,
        "second_pnr"
    ] = True

    second_pnr_count = df_patients["second_pnr"].value_counts()
    nb_second_pnr = (
        0 if True not in second_pnr_count else second_pnr_count[True]
    )
    logger.debug(f"{nb_second_pnr} patients need a second pnr")
    return df_patients


def check_scadenza():
    """
    Check if the scadenza is not too close to the appointement date.
    """
    pass


def extract_scadenza_from_df(df_patients: pd.DataFrame) -> pd.DataFrame:
    """
    Extract scadenza information from the DataFrame and update the DataFrame.

    Args:
        df_patients (pd.DataFrame): The input DataFrame.

    Returns:
        None
    """

    nb_patient_before = len(df_patients.index)

    df_patients["scad"] = ""
    scad = pd.Series(df_patients["Note"])
    scad.dropna(inplace=True)
    pattern = r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b"
    scad = scad.str.findall(pattern).dropna()
    df_patients["scad"] = (
        scad.str[0].dropna().apply(dateutil.parser.parse, dayfirst=True)
    )

    nb_patient_after = len(df_patients.index)

    if nb_patient_before != nb_patient_after:
        logger.error(
            f"{nb_patient_before - nb_patient_after} "
            f"patients were dropped because of their Scadenza. "
            f"This REALLY should not happen. DATA WAS LOST ! "
        )
    return df_patients


def add_pnr_to_df(df_patients: pd.DataFrame) -> pd.DataFrame:
    """
    Add the PNR information to the DataFrame.

    Args:
        df_patients (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the added PNR column.
    """
    nb_patient_before = len(df_patients.index)
    pnr = df_patients["Note"]
    pnr.dropna(inplace=True)
    # filtered_df = pnr[pnr.str.contains("pnr", case=False)]
    pattern = r"\b[XB][XB]\w{6}\b"
    df_patients["PNR"] = pnr.str.findall(pattern, re.IGNORECASE)

    # print(df_patients)
    # df_patients["PNR"].fillna("").apply(list)

    idx = df_patients["PNR"].isna()
    df_patients.loc[idx, "PNR"] = df_patients.loc[idx, "PNR"].fillna(
        "[]"
    ).apply(
        eval
    )

    nb_patient_after = len(df_patients.index)
    nb_patient_after = len(df_patients.index)

    if nb_patient_before != nb_patient_after:
        logger.error(
            f"{nb_patient_before - nb_patient_after} "
            f"patients were dropped because of their ESAME. "
            f"This REALLY should not happen. DATA WAS LOST ! "
        )
    return df_patients


def prepare_params(config, date, path_dir_data):
    """
    Prepare parameters for create_df_from_excel function.

    Args:
        config (dict): The configuration dictionary.
        date (str): Current date as string.
        path_dir_data (str): The directory for the data.

    Returns:
        dict: A dictionary with the prepared parameters.
    """
    input_file = os.path.join(
        path_dir_data, config["path"]["filename_input"]
    )
    output_file = os.path.join(
        path_dir_data, config["path"]["filename_output"]
    )

    path_file_excel_next_appointments = os.path.join(
        os.path.expanduser(config["path"]["path_file_input"]),
        input_file
    )
    path_file_second_pnr = os.path.expanduser(
        config["path"]["path_file_second_pnr"]
    )
    path_cat_code = os.path.expanduser(
        config["path"]["path_cat_code"]
    )
    result_file = os.path.join(
        os.path.expanduser(
            config["path"]["path_file_output"].format(date=date)
        ),
        output_file
    )

    return {
        "path_file_excel_next_appointments": path_file_excel_next_appointments,
        "path_file_second_pnr": path_file_second_pnr,
        "path_cat_code": path_cat_code,
        "accepted_insurances": (
            Insurance.QUAS.value, Insurance.QUAS_PENSIONATI.value
        ),
        "result_file": result_file,
    }

