""" Main code retrieving patient data with webdriver requests and HTML scrapping. """
import csv
import os
import time
from datetime import datetime
from typing import List, Dict

import rule_engine
from dateutil.relativedelta import relativedelta
from rich.console import Console
from selenium.webdriver.common.by import By

from logger import logger
from pnr_status import PNR_STATUS_MANAGER
from read_pdf import check_pdf
from response import Response
from rules_engine import RULES_ENGINE
from telerik_bypass import fetch_telerik_pdf
from webdriver import WebDriver

console = Console()


class ProcessPatients:
    def __init__(
        self,
        username: str,
        password: str,
        filename_output: str,
        webdriver_headless: bool,
        path_dir_output: str,
        path_exec_firefox: str,
        path_file_input: str,
        zip_with_password: bool,
        config_yaml: dict,
    ):
        self.username = username
        self.password = password
        self.filename_output = filename_output
        self.webdriver_headless = webdriver_headless
        self.path_dir_output = path_dir_output
        self.path_exec_firefox = path_exec_firefox
        self.path_file_input = path_file_input
        self.zip_with_password = zip_with_password
        self.config_yaml = config_yaml
        self.webdriver = WebDriver(
            path_dir_output=self.path_dir_output,
            path_exec_firefox=self.path_exec_firefox,
            headless=self.webdriver_headless,
        )
        self.response = Response(
            path_file_output=os.path.join(
                self.path_dir_output, self.filename_output
            ),
            zip_with_password=self.zip_with_password,
        )

    def process_patients(self):
        patients = self._parse_input_from_hospital(self.path_file_input)

        # Initialize webdriver & connect to the insurance web portal
        self.login()

        # Process patients batch
        for patient in patients:
            logger.info(f'Processing patient : "{patient}"')
            self.process_patient(patient)

        # Need to call this method otherwise the driver process stay in memory
        self.webdriver.quit()

        # Prepare & send the response to the hospital
        self.response.send_mail_to_hospital()

    def login(self):
        """
        Login to the portal with a given web session.
        """
        url_login = (
            r"https://app.investire-in-italy/"
            r"CentriDiagnostici/MenuCentri"
        )

        if self.username == "" or self.password == "":
            raise ValueError(
                "The username or password are missing, it's"
                " not possible to login to the insurance server"
            )

        # Login into XWYZ is a simple POST on a form, they may try to patch it.
        payload = {"UserName": self.username, "Password": self.password}
        self.webdriver.post(url_login, payload=payload)

        # Login must be made twice on this site for some reason
        return self.webdriver.post(url_login, payload=payload)

    def _parse_input_from_hospital(
        self,
        path_file_input: str,
    ) -> list[dict]:
        """
        Parse the CSV file resulting of the processing executed
        in the hospital virtual machine.
        Assumes that the CSV contains a header
        as well as matching keys.
        """

        element_array = []
        with open(path_file_input) as fp:
            for row in csv.DictReader(fp):
                item_dict = {}
                for item in self.config_yaml["mapping"]["csv"]:
                    if item["coltype"] == "string":
                        item_dict[item["var_name"]] = row[item["colname"]]
                    elif item["coltype"] == "array":
                        item_dict[item["var_name"]] = ast.literal_eval(
                            row[item["colname"]]
                        )
                    elif item["coltype"] == "bool":
                        item_dict[item["var_name"]] = row[item["colname"]].lower() in (
                            "true",
                        )
                    elif item["coltype"] == "date":
                        item_dict[item["var_name"]] = datetime.strptime(
                            row[item["colname"]], item["date_format"]
                        )
                    else:
                        raise ValueError(
                            "Unknown type found in the YAML config file"
                        )
                element_array.append(item_dict)

        return element_array

    def process_patient(self, patient_data: dict):
        patient_data["age"] = relativedelta(
            datetime.now(), patient_data["birthday"]
        ).years
        comments = []

        engine = RULES_ENGINE(self.config_yaml)
        engine_results = engine.execute(
            "rules_context.deal_breakers.rules",
            patient_data,
            comments
        )

        if engine_results["passed"] > 0:
            patient_data["pic"] = None
            self.response.add_patient(
                patient_data=patient_data,
                comments=" / ".join(engine_results["actions"]),
            )
            return

        pnr_status_manager = PNR_STATUS_MANAGER("config.yaml")

        for pnr in patient_data["pnr"]:
            logger.info(f"Processing PNR : {pnr}")
            patient_data, comments = self.process_pnr(
                engine, patient_data, pnr,
                pnr_status_manager, comments
            )

        self.response.add_patient(
            patient_data=patient_data,
            comments=" / ".join(comments),
        )

    def process_pnr(
        self, engine, patient_data: dict, pnr: str, pnr_status_manager, comments: List
    ) -> tuple:
        html_page_identify = self.fetch_patient_data(pnr)
        patient_data["pnr_status"] = pnr_status_manager.get_pnr_status(
            html_page_identify
        )

        if rule_engine.Rule("pnr_status in [1,2]").evaluate(patient_data):
            if rule_engine.Rule("pnr_status == 1").evaluate(patient_data):
                self._check_request_accepted(
                    pnr,
                    patient_data["esame"],
                    patient_data["prestazioni"]
                )

            patient_data["pic"] = self._fetch_pic_from_database(
                patient_pnr=pnr
            )

            fetch_telerik_pdf(
                webdriver=self.webdriver, patient_pic=patient_data["pic"]
            )

            error_codes = check_pdf(
                pic_number=patient_data["pic"],
                insurance_name=patient_data["insurance_name"],
                fiscal_code=patient_data["codice_fiscale"],
            )

            engine.execute(
                "rules_context.patient_data.rules", patient_data, comments
            )

            engine.execute(
                "rules_context.pdf_analysis.rules",
                {"error_codes": error_codes},
                comments,
            )

        else:
            patient_data["pic"] = None

        engine.execute(
            "rules_context.webportal.rules", patient_data, comments
        )

        return patient_data, comments

    def fetch_patient_data(self, patient_pnr: str) -> str:
        return self.webdriver.get(
            f"https://app.investire-in-italy.it/"
            f"GestionePNR/CercaQuadro?PNR={patient_pnr}"
        ).text

    def _fetch_pic_from_database(self, patient_pnr: str) -> str:
        url_database = (
            r"https://app.investire-in-italy.it/"
            r"CentriDiagnostici/ControlloQuadri/"
            r"GridAutorizzazioni_Read"
        )
        payload = {
            "page": 1,
            "pageSize": 50,
            "FieldFilter": patient_pnr,
        }

        result = self.webdriver.post(url_database, payload=payload)
        patient_pic = result.json()["Data"][0]["NumeroAuth"]

        return patient_pic

    def _check_request_accepted(
        self, patient_pnr: str, patient_esame: str, code_prestazioni: str
    ):
        url_identify_patient = (
            r"https://app.investire-in-italy.it/"
            r"CentriDiagnostici/ControlloQuadri/Index2"
        )
        self.webdriver.get(url_identify_patient, backend="selenium")

        # Find where to input patient PNR
        self.webdriver.find_element(
            by=By.ID, value="PNR"
        ).send_keys(patient_pnr)

        # Submit search. The button is behind an alert,
        # a javascript click can overcome this.
        self.webdriver.click_js(
            self.webdriver.find_element(
                by=By.ID, value="cercaQuadro"
            )
        )

        # Accept that this is our patient, at the bottom of the page
        time.sleep(1)
        self.webdriver.click_js(
            self.webdriver.find_element(
                by=By.ID, value="btnQuadroOK"
            )
        )

        # Select correct prestazioni from scroller
        time.sleep(1)
        self.webdriver.find_element(
            By.CSS_SELECTOR,
            ".k-widget.k-multiselect.k-multiselect-clearable"
        ).click()

        # This allows to try the code that we have in the
        # list and if it doesn't work try the next one
        # This list comes from the categories to chose on the
        # site in order to create a prior-authorization
        codes_to_try = [
            code_prestazioni, "Visite specialistiche", "Altre prestazioni"
        ]

        for current_code in codes_to_try:
            for item in self.webdriver.find_elements(
                    by=By.CLASS_NAME, value="k-item"
            ):
                if item.text == current_code:
                    break

            # The 'else' part of a for loop is executed
            # when the loop completed normally
            # (i.e., did not encounter a break statement)
            # In this case, we immediately continue with the
            # next iteration of the outer loop,
            # skipping the rest of this iteration
            else:
                continue

            # If we've reached this point,
            # it means the inner loop broke (i.e., we found the item)
            # So we break the outer loop as well
            break
        # The 'else' part of the outer for loop is executed
        # only if the loop completed normally
        # (i.e., we did not find the item with any of the codes)
        else:
            # If we've reached this point,
            # we did not find the item with any of the codes,
            # so we raise an error
            raise ValueError(
                f"Unable to find prestazioni with any of the provided codes"
            )

        # If we've reached this point,
        # we've found the item and
        # broken both loops
        # So we click the item
        item.click()

        # Check that the prestazioni
        # is possible for this patient
        # TODO: We are assuming for now that the prestazioni
        #  is possible, we need to find PNR for which it is not.
        self.webdriver.click_js(
            self.webdriver.find_element(by=By.ID, value="btnVerifica")
        )

        # Retrieve the message
        # TODO: We still have not got any PNR for
        #  which the insurance refuses the patient.
        #  So we cannot assess that it
        #  catches error messages
        time.sleep(1)
        message = self.webdriver.find_elements(
            by=By.XPATH, value="//h4/following-sibling::p"
        )[0].text
        logger.debug(f"Insurance response : {message}")

        # Input the patient ESAME into
        self.webdriver.find_element(
            by=By.ID, value="NoteAuth"
        ).send_keys(patient_esame)

        # Submit search. The button is behind an alert,
        # a javascript click can overcome this.
        self.webdriver.click_js(
            self.webdriver.find_element(
                by=By.ID, value="cercaQuadro"
            )
        )

        # Submit patient to the database
        self.webdriver.click_js(
            self.webdriver.find_element(
                by=By.ID, value="btnIstruisci"
            )
        )

    @staticmethod
    def _retrieve_pic_from_identify_page(self, html_body: str) -> str:
        """
        Retrieve the PIC from the html body of the page
        Args:
            self:
            html_body: html body of the page

        Returns:
            PIC of the patient
        """
        patient_pic = html_body.split(
            "ListaAutorizzazioni?filter="
        )[-1].split('"')[0]
        logger.info(
            f'Successfully retrieved PIC : "{patient_pic}"'
        )
        return patient_pic

    def fetch_webpage(self, url: str) -> str:
        try:
            page = self.webdriver.get(url).text
            return page
        except Exception as e:
            logger.error(f"Failed to fetch webpage from {url}: {e}")
            # Handle exception or propagate it further

    @staticmethod
    def parse_patient_data(self, patient_page: str, pnr_status_manager) -> dict:
        try:
            patient_data = {"pnr_status": pnr_status_manager.get_pnr_status(patient_page)}
            return patient_data
        except Exception as e:
            logger.error(f"Failed to parse patient data: {e}")
            # Handle exception or propagate it further

    def fetch_and_parse_pic(self, patient_pnr: str) -> str:
        url_database = (
            r"https://app.investire-in-italy.it/"
            r"CentriDiagnostici/ControlloQuadri/"
            r"GridAutorizzazioni_Read"
        )
        payload = {
            "page": 1,
            "pageSize": 50,
            "FieldFilter": patient_pnr,
        }

        try:
            result = self.webdriver.post(url_database, payload=payload)
            patient_pic = result.json()["Data"][0]["NumeroAuth"]
            return patient_pic
        except Exception as e:
            logger.error(f"Failed to fetch pic from database: {e}")
            # Handle exception or propagate it further