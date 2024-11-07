import logging
import os

from zeep import Client
from zeep.transports import Transport
from requests import Session
from src.abacus_cc_loader_from_centaur import (
    AbacusCCLoaderFromCentaur,
)


class CreditCardAbacusService:
    def __init__(self):
        self.centaur_url = os.getenv("CentaurServiceUrl")

        # Setup logger for event logging (simulating event log in Python)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename="creditcard_abacus_service.log",
        )
        self.logger = logging.getLogger("CreditCardAbacusService")
        self.logger.info("CreditCardAbacusService initialized.")

    def run(self):
        """Run the main tasks of the service when triggered."""
        self.logger.info("CreditCardAbacusService started by EKS CronJob.")
        self.transfer_paylink_file()
        self.do_load()

        self.logger.info("CreditCardAbacusService run completed.")

    def transfer_paylink_file(self):
        try:
            # Create a session and transport with a timeout of 2 minutes (120 seconds)
            session = Session()
            transport = Transport(session=session, timeout=120)  # Timeout of 2 minutes

            # Create a zeep client using the WSDL URL and custom transport
            client = Client(wsdl=self.centaur_url, transport=transport)

            # Call the transfer_paylink_file method from the SOAP service
            result = client.service.TransferPaylinkFile()
            if result:
                self.logger.info("Pay link file transferred!")
            else:
                self.logger.info("Pay link file not transferred.")
        except Exception as ex:
            self.logger.error(f"Error reply from Centaur service: {str(ex)}")

    def do_load(self):
        """Mimics the DoLoad method from C#."""
        try:
            if AbacusCCLoaderFromCentaur.is_load_fin():
                self.logger.info(
                    "Abacus Finished for max working day, Starting Delinquency..."
                )

                if AbacusCCLoaderFromCentaur.is_ok_to_call_deliquency():
                    self.logger.info(
                        "Abacus Finished for this day, Starting Delinquency..."
                    )

                    session = Session()

                    transport = Transport(
                        session=session, timeout=900
                    )  # Timeout of 15 minutes

                    client = Client(wsdl=self.centaur_url, transport=transport)

                    centaur_service = client.service

                    if centaur_service.ProcessDeliquency():
                        self.logger.info("Delinquency Finished, Starting Load...")
                        try:
                            load_status = AbacusCCLoaderFromCentaur.load(True)
                            if (
                                load_status
                                == AbacusCCLoaderFromCentaur.LoadStatus.WAITING_FOR_FIN
                            ):
                                self.logger.info("Waiting for fin! Trying again later.")
                            elif (
                                load_status
                                == AbacusCCLoaderFromCentaur.LoadStatus.SUCCESS
                            ):
                                self.logger.info(
                                    "Done loading CreditCard data into Abacus."
                                )
                            elif (
                                load_status
                                == AbacusCCLoaderFromCentaur.LoadStatus.ERROR
                            ):
                                self.logger.error(
                                    "Error loading CreditCard data into Abacus."
                                )
                            elif (
                                load_status
                                == AbacusCCLoaderFromCentaur.LoadStatus.FINISHED
                            ):
                                self.logger.info("Finished for this working day!")
                            else:
                                self.logger.error(f"Unexpected status: {load_status}")
                        except Exception as ex:
                            self.logger.error(f"Error during load: {str(ex)}")
                    else:
                        self.logger.info(
                            "Delinquency not finished! Trying again later."
                        )
                else:
                    self.logger.info(
                        "Waiting for Abacus for today’s load! Trying again later."
                    )
            else:
                self.logger.info("Waiting for Abacus! Trying again later.")
        except Exception as ex:
            self.logger.error(f"Error during load: {str(ex)}")
