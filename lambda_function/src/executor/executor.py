"""
This Module contains the Exector class that determines
which function to execute based on the event rule.
"""
import os
import json
import re

from typing import Any, Dict
from swxsoc.logger import log

import pandas as pd
import boto3
from gcn_kafka import Producer

from datetime import datetime, timezone, timedelta


def handle_event(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles the event passed to the Lambda function to initialize the FileProcessor.

    :param event: Event data passed from the Lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Success) or 500 (Error) HTTP response
    :rtype: dict
    """
    log.info("Received event", extra={"event": event, "context": context})

    try:
        # Validate event structure
        if not event.get("resources"):
            raise ValueError("Event is missing 'resources' key.")

        # Extract the rule ARN
        rule_arn = event["resources"][0]
        log.debug("Extracted rule ARN", extra={"rule_arn": rule_arn})

        # Use regex to extract the rule name
        rule_name_match = re.search(r"rule/(.+)", rule_arn)
        if not rule_name_match:
            raise ValueError("Invalid rule ARN format. Could not extract rule name.")
        function_name = rule_name_match.group(1)
        log.info(f"Rule Name Extracted: {function_name}")

        # Execute the corresponding function
        executor = Executor(function_name)
        executor.execute()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Execution completed successfully."}),
        }

    except Exception as e:
        log.error("Error handling event", exc_info=True, extra={"event": event})
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }


class Executor:
    """
    Executes the appropriate function based on the event rule.

    :param function_name: The name of the function to execute based on the event
    :type function_name: str
    """

    def __init__(self, function_name: str) -> None:
        self.function_name = function_name
        self.function_mapping = {
            "get_GOESXRS_alert_stream": self.goes_xrs_alert_stream,
        }
        try:
            # Initialize Grafana API Key
            session = boto3.session.Session()
            client = session.client(service_name="secretsmanager", region="us-east-1")

            response = client.get_secret_value(SecretId="gdc_test_kafka")
            secret = json.loads(response["SecretString"])
            for value in ["GDC_TEST_CLIEND_ID", "GDC_TEST_CLIENT_SECRET"]:
                os.environ[value.upper()] = secret[value]
                log.info(f"{value} secret loaded successfully")
        except Exception as e:
            log.error("Error reading secrets", exc_info=True)

    def execute(self) -> None:
        """
        Executes the mapped function.
        """
        if self.function_name not in self.function_mapping:
            raise ValueError(f"Function '{self.function_name}' is not recognized.")
        log.info(f"Executing function: {self.function_name}")
        self.function_mapping[self.function_name]()

    @staticmethod
    def goes_xrs_alert_stream():
        """
        Get latest GOES XRS data and generate kafka messages.
        """
        DOMAIN = "test.gcn.nasa.gov"
        CLIENT_ID = os.getenv("GDC_TEST_CLIEND_ID")
        CLIEND_SECRET = os.getenv("GDC_TEST_CLIENT_SECRET")
        producer = Producer(
            client_id=CLIENT_ID, client_secret=CLIEND_SECRET, domain=DOMAIN
        )

        def produce_alert_msg(
            topic, description: str, alert_type: str, alert_datetime: datetime
        ):
            data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Alert",
                "description": description,
                "alert_datetime": alert_datetime.isoformat(),
                "alert_tense": "curent",
                "alert_type": alert_type,
            }
            # JSON data converted to byte string format
            data_json = json.dumps(data).encode()
            producer.produce(topic, data_json)
            producer.flush()

        def produce_flux_msg(latest_time: datetime, latest_flux: float, kind: str):
            if kind not in ["long", "short"]:
                raise ValueError("kind must be 'long' or 'short'")
            data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "alert_datetime": latest_time.isoformat(),
                "flux": latest_flux
            }
            data_json = json.dumps(data).encode()
            producer.produce(f"gcn.notices.swxsoc.goes_xrs_flux{kind}", data_json)
            producer.flush()

        SEVERITIES = {
            "X10": 1e-3,
            "X5": 5e-4,
            "X1": 1e-4,
            "M5": 5e-5,
            "M1": 1e-5,
            "C5": 5e-6,
        }

        log.info("Getting GOES XRS data from NOAA")
        try:
            goes_json_data = pd.read_json(
                "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json"
            )
            # Convert the 'time_tag' column to datetime and set it as the index
            goes_json_data["time_tag"] = pd.to_datetime(goes_json_data["time_tag"])
            goes_json_data.set_index("time_tag", inplace=True)
            goes_xrs_long = goes_json_data[goes_json_data["energy"] == "0.1-0.8nm"]
            goes_xrs_short = goes_json_data[goes_json_data["energy"] == "0.05-0.4nm"]

            # Filter the data to include only rows where the time_tag is within the last 5 minutes
            utc_now = datetime.now(timezone.utc)
            five_minutes_ago = utc_now - timedelta(minutes=5)
            recent_data = goes_xrs_long[goes_xrs_long.index >= five_minutes_ago]
            old_data = goes_xrs_long[goes_xrs_long.index < five_minutes_ago]
            if len(recent_data) > 0:
                latest_flux = recent_data["flux"].values[-1]
                latest_time = recent_data.index[-1]
                log.info(f"Latest ({latest_time}) GOES XRS flux value  : {latest_flux}")
                average_new_flux = recent_data["flux"].mean()
                old_flux = old_data["flux"].values[-1]

                produce_flux_msg(latest_time, latest_flux, "long")
                produce_flux_msg(latest_time, goes_xrs_short['flux'].values[-1], "short")

                # check if the flux exceeded any severity threshold
                if any(average_new_flux >= value for value in SEVERITIES.values()) and average_new_flux > old_flux:
                    for severity, threshold in SEVERITIES.items():
                        if average_new_flux >= threshold and old_flux < threshold:
                            log.info(f"New flux exceeds {severity} threshold")
                            produce_alert_msg(
                                f"gcn.notices.swxsoc.goes_xrs_{severity.lower()}flare_alert",
                                f"GOES XRS flux exceeded {severity} threshold",
                                f"{severity} Flare Alert",
                                utc_now,
                            )
                # check if the flux is now decreasing
                if old_flux > average_new_flux:
                    for severity, threshold in SEVERITIES.items():
                        if old_flux >= threshold and average_new_flux < threshold:
                            log.info(f"Flux has decreased below {severity} threshold, {severity}alert is over")
                            produce_alert_msg(
                                f"gcn.notices.swxsoc.goes_xrs_{severity.lower()}flare_alert",
                                f"GOES XRS flux has decreased below {severity} threshold",
                                f"{severity} Flare Alert End",
                                utc_now,
                            )
            else:
                log.info("No new GOES XRS data in the last 5 minutes")

        except Exception as e:
            log.error("Error importing GOES data to Timestream", exc_info=True)
            raise
