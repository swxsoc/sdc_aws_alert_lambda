"""
This module dispatches alert functions based on the EventBridge rule name.
"""
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

try:
    from swxsoc import log
except ImportError:  # pragma: no cover - local fallback when SWxSOC is unavailable
    import logging

    log = logging.getLogger(__name__)


def handle_event(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle the Lambda event and dispatch the matching alert function.

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
        dispatcher = AlertDispatcher(function_name)
        dispatcher.execute()

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


class AlertDispatcher:
    """
    Executes the appropriate function based on the event rule.

    :param function_name: The name of the function to execute based on the event
    :type function_name: str
    """

    def __init__(self, function_name: str) -> None:
        self.function_name = function_name
        self.function_mapping = {
            "get_GOESXRS_alert_stream": self.goes_xrs_alert_stream,
            "get_goesxrs_alert_stream": self.goes_xrs_alert_stream,
        }
        self._load_secrets()

    @staticmethod
    def _load_secrets() -> None:
        """Load GCN credentials from Secrets Manager into the environment."""
        try:
            import boto3

            session = boto3.session.Session()
            client = session.client(service_name="secretsmanager")
            env_to_secret_keys = {
                "GCN_CLIENT_ID_SECRET_ARN": "gcn_client_id",
                "GCN_CLIENT_SECRET_SECRET_ARN": "gcn_client_secret",
            }
            for env_var, secret_key in env_to_secret_keys.items():
                if os.getenv(secret_key.upper()):
                    continue
                secret_arn = os.getenv(env_var)
                if not secret_arn:
                    continue
                response = client.get_secret_value(SecretId=secret_arn)
                secret = json.loads(response["SecretString"])
                os.environ[secret_key.upper()] = secret[secret_key]
                log.info("Loaded secret", extra={"secret_key": secret_key})
        except Exception:
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
        import pandas as pd
        from gcn_kafka import Producer

        domain = os.getenv("GCN_DOMAIN", "test.gcn.nasa.gov")
        client_id = os.getenv("GCN_CLIENT_ID")
        client_secret = os.getenv("GCN_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise ValueError("GCN credentials are not loaded")
        producer = Producer(
            client_id=client_id, client_secret=client_secret, domain=domain
        )

        def produce_alert(
            topic: str, description: str, alert_type: str, alert_datetime: datetime
        ) -> None:
            data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Alert",
                "description": description,
                "alert_datetime": alert_datetime.isoformat(),
                "alert_tense": "current",
                "alert_type": alert_type,
            }
            data_json = json.dumps(data).encode()
            producer.produce(topic, data_json)
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
            # Filter the data to include only rows where the time_tag is within the last 5 minutes
            utc_now = datetime.now(timezone.utc)
            five_minutes_ago = utc_now - timedelta(minutes=5)
            recent_data = goes_xrs_long[goes_xrs_long.index >= five_minutes_ago]
            old_data = goes_xrs_long[goes_xrs_long.index < five_minutes_ago]
            if recent_data.empty:
                raise ValueError("No recent GOES XRS data available")
            if old_data.empty:
                raise ValueError("No historical GOES XRS baseline available")
            average_new_flux = recent_data["flux"].mean()
            latest_flux = float(recent_data["flux"].iloc[-1])
            latest_time = recent_data.index[-1]
            old_flux = float(old_data["flux"].iloc[-1])

            # send the latest flux value to kafka topic
            data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "alert_datetime": latest_time.isoformat(),
                "flux": latest_flux,
            }
            data_json = json.dumps(data).encode()
            producer.produce("gcn.notices.swxsoc.goes_xrs_flux", data_json)
            producer.flush()

            # check if the flux exceeded any severity threshold
            if any(average_new_flux >= value for value in SEVERITIES.values()) and average_new_flux > old_flux:
                for severity, threshold in SEVERITIES.items():
                    if average_new_flux >= threshold and old_flux < threshold:
                        log.info(f"New flux exceeds {severity} threshold")
                        produce_alert(
                            f"gcn.notices.swxsoc.goes_xrs_{severity.lower()}flare_alert",
                            f"GOES XRS flux exceeded {severity} threshold",
                            f"{severity} Flare Alert",
                            utc_now,
                        )
            # check if the flux is now decreasing
            if old_flux > average_new_flux:
                for severity, threshold in SEVERITIES.items():
                    if old_flux >= threshold and average_new_flux < threshold:
                        log.info(
                            f"Flux has decreased below {severity} threshold, {severity} alert is over"
                        )
                        produce_alert(
                            f"gcn.notices.swxsoc.goes_xrs_{severity.lower()}flare_alert",
                            f"GOES XRS flux has decreased below {severity} threshold",
                            f"{severity} Flare Alert End",
                            utc_now,
                        )

        except Exception:
            log.error("Error generating GOES XRS alerts", exc_info=True)
            raise
