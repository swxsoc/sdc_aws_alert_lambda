"""
This module dispatches alert functions based on the EventBridge rule name.
"""
import io
import json
import os
import re
import time
import urllib.request
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
    def _secret_mapping() -> Dict[str, str]:
        return {
            "GCN_CLIENT_ID_SECRET_ARN": "GCN_CLIENT_ID",
            "GCN_CLIENT_SECRET_SECRET_ARN": "GCN_CLIENT_SECRET",
        }

    @staticmethod
    def _parse_secret_string(secret_string: str) -> Dict[str, str]:
        """Parse a Secrets Manager string as JSON first, then dotenv-style text."""
        try:
            secret = json.loads(secret_string)
        except json.JSONDecodeError:
            secret = {}
            for line in secret_string.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                secret[key.strip()] = value.strip().strip("\"'")

        if not isinstance(secret, dict):
            raise ValueError("SecretString must be a JSON object or dotenv-style text")

        return {
            str(key): str(value)
            for key, value in secret.items()
            if value is not None
        }

    @staticmethod
    def _get_secret_value(secret: Dict[str, str], env_name: str) -> str:
        key_options = (
            env_name,
            env_name.lower(),
            env_name.removeprefix("GCN_").lower(),
        )
        for key in key_options:
            value = secret.get(key)
            if value:
                return value
        raise KeyError(
            f"Secret is missing one of the expected keys: {', '.join(key_options)}"
        )

    @staticmethod
    def _load_secrets() -> None:
        """Load GCN credentials from Secrets Manager into the environment."""
        secret_mapping = AlertDispatcher._secret_mapping()
        secrets_to_load = {
            secret_arn_env: credential_env
            for secret_arn_env, credential_env in secret_mapping.items()
            if not os.getenv(credential_env)
        }
        if not secrets_to_load:
            return

        missing_secret_env_vars = [
            env_var for env_var in secrets_to_load if not os.getenv(env_var)
        ]
        if missing_secret_env_vars:
            raise RuntimeError(
                "Missing GCN credentials. Set GCN_CLIENT_ID and GCN_CLIENT_SECRET "
                "directly, or configure Secrets Manager ARNs in: "
                f"{', '.join(missing_secret_env_vars)}"
            )

        try:
            import boto3

            session = boto3.session.Session()
            client = session.client(service_name="secretsmanager")
            for env_var, credential_env in secrets_to_load.items():
                secret_arn = os.getenv(env_var)
                if not secret_arn:
                    continue
                response = client.get_secret_value(SecretId=secret_arn)
                secret_string = response.get("SecretString")
                if not secret_string:
                    raise ValueError("Secret must contain SecretString")
                secret = AlertDispatcher._parse_secret_string(secret_string)
                os.environ[credential_env] = AlertDispatcher._get_secret_value(
                    secret, credential_env
                )
                log.info("Loaded secret", extra={"credential_env": credential_env})
        except Exception as exc:
            log.error(
                "Error reading secrets",
                exc_info=True,
                extra={"required_env": sorted(secrets_to_load.values())},
            )
            raise RuntimeError(
                f"Error reading GCN credentials from Secrets Manager: {exc}"
            ) from exc

    def execute(self) -> None:
        """
        Executes the mapped function.
        """
        if self.function_name not in self.function_mapping:
            raise ValueError(f"Function '{self.function_name}' is not recognized.")
        log.info(f"Executing function: {self.function_name}")
        self.function_mapping[self.function_name]()

    @staticmethod
    def _read_goes_xrs_data():
        """Read GOES XRS JSON data with a bounded HTTP timeout."""
        import pandas as pd

        noaa_url = os.getenv(
            "GOES_XRS_URL",
            "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json",
        )
        timeout_seconds = float(os.getenv("GOES_XRS_HTTP_TIMEOUT_SECONDS", "10"))
        start_time = time.monotonic()
        log.info(
            "Fetching GOES XRS data from NOAA",
            extra={"url": noaa_url, "timeout_seconds": timeout_seconds},
        )
        with urllib.request.urlopen(noaa_url, timeout=timeout_seconds) as response:
            payload = response.read()
        log.info(
            "Fetched GOES XRS data from NOAA",
            extra={"elapsed_seconds": time.monotonic() - start_time},
        )
        return pd.read_json(io.BytesIO(payload))

    @staticmethod
    def goes_xrs_alert_stream():
        """
        Get latest GOES XRS data and generate kafka messages.
        """
        import pandas as pd

        domain = os.getenv("GCN_DOMAIN", "test.gcn.nasa.gov")
        client_id = os.getenv("GCN_CLIENT_ID")
        client_secret = os.getenv("GCN_CLIENT_SECRET")
        producer_flush_timeout_seconds = float(
            os.getenv("GCN_PRODUCER_FLUSH_TIMEOUT_SECONDS", "10")
        )
        producer_delivery_timeout_seconds = int(
            os.getenv("GCN_PRODUCER_DELIVERY_TIMEOUT_SECONDS", "30")
        )
        if not client_id or not client_secret:
            raise ValueError("GCN credentials are not loaded")
        producer = None

        def get_producer():
            nonlocal producer
            if producer is not None:
                return producer
            log.info(
                "Creating GCN Kafka producer",
                extra={
                    "domain": domain,
                    "delivery_timeout_seconds": producer_delivery_timeout_seconds,
                },
            )
            start_time = time.monotonic()
            from gcn_kafka import Producer

            producer = Producer(
                client_id=client_id,
                client_secret=client_secret,
                domain=domain,
                **{"delivery.timeout.ms": producer_delivery_timeout_seconds * 1000},
            )
            log.info(
                "Created GCN Kafka producer",
                extra={"elapsed_seconds": time.monotonic() - start_time},
            )
            return producer

        def flush_producer() -> None:
            active_producer = get_producer()
            log.info(
                "Flushing GCN Kafka producer",
                extra={"timeout_seconds": producer_flush_timeout_seconds},
            )
            start_time = time.monotonic()
            remaining_messages = active_producer.flush(producer_flush_timeout_seconds)
            log.info(
                "Finished flushing GCN Kafka producer",
                extra={
                    "elapsed_seconds": time.monotonic() - start_time,
                    "remaining_messages": remaining_messages,
                },
            )
            if remaining_messages:
                raise TimeoutError(
                    "Timed out delivering messages to GCN Kafka. "
                    f"{remaining_messages} message(s) remained queued after "
                    f"{producer_flush_timeout_seconds} seconds."
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
            get_producer().produce(topic, data_json)
            flush_producer()

        SEVERITIES = {
            "X10": 1e-3,
            "X5": 5e-4,
            "X1": 1e-4,
            "M5": 5e-5,
            "M1": 1e-5,
            "C5": 5e-6,
        }
        recent_window_minutes = int(os.getenv("GOES_XRS_RECENT_WINDOW_MINUTES", "5"))
        feed_stale_minutes = int(os.getenv("GOES_XRS_FEED_STALE_MINUTES", "15"))

        log.info("Getting GOES XRS data from NOAA")
        try:
            goes_json_data = AlertDispatcher._read_goes_xrs_data()
            # Convert the 'time_tag' column to datetime and set it as the index
            goes_json_data["time_tag"] = pd.to_datetime(goes_json_data["time_tag"])
            goes_json_data.set_index("time_tag", inplace=True)
            goes_xrs_long = goes_json_data[
                goes_json_data["energy"] == "0.1-0.8nm"
            ].sort_index()
            if goes_xrs_long.empty:
                raise ValueError("No GOES XRS long-channel data available")

            utc_now = datetime.now(timezone.utc)
            latest_time = goes_xrs_long.index[-1]
            feed_age = utc_now - latest_time.to_pydatetime()
            if feed_age > timedelta(minutes=feed_stale_minutes):
                log.warning(
                    "GOES XRS feed is stale",
                    extra={
                        "latest_time": latest_time.isoformat(),
                        "feed_age_seconds": feed_age.total_seconds(),
                    },
                )
                return

            recent_window_start = latest_time - timedelta(minutes=recent_window_minutes)
            recent_data = goes_xrs_long[goes_xrs_long.index >= recent_window_start]
            old_data = goes_xrs_long[goes_xrs_long.index < recent_window_start]
            if recent_data.empty:
                raise ValueError("No recent GOES XRS data available")
            if old_data.empty:
                raise ValueError("No historical GOES XRS baseline available")
            average_new_flux = recent_data["flux"].mean()
            latest_flux = float(recent_data["flux"].iloc[-1])
            old_flux = float(old_data["flux"].iloc[-1])

            # send the latest flux value to kafka topic
            data = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "alert_datetime": latest_time.isoformat(),
                "flux": latest_flux,
            }
            data_json = json.dumps(data).encode()
            get_producer().produce("gcn.notices.swxsoc.goes_xrs_flux", data_json)
            flush_producer()

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
