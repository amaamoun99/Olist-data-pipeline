import json
import logging
import requests
from google.cloud import bigquery
from pathlib import Path
from great_expectations.validator.validator import Validator
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine

SLACK_WEBHOOK_URL = (
    ""
)


def send_slack_alert(message: str):
    payload = {"text": message, "mrkdwn": True}
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    if response.status_code != 200:
        raise Exception(f"Slack webhook failed: {response.text}")


def run_ge_check(query_file: str, expectation_file: str):
    client = bigquery.Client()

    base_dir = Path(__file__).parent
    query_path = base_dir / query_file
    expectation_path = base_dir / expectation_file

    with open(query_path, "r") as f:
        query = f.read()
    df = client.query(query).result().to_dataframe()

    with open(expectation_path, "r") as f:
        expectations_config = json.load(f)

    execution_engine = PandasExecutionEngine()
    batch = Batch(data=df)
    validator = Validator(execution_engine=execution_engine, batches=[batch])

    suite = ExpectationSuite(
        expectation_suite_name=expectations_config["expectation_suite_name"],
        expectations=expectations_config["expectations"],
    )
    results = validator.validate(expectation_suite=suite)

    if not results["success"]:
        failed_details = []
        for result in results["results"]:
            if not result["success"]:
                exp_type = result["expectation_config"]["expectation_type"]
                kwargs = result["expectation_config"].get("kwargs", {})
                column = kwargs.get("column", "N/A")

                failure = f"""*❌ Expectation Failed:*• *Type:* `{exp_type}`• *Column:* `{column}`"""
                logging.error(failure)
                failed_details.append(failure)

        slack_message = (
            f"*❗Data Validation Failed:* `{expectation_file}`\n"
            + "\n".join(failed_details)
        )
        send_slack_alert(slack_message)
    else:
        logging.info(f"✅ All expectations passed for {expectation_file}")