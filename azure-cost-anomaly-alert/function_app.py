from pathlib import Path
import os
import csv
import json
import logging
import io

from datetime import datetime
from datetime import timedelta

from collections import defaultdict
from typing import Dict

import pymsteams

import azure.functions as func

from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

app = func.FunctionApp()

TAG_TO_FILTER: str = "OBI_components"
RANGE_TO_COMPARE_IN_DAYS: int = 7
PERCENT_CHANGE_NOTIFICATION_CUTOFF: int = 20


@app.function_name(name="eventGridTrigger")
@app.event_grid_trigger(arg_name="event")
def event_triggered(event: func.EventGridEvent):
    result = json.dumps(
        {
            "id": event.id,
            "data": event.get_json(),
            "topic": event.topic,
            "subject": event.subject,
            "event_type": event.event_type,
        }
    )
    logging.info(f"Event processed: {result}")
    if event.subject.endswith(".csv"):
        event_data = event.get_json()
        # url split by / fields: protocol, empty, storage_account, container_name, path
        # by convention the first folder will be the subscription name, with underscores instead of spaces
        storage_account = event_data["url"].split("/")[2]
        blob_container = event_data["url"].split("/")[3]
        blob_name = "/".join(event_data["url"].split("/")[4:])
        subscription_name = event_data["url"].split("/")[4].replace("_", " ").title()

        logging.info(f"Storage account: {storage_account}")
        logging.info(f"Blob container: {blob_container}")
        logging.info(f"Blob name: {blob_name}")

        combined_blob_data = io.BytesIO()
        if datetime.now().day <= 7:
            logging.info("Too close to start of month; reading previous blobs")
            previous_blob_paths = get_previous_blob_paths(
                storage_account, blob_container, blob_name
            )
            logging.info(f"Previous blob names: {previous_blob_paths}")
            for previous_blob_name in previous_blob_paths:
                logging.info(f"Reading previous blob {previous_blob_name}")
                blob_data = read_blob(
                    storage_account,
                    blob_container,
                    previous_blob_name,
                    event_data["contentLength"],
                )

                combined_blob_data.write(blob_data.read())

        logging.info(f"Reading current month blob: {blob_name}")
        blob_data = read_blob(
            storage_account, blob_container, blob_name, event_data["contentLength"]
        )
        combined_blob_data.write(blob_data.read())
        combined_blob_data.seek(0)
        calculate_diff(subscription_name=subscription_name, data=combined_blob_data)


def blob_authenticate(account_url: str) -> BlobServiceClient:
    client_id = os.environ["MANAGED_IDENTITY_CLIENT_ID"]
    managed_identity_credential = ManagedIdentityCredential(client_id=client_id)
    blob_service_client = BlobServiceClient(
        account_url, credential=managed_identity_credential
    )

    return blob_service_client


def get_container_client(storage_account: str, blob_container: str) -> ContainerClient:
    blob_service_client = blob_authenticate(account_url=f"https://{storage_account}")
    container_client = blob_service_client.get_container_client(
        container=blob_container
    )

    return container_client


def read_blob(
    storage_account: str, blob_container: str, blob_name: str, content_length: float
) -> io.BytesIO:
    """
    Read a blob into in io.BytesIO object and return with the cursor at position 0
    """
    container_client = get_container_client(storage_account, blob_container)
    stream = io.BytesIO()
    bytes_downloaded = 0

    while bytes_downloaded < content_length:
        bytes_downloaded += container_client.download_blob(blob=blob_name).readinto(
            stream
        )

    stream.seek(0)
    return stream


def get_previous_blob_paths(
    storage_account: str, blob_container: str, current_blob_name: str
) -> list[str]:
    """
    Return a list of blob paths for the previous month. It should be only one, but you never know.
    """
    current_blob_path = Path(current_blob_name)
    time_range_dir = current_blob_path.parent.parent.name
    start, _ = time_range_dir.split("-")
    start_date = datetime.strptime(start, "%Y%m%d")
    previous_end_date = start_date - timedelta(days=1)
    previous_start_date = previous_end_date - timedelta(days=previous_end_date.day - 1)
    previous_time_range_dir = f"{previous_start_date.strftime('%Y%m%d')}-{previous_end_date.strftime('%Y%m%d')}"
    previous_blob_prefix_path = (
        current_blob_path.parent.parent.parent / previous_time_range_dir
    )
    previous_blobs = []
    container_client = get_container_client(storage_account, blob_container)
    for blob_name in container_client.list_blob_names(
        name_starts_with=previous_blob_prefix_path.as_posix()
    ):
        if blob_name.endswith(".csv"):
            previous_blobs.append(blob_name)

    return sorted(previous_blobs)


def extract_data_csv(
    data: io.BytesIO,
    range_start: datetime,
    range_stop: datetime,
    today: datetime,
) -> Dict:
    cost_changes = {}
    range_total_component_cost = defaultdict(float)
    range_average_component_cost = defaultdict(float)
    yesterday_total_component_cost = defaultdict(float)

    wrapper = io.TextIOWrapper(data)
    reader = csv.DictReader(wrapper, delimiter=",")
    for row in reader:
        row_date = datetime.strptime(row["date"], "%m/%d/%Y")
        row_component_tag = json.loads(row["tags"] or "{}").get(
            TAG_TO_FILTER, "UNTAGGED"
        )
        if range_start <= row_date < range_stop:
            range_total_component_cost[row_component_tag] += float(
                row["costInBillingCurrency"]
            )
        elif range_stop <= row_date < today:
            yesterday_total_component_cost[row_component_tag] += float(
                row["costInBillingCurrency"]
            )

    for tag in range_total_component_cost:
        range_average_component_cost[tag] = (
            range_total_component_cost[tag] / RANGE_TO_COMPARE_IN_DAYS
        )

    for tag in yesterday_total_component_cost:
        logging.info(f"Yesterday cost for {tag}: {yesterday_total_component_cost[tag]}")
        logging.info(f"Average cost for {tag}: {range_average_component_cost[tag]}")
        if range_average_component_cost[tag] != 0:
            percent_change = (
                (
                    yesterday_total_component_cost[tag]
                    - range_average_component_cost[tag]
                )
                / abs(range_average_component_cost[tag])
            ) * 100
            line = f"OBI_component {tag} changed {percent_change:.2f}% ({range_average_component_cost[tag]:.2f} -> {yesterday_total_component_cost[tag]:.2f}) yesterday compared to 7 days before"
            logging.info(line)
            if percent_change >= PERCENT_CHANGE_NOTIFICATION_CUTOFF:
                cost_changes[tag] = {
                    "percent_change": percent_change,
                    "range_averages": range_average_component_cost[tag],
                    "yesterday_cost": yesterday_total_component_cost[tag],
                }

    return cost_changes


def calculate_diff(
    subscription_name: str,
    data: io.BytesIO,
) -> None:
    logging.info("Processing blob")

    range_start = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=RANGE_TO_COMPARE_IN_DAYS + 1)
    range_stop = datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    cost_changes = extract_data_csv(
        data=data,
        range_start=range_start,
        range_stop=range_stop,
        today=today,
    )

    send_formatted_teams_message(subscription_name, cost_changes)


def send_formatted_teams_message(
    subscription_name, cost_changes: Dict[str, Dict[str, float]]
) -> None:
    if not cost_changes:
        logging.info("No cost anomalies; not sending a message")
        return
    webhook_url = os.environ["TEAMS_WEBHOOK_URL"]
    card = pymsteams.connectorcard(webhook_url)
    card.title(mtitle=f"Cost anomalies in subscription {subscription_name}")
    card.summary(msummary="Cost anomalies")
    section = pymsteams.cardsection()
    section_text = """<table>
    <tr>
    <th> Component </th><th> % Change </th><th> 7-day Average </th><th> Yesterday's Value </th>
    </tr>
    """
    for cost_change_tag, change_data in cost_changes.items():
        change_data["percent_change"]
        if change_data["percent_change"] > 0:
            format_string = "<span style='color:red'>"
        else:
            format_string = "<span style='color:green'>"
        section_text += f"""<tr><td> {format_string}{cost_change_tag}</span> </td>
                                <td> {"+" if change_data["percent_change"] > 0 else ""}{change_data["percent_change"]:.2f}% </td>
                                <td> {change_data["range_averages"]:.2f} </td>
                                <td> {change_data["yesterday_cost"]:.2f}</td>
                            </tr>"""
    section_text += "</table>"
    section.text(section_text)
    logging.info(f"Section text: {section_text}")
    card.addSection(section)
    card.send()
