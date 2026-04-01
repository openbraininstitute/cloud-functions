import os

import json

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient

TASK_TO_MACHINE_MAP = {
    os.environ["OPENDATA_TASK_ARN"]: "aws-us-west-2-datasync-agent",
    os.environ["INTERNAL_PUBLIC_DATA_TASK_ARN"]: "aws-us-east-1-datasync-agent",
}


def lambda_handler(event, context):
    finished_execution_arns = event.get("resources")
    result = {"statusCode": 200, "body": json.dumps({})}
    stop_results = []

    for finished_execution_arn in finished_execution_arns:
        finished_task_arn = "/".join(finished_execution_arn.split("/")[:2])
        stop_results.append(stop_machine_for_task(finished_task_arn))

    for stop_result in stop_results:
        if "stop_task" in stop_result:
            stop_result.wait()
            if stop_result.status() == "Succeeded":
                result["body"][stop_result["task_arn"]] = (
                    f"Azure Agent VM {stop_result['VM']} stopped successfully"
                )
            else:
                result["body"][stop_result["task_arn"]] = (
                    f"Azure Agent VM {stop_result['VM']} failed to stop with status {stop_result.status()}"
                )
                result["statusCode"] = 500
        else:
            result["body"][stop_result["task_arn"]] = (
                f"No Azure Agent VM associated with task {stop_result['task_arn']}"
            )

    return result


def stop_machine_for_task(finished_task_arn):
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]

    if finished_task_arn not in TASK_TO_MACHINE_MAP:
        return {"task": finished_task_arn}

    vm_name = TASK_TO_MACHINE_MAP[finished_task_arn]

    credential = DefaultAzureCredential()
    compute_client = ComputeManagementClient(credential, subscription_id)

    async_vm_stop = compute_client.virtual_machines.begin_power_off(
        resource_group_name, vm_name, skip_shutdown=False
    )
    return {"VM": vm_name, "task_arn": finished_task_arn, "stop_task": async_vm_stop}
