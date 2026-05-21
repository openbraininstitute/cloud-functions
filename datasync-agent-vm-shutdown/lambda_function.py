import os
import json
import logging


from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.core.exceptions import ResourceExistsError

TASK_TO_MACHINE_MAP = {
    os.environ["OPENDATA_TASK_ARN"]: "aws-us-west-2-datasync-agent",
    os.environ["INTERNAL_PUBLIC_DATA_TASK_ARN"]: "aws-us-east-1-datasync-agent",
}

logger = logging.getLogger(__name__)
sh = logging.StreamHandler()
fmt = logging.Formatter("[%(asctime)s][%(levelname)s] %(msg)s")
sh.setFormatter(fmt)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    finished_execution_arns = event.get("resources")
    logger.debug(f"Finished execution arns: {finished_execution_arns}")
    result = {"statusCode": 200, "body": {}}
    stop_results = []

    for finished_execution_arn in finished_execution_arns:
        logger.info(f"Finished execution arn: {finished_execution_arn}")
        finished_task_arn = "/".join(finished_execution_arn.split("/")[:2])
        stop_result = stop_machine_for_task(finished_task_arn)
        logger.debug(f"Got stop result: {stop_result}")
        stop_results.append(stop_result)

    for stop_result in stop_results:
        logger.debug(f"Checking whether there's a stop task in {stop_result}")
        if stop_task := stop_result.get("stop_task"):
            logger.debug("Stop task found; waiting for it")
            stop_task.wait()
            if stop_task.status() == "Succeeded":
                result["body"][stop_result["task_arn"]] = (
                    f"Azure Agent VM {stop_result['VM']} stopped successfully"
                )
            else:
                result["body"][stop_result["task_arn"]] = (
                    f"Azure Agent VM {stop_result['VM']} failed to stop with status {stop_task.status()}"
                )
                result["statusCode"] = 500
        else:
            result["body"][stop_result["task_arn"]] = (
                f"No Azure Agent VM associated with task {stop_result['task_arn']} or VM was deallocated."
            )

    result["body"] = json.dumps(result["body"])
    logger.info(f"Return value: {result}")
    return result


def stop_machine_for_task(finished_task_arn):
    logger.info(f"Stopping machine for finished task {finished_task_arn}")
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]

    if finished_task_arn not in TASK_TO_MACHINE_MAP:
        logger.info(f"No machine associated with task {finished_task_arn}")
        return {"task_arn": finished_task_arn}

    vm_name = TASK_TO_MACHINE_MAP[finished_task_arn]

    tenant_id = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")

    if tenant_id and client_id and client_secret:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    else:
        logger.critical(
            "At least one of AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET is not set; falling back to DefaultAzureCredential"
        )
        credential = DefaultAzureCredential()
    compute_client = ComputeManagementClient(credential, subscription_id)

    try:
        logger.debug("Begin poweroff")
        async_vm_stop = compute_client.virtual_machines.begin_power_off(
            resource_group_name, vm_name, skip_shutdown=False
        )
    except ResourceExistsError:
        logger.info(
            f"VM {vm_name} was deallocated, probably after being turned off too long, can't power down."
        )
        return {"VM": vm_name, "task_arn": finished_task_arn}

    return {"VM": vm_name, "task_arn": finished_task_arn, "stop_task": async_vm_stop}
