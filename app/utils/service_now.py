import requests
from app.utils.conductor_logger import log_message
from app.service.mongo_service import save_report_data, client, get_service_now_credentials
import base64

def send_to_service_now_rerun_worker(task):
    task_id    = task.task_id
    input_data = task.input_data
    log_message(task_id, "Rerun: sending data to ServiceNow")

    # unpack
    case_id        = input_data.get("case_id")
    agent_output   = input_data.get("agent_output", {})           # dict keyed by agent name
    # submission_data = input_data.get("submission_data", {})       # Whether madhavi needs the updated merged one or she does not needs this at all maybe. 

    db = client["Submission_Intake"]
    collection = db["BP_DATA"]
    mongo_doc     = collection.find_one({"case_id": case_id}) or {}
    print(mongo_doc)
    submission_data = mongo_doc.get("submission_data", {})

    payload = {
        "case_id":      case_id,
        "insights": agent_output,
        "update": True
    }

    ##demo 1
    # resp = requests.post(
    #     "https://elevatenowtechdemo1.service-now.com/api/x_elete_ins/load_package/commons",
    #     headers={"Content-Type": "application/json"},
    #     json=payload
    # )

    #demo 2
    resp = requests.post(
        "https://elevatenowtechdemo2.service-now.com/api/x_elete_clear_36_0/load_package/commons",
        headers={"Content-Type": "application/json"},
        json=payload
    )
    

    log_message(task_id, f"ServiceNow status: {resp.status_code}")
    log_message(task_id, f"ServiceNow body:   {resp.json()}")
    log_message(task_id, f"ServiceNow body:   {payload}")

    return {
        "status": "COMPLETED",
        "outputData": {
            "status_code":     resp.status_code,
            "response":        resp.json(),
        }
    }


def send_to_service_now(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,f"Sending the data to service now")

    headers = {"Content-Type": "application/json"}

    ##demo 1
    # url = "https://elevatenowtechdemo1.service-now.com/api/x_elete_ins/load_package/commons"

    ##demo 2
    url ="https://elevatenowtechdemo2.service-now.com/api/x_elete_clear_36_0/load_package/commons"
    

    try:
        # Extract actual input parameters
        case_id = input_data.get("case_id")
        tx_id = input_data.get("tx_id")
        agent_output = input_data.get("agent_output", {})
        submission_data = input_data.get("submission_data", {})

        # Extract insights from agent_output

        data = {
            "case_id": case_id,
            "tx_id": tx_id,
            "parsed_data": submission_data,
            "insights": agent_output,
            "update": False
        }

        log_message(task_id,f"Data to be sent: {data}")


        response = requests.post(url, headers=headers, json=data)
        log_message(task_id,f"Response status: {response.status_code}")
        log_message(task_id,f"Response body: {response.json()}")
        return {
            "status": "COMPLETED",
            "outputData": {
                "status_code":     response.status_code,
                "response":        response.json(),
            }
        }

    except Exception as e:
        log_message(task_id,f"Error sending data to ServiceNow: {e}")
        raise



#ven instance


def send_to_service_now_rerun_worker_ven(task):
    task_id    = task.task_id
    input_data = task.input_data
    log_message(task_id, "Rerun: sending data to ServiceNow")

    credentials = get_service_now_credentials()
    username = credentials["username"]
    password = credentials["password"]

    auth_string = f"{username}:{password}"
    auth_header = base64.b64encode(auth_string.encode('ascii')).decode('ascii')

    # unpack
    case_id        = input_data.get("case_id")
    agent_output   = input_data.get("agent_output", {})           # dict keyed by agent name
    # submission_data = input_data.get("submission_data", {})       # Whether madhavi needs the updated merged one or she does not needs this at all maybe. 

    db = client["Submission_Intake"]
    collection = db["BP_DATA"]
    mongo_doc     = collection.find_one({"case_id": case_id}) or {}
    print(mongo_doc)
    submission_data = mongo_doc.get("submission_data", {})

    payload = {
        "case_id":      case_id,
        "insights": agent_output,
        "update": True
    }

    log_message(task_id, f"Payload to be sent: {payload}")

    resp = requests.post(
        "https://cert362.service-now.com/api/x_elete_clear_36_0/load_package/commons",
        headers={"Content-Type": "application/json","Authorization": f"Basic {auth_header}"},
        json=payload
    )

    # resp = requests.post(
    #     "https://cert2054.service-now.com/api/x_elete_clear_36_0/load_package/commons",
    #     headers={"Content-Type": "application/json"},
    #     json=payload
    # )

    

    log_message(task_id, f"ServiceNow status: {resp.status_code}")
    log_message(task_id, f"ServiceNow body:   {resp.json()}")

    return {
        "status": "COMPLETED",
        "outputData": {
            "status_code":     resp.status_code,
            "response":        resp.json(),
        }
    }


def send_to_service_now_ven(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,f"Sending the data to service now")

    credentials = get_service_now_credentials()
    username = credentials["username"]
    password = credentials["password"]

    auth_string = f"{username}:{password}"
    auth_header = base64.b64encode(auth_string.encode('ascii')).decode('ascii')

    headers = {"Content-Type": "application/json","Authorization": f"Basic {auth_header}"}

    url = "https://cert362.service-now.com/api/x_elete_clear_36_0/load_package/commons"
    #url = "https://cert2054.service-now.com/api/x_elete_clear_36_0/load_package/commons"

    

    try:
        # Extract actual input parameters
        case_id = input_data.get("case_id")
        tx_id = input_data.get("tx_id")
        agent_output = input_data.get("agent_output", {})
        submission_data = input_data.get("submission_data", {})

        # Extract insights from agent_output

        data = {
            "case_id": case_id,
            "tx_id": tx_id,
            "parsed_data": submission_data,
            "insights": agent_output,
            "update": False
        }
        log_message(task_id,f"Data to be sent: {data}")


        response = requests.post(url, headers=headers, json=data)
        log_message(task_id,f"Response status: {response.status_code}")
        log_message(task_id,f"Response body: {response.json()}")
        return {
            "status": "COMPLETED",
            "outputData": {
                "status_code":     response.status_code,
                "response":        response.json(),
            }
        }

    except Exception as e:
        log_message(task_id,f"Error sending data to ServiceNow: {e}")
        raise
