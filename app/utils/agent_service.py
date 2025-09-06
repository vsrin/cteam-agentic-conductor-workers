import copy
from datetime import time
import json
import random
import markdown
import requests
from app.utils.conductor_logger import log_message
from app.service.mongo_service import save_report_data, client

# Updated to use AgentName instead of AgentID
TARGET_AGENT_NAMES = {
    "LossInsights",
    "ExposureInsights", 
    "EligibilityCheck",
    "InsuranceVerify",
    "PropEval",
    "BusineesProfileSearch"  # Using your exact DB name with typo
}

# Updated AGENT_PROMPTS to match your exact AgentNames
AGENT_PROMPTS = {
    "LossInsights":       "Please provide loss insights for the data.",
    "ExposureInsights":   "Please provide exposure insights for the data.",
    "EligibilityCheck":  "Please check eligibility based on the data.",
    "InsuranceVerify": "Please verify insurance details in the data.",
    "PropEval":          "Please provide Property evaluation insights for the data.",
    "BusineesProfileSearch": "Please search the business profile based on the data.",  # Using your exact DB name
}

def deep_update(original: dict, updates: dict) -> dict:
    """
    Recursively walk `original`. Whenever you encounter a key that's in `updates`:
      - if original[k] is a dict with a 'value' key, replace original[k]['value'] and update 'score' if present
      - otherwise replace original[k] outright.
    Returns the mutated `original`.
    """
    
    # Create a case-insensitive lookup for updates
    updates_lower = {k.lower(): (k, v) for k, v in updates.items()}
    
    for k, v in list(original.items()):
        # Check if this key needs updating (case-insensitive)
        if k.lower() in updates_lower:
            original_key, new_val = updates_lower[k.lower()]
            if isinstance(original[k], dict) and 'value' in original[k]:
                original[k]['value'] = new_val
                # Update score if it exists, otherwise set a default score
                if 'score' in original[k]:
                    original[k]['score'] = "100"
            else:
                original[k] = new_val

        # If the value is itself a dict, recurse into it
        if isinstance(original[k], dict):
            deep_update(original[k], updates)  # Pass original updates, function will recreate the lookup

    return original

def craft_agent_config(agent_data):
    cfg = agent_data.get("Configuration", {})
    kb = agent_data.get("selectedKnowledgeBase")
    toggle = cfg.get("structured_output_toggle", False)
    raw = cfg.get("structured_output", "{}")

    if not toggle:
        structured = {}
    elif isinstance(raw, bool) and not raw:
        structured = None
    elif isinstance(raw, str):
        structured = json.loads(raw)
        structured = structured.get("structured_output", structured)
    else:
        structured = raw.get("structured_output", raw)

    agent_config = {
        "AgentID": agent_data.get("AgentID", ""),
        "AgentName": agent_data.get("AgentName", ""),
        "AgentDesc": agent_data.get("AgentDesc", ""),
        "CreatedOn": agent_data.get("CreatedOn", ""),
        "Configuration": {
            "name": cfg.get("name", ""),
            "function_description": cfg.get("function_description", ""),
            "system_message": cfg.get("system_message", ""),
            "tools": cfg.get("tools", []),
            "category": cfg.get("category", ""),
            "structured_output": structured,
            "knowledge_base": {
                "id": kb.get("id", ""),
                "name": kb.get("name", ""),
                "enabled": "yes",
                "collection_name": kb.get("collection_name", ""),
                "embedding_model": "BAAI/bge-small-en-v1.5",
                "description": kb.get("description", ""),
                "number_of_chunks": 5
            } if kb else {}
        },
        "isManagerAgent": agent_data.get("isManagerAgent", False),
        "selectedManagerAgents": agent_data.get("selectedManagerAgents", []),
        "managerAgentIntention": agent_data.get("managerAgentIntention", ""),
        "selectedKnowledgeBase": kb if kb else {},
        "knowledge_base": {
            "id": kb.get("id", ""),
            "name": kb.get("name", ""),
            "enabled": "yes",
            "collection_name": kb.get("collection_name", ""),
            "embedding_model": "BAAI/bge-small-en-v1.5",
            "description": kb.get("description", ""),
            "number_of_chunks": 5
        } if kb else {},
        "coreFeatures": agent_data.get("coreFeatures", {}),
        "llmProvider": agent_data.get("llmProvider", ""),
        "llmModel": agent_data.get("llmModel", "")
    }

    return agent_config

def call_ven_agent_service(task):
    task_id    = task.task_id
    input_data = task.input_data or {}
    submission = input_data.get("submission_data", {})
    thread_id  = input_data.get("thread_id", random.randint(1,100000))

    if thread_id is None or thread_id == "":
        thread_id = random.randint(1, 100000)
    else:
        try:
            thread_id = int(thread_id)  # Convert to int
        except (ValueError, TypeError):
            thread_id = random.randint(1, 100000)
    
    log_message(task_id, f"Using thread_id: {thread_id} (type: {type(thread_id)})")

    case_id = input_data.get("case_id", "")

    log_message(task_id, f"Calling agents for case_id: {case_id}")

    # CHANGED: Query by AgentName instead of AgentID
    agents = client["ven_instance"]["ven_agents"].find({
        "AgentName": {"$in": list(TARGET_AGENT_NAMES)}
    })

    # helper: safely convert markdown→HTML
    def md2html(s: str) -> str:
        return markdown.markdown(s, extensions=['extra'])

    # convert top-level sections like in the rerun function
    def convert_section(val):
        if isinstance(val, str):
            return md2html(val)
        if isinstance(val, dict):
            if "result" in val:
                val["result"] = md2html(val["result"])
            elif isinstance(val, str):
                val["response"] = md2html(val["response"])
            return val
        return val

    # CHANGED: Mapping of AgentNames to their specific endpoints
    agent_endpoints = {
        "InsuranceVerify": "https://insuranceverify.enowclear360.com/query",
        "LossInsights": "https://lossinsights.enowclear360.com/query",
        "PropEval": "https://propeval.enowclear360.com/query",
        "ExposureInsights": "https://exposureinsights.enowclear360.com/query",
        "EligibilityCheck": "https://eligibility.enowclear360.com/query",
        "BusineesProfileSearch": "https://businessprofile.enowclear360.com/query",  # Using your exact DB name
    }

    # Default endpoint for agents not specified in the mapping
    default_endpoint = "http://54.80.147.224:9000/query"

    results = {}
    for agent in agents:
        agent_id   = agent.get("AgentID")
        agent_name = agent.get("AgentName", agent["AgentID"])  # Get AgentName
        agent_cfg  = craft_agent_config(agent)
        suffix     = AGENT_PROMPTS.get(agent_name, "")

        # CHANGED: Check by agent_name instead of agent_id
        if agent_name == "InsuranceVerify":
            sub_data = f"case_id : {case_id}"

        elif agent_name == "LossInsights":
            sub_data = f"case_id : {case_id}"

        elif agent_name == "PropEval":
            sub_data = f"case_id : {case_id}"       

        elif agent_name == "ExposureInsights":
            sub_data = f"case_id : {case_id}" 

        elif agent_name == "EligibilityCheck":
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Run", None)

        elif agent_name == "BusineesProfileSearch":  # Using your exact DB name
            # Only send Common within Submission Data
            sub_data = {
                "Common": submission.get("Common")
            }

        else:
            # Default case, send the entire submission
            sub_data = submission

        full_message = f"{sub_data} {suffix}".strip()
        log_message(task_id, f"sending to {agent_name!r}")

        try:
            # CHANGED: Select endpoint based on agent_name, fallback to default if not found
            endpoint = agent_endpoints.get(agent_name, default_endpoint)
            r = requests.post(
                endpoint,
                json={
                    # "agent_config": agent_cfg,  # Commented out as per original
                    "message":      full_message,
                    "thread_id":    thread_id
                },
                timeout=300
            )
            raw = r.json()

            # apply markdown→HTML conversion to each top-level field
            for k, v in raw.items():
                raw[k] = convert_section(v)

            results[agent_name] = raw

        except Exception as e:
            results[agent_name] = {"error": str(e)}

    return results

def call_ven_agent_service_rerun(task):
    task_id       = task.task_id
    input_data    = task.input_data or {}
    log_message(task_id, "Rerun: pull + call agents")

    # 1) pull from Mongo
    case_id       = input_data.get("case_id")
    modified_data = input_data.get("modified_data", {})
    mongo_doc     = client["Submission_Intake"]["BP_DATA"].find_one({"case_id": case_id}) or {}
    submission    = mongo_doc.get("submission_data", {})

    # 2) merge
    merged_data = deep_update(copy.deepcopy(submission), modified_data)
    thread_id   = input_data.get("thread_id", random.randint(1,100000))

    if thread_id is None or thread_id == "":
        thread_id = random.randint(1, 100000)
    else:
        try:
            thread_id = int(thread_id)  # Convert to int
        except (ValueError, TypeError):
            thread_id = random.randint(1, 100000)

    # helper: safely convert markdown→HTML
    def md2html(s: str) -> str:
        return markdown.markdown(s, extensions=['extra'])

    # process each top‑level field in an agent's response
    def convert_section(val):
        # if the entire section is a plain string → convert it
        if isinstance(val, str):
            return md2html(val)

        # if it's a dict:
        if isinstance(val, dict):
            
            if isinstance(val, dict) and "result" in val:
                # convert resp["result"]
                val["result"] = md2html(val["result"])
            elif isinstance(val, str):
                # convert the whole response string
                val["response"] = md2html(val["response"])
            # leave metadata or other keys untouched
            return val

        # any other type → leave
        return val

    # 3) call agents
    results = {}
    
    # CHANGED: Query by AgentName instead of AgentID
    agents = client["ven_instance"]["ven_agents"].find({
        "AgentName": {"$in": list(TARGET_AGENT_NAMES)}
    })

    # CHANGED: Mapping of AgentNames to their specific endpoints
    agent_endpoints = {
        "InsuranceVerify": "https://insuranceverify.enowclear360.com/query",
        "LossInsights": "https://lossinsights.enowclear360.com/query",
        "PropEval": "https://propeval.enowclear360.com/query",
        "ExposureInsights": "https://exposureinsights.enowclear360.com/query",
        "EligibilityCheck": "https://eligibility.enowclear360.com/query",
        "BusineesProfileSearch": "https://businessprofile.enowclear360.com/query",  # Using your exact DB name
    }
    # Default endpoint for agents not specified in the mapping
    default_endpoint = "http://54.80.147.224:9000/query"

    for agent in agents:
        name   = agent.get("AgentName", agent["AgentID"])  # Get AgentName
        agent_id   = agent.get("AgentID")
        config = craft_agent_config(agent)
        suffix = AGENT_PROMPTS.get(name, "")

        # CHANGED: Check by agent name instead of agent_id
        if name == "InsuranceVerify":
            sub_data = f"case_id : {case_id},modified_data : {modified_data}"

        elif name == "LossInsights":
            sub_data = f"case_id : {case_id},modified_data : {modified_data}"

        elif name == "PropEval":
            sub_data = f"case_id : {case_id},modified_data : {modified_data}"       

        elif name == "ExposureInsights":
            sub_data = f"case_id : {case_id},modified_data : {modified_data}" 

        elif name == "EligibilityCheck":
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Run", None)
            print(f"eligibility_input:{sub_data}")

        elif name == "BusineesProfileSearch":  # Using your exact DB name
            # Only send Common within Submission Data
            sub_data = {
                "Common": submission.get("Common")
            }

        else:
            # Default case, send the entire submission
            sub_data = submission

        full_message = f"{sub_data} {suffix}".strip()

        print(f"full_message:{full_message}")

        try:
            log_message(task_id, f"Calling {name}")
            # CHANGED: Select endpoint based on agent name, fallback to default if not found
            endpoint = agent_endpoints.get(name, default_endpoint)
            raw = requests.post(
                endpoint,
                json={"message": full_message, "thread_id": thread_id},
                timeout=300
            ).json()

            # convert every top‑level value
            for k, v in raw.items():
                raw[k] = convert_section(v)

            results[name] = raw

        except Exception as e:
            results[name] = {"error": str(e)}

    return {
        "status": "COMPLETED",
        "outputData": {
            "agent_output":    results,
            "submission_data": merged_data
        }
    }
