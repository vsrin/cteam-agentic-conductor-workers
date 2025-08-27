import base64
from pymongo import MongoClient
from app.config.mongo_config import (
    MONGO_URI,
    DATABASE_NAME,
    PROJECT_SERVICE_COLLECTION,
    BP_COLLECTION,
    SERVICE_NOW_CREDENTIALS_COLLECTION,
)

from app.config.bp_api_config import DATA_PACKAGE_IDS
from datetime import datetime

client = MongoClient(MONGO_URI)

db = client[DATABASE_NAME]

# Data from service now
project_service_collection = db[PROJECT_SERVICE_COLLECTION]

# Data from Boldpenguin
bp_collection = db[BP_COLLECTION]

# service now credentials
service_now_credentials_collection = db[SERVICE_NOW_CREDENTIALS_COLLECTION]


def fetch_case_files(case_id):
    """Fetches files associated with a case ID from MongoDB."""
    try:

        case_data = project_service_collection.find_one({"case_id": case_id})
        if not case_data:
            raise ValueError(f"No record found for case ID: {case_id}")

        files = [
            {
                "filename": attachment["filename"],
                "file_data": base64.b64decode(attachment["data"]),
            }
            for attachment in case_data.get("attachments", [])
        ]

        if not files:
            raise ValueError(f"No attachments found for case ID: {case_id}")

        return files
    except Exception as e:
        raise Exception(f"Error fetching files from MongoDB: {e}")


def save_report_data(report_data, artifi_id, tx_id):
    """Saves report data into MongoDB with mapped fields."""
    if not report_data:
        raise ValueError("Report data is missing")

    report_doc = {
        "artifi_id": artifi_id,
        "tx_id": tx_id,
        "created_on": datetime.now(),
    }

    print(report_data)
    print(type(report_data))

    report_doc["bp_parsed_response"] = report_data

    # for col_name in DATA_PACKAGE_IDS:
    #     report_doc[col_name] = report_data.get(col_name, None)

    bp_collection.insert_one(report_doc)
    print(f"Report data for tx_id {tx_id} saved successfully")

def get_service_now_credentials():
    """Fetch ServiceNow credentials from MongoDB"""
    try:
        credentials = service_now_credentials_collection.find_one(
            {"_id": "default_credentials"}
        )
        
        if not credentials:
            raise ValueError("ServiceNow credentials not found in database")
            
        return {
            "auth_token": credentials["auth_token"],
            "username": credentials["username"], 
            "password": credentials["password"]
        }
    except Exception as e:
        raise Exception(f"Error fetching ServiceNow credentials: {e}")
