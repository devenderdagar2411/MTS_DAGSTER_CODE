# my_dagster_project/glue_trigger_job_new.py
import os, json, requests
from dagster import op, job

# ---- CONSTANTS (hard‑coded job parameters) ---------------------------------
FILE_PATH = "s3://mts-etl-poc-ibmi-lake/SASLRM_2025-06-10_04-35-09.csv"
JOB_NAME  = "Saslrm"
SQL_QUERY = "SELECT * FROM myDataSource"
TRIGGER_URL = (
    "https://p0x5s9afkj.execute-api.us-east-1.amazonaws.com/V1/trigger-glue"
)
API_KEY = os.getenv("STATUS_API_KEY", "UUiSeD91pf42yKbcDPv6952g8YmiFYGe7nuazbt6")
# ----------------------------------------------------------------------------

@op
def call_glue_trigger_api(context):
    payload = {
        "file_path": FILE_PATH,
        "job_name":  JOB_NAME,
        "sql_query": SQL_QUERY,
    }
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}

    resp = requests.post(TRIGGER_URL, json=payload, headers=headers)
    context.log.info("HTTP %s – %s", resp.status_code, resp.text)
    resp.raise_for_status()

    data = resp.json()
    context.log.info("Parsed JSON:\n%s", json.dumps(data, indent=2))
    return data

@job
def glue_trigger_job():
    call_glue_trigger_api()
