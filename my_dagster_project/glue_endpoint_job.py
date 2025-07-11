# my_dagster_project/glue_endpoint_job.py
import os, json, requests
from dagster import op, job

# ---- CONSTANTS you would normally pass in run‑config -----------------------
JOB_NAME   = "glu-ibmi-s3-to-ice-itclvt-UAT"
JOB_RUN_ID = (
    "jr_2ad018d32dce3d07331593add1a7314e3cac56d38ad8c2b3c92917c9cdbbee4b"
)
ENDPOINT_URL = (
    "https://p0x5s9afkj.execute-api.us-east-1.amazonaws.com/V1/GET-glue-job-status"
)
API_KEY = os.getenv("STATUS_API_KEY", "UUiSeD91pf42yKbcDPv6952g8YmiFYGe7nuazbt6")
# ----------------------------------------------------------------------------

@op
def call_glue_status_api(context):
    payload = {"job_name": JOB_NAME, "job_run_id": JOB_RUN_ID}
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}

    resp = requests.post(ENDPOINT_URL, json=payload, headers=headers)
    context.log.info("HTTP %s – %s", resp.status_code, resp.text)
    resp.raise_for_status()

    data = resp.json()
    context.log.info("Parsed JSON:\n%s", json.dumps(data, indent=2))
    return data

@job
def glue_job_runner():
    call_glue_status_api()
