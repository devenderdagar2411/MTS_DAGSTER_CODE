# my_dagster_project/dbt_cloud_job.py
import os, requests
from dagster import op, job

DBT_CLOUD_API_TOKEN  = os.getenv("DBT_CLOUD_API_TOKEN")
DBT_CLOUD_ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")
DBT_CLOUD_JOB_ID     = os.getenv("DBT_CLOUD_JOB_ID")

@op(description="Trigger a dbt Cloud job run.")
def trigger_dbt_cloud_job(context):
    base = f"https://xn636.us1.dbt.com/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}"
    url  = f"{base}/jobs/{DBT_CLOUD_JOB_ID}/run/"

    payload = {"cause": f"Triggered from Dagster run {context.run_id}"}
    headers = {"Authorization": f"Token {DBT_CLOUD_API_TOKEN}", "Content-Type": "application/json"}

    resp = requests.post(url, json=payload, headers=headers)
    context.log.info("HTTP %s – %s", resp.status_code, resp.text)
    resp.raise_for_status()

    run_id = resp.json()["data"]["id"]
    context.log.info("Triggered dbt Cloud run %s", run_id)
    return run_id

@job
def dbt_trigger_job():
    trigger_dbt_cloud_job()
