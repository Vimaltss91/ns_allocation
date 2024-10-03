import time
import re
import requests
import sys
import json
import subprocess
import logging
import mysql.connector
from mysql.connector import Error
import config
from config import GITLAB_API_URL, ACCESS_TOKEN, CURRENT_PIPELINE_ID, PROJECT_ID, JOB_NAME_PATTERN, SLEEP_DURATION
from helpers import generate_sql_params
from sql_helpers import update_build_status

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Job status tracker
job_status_tracker = {}


def fetch_jobs():
    """Fetch jobs from the GitLab API for the current pipeline."""
    headers = {"PRIVATE-TOKEN": ACCESS_TOKEN}
    response = requests.get(f"{GITLAB_API_URL}/projects/{PROJECT_ID}/pipelines/{CURRENT_PIPELINE_ID}/jobs", headers=headers)

    if response.status_code != 200:
        logging.error(f"Failed to fetch jobs list. Status code: {response.status_code}")
        sys.exit(1)

    return response.json()


def process_job(cursor,job):
    """Process each job based on its status and track state changes."""
    job_name = job["name"]
    job_id = job["id"]
    job_status = job["status"]

    if re.match(JOB_NAME_PATTERN, job_name):
        logging.info(f"Processing job: {job_name} - {job_id} (status: {job_status})")

        # Check if the job was previously tracked
        previous_status = job_status_tracker.get(job_name)

        # If first time seeing the job or there's a status change
        if previous_status is None or previous_status != job_status:
            handle_job_status_change(cursor,job_name, job_id, previous_status, job_status)

        # Track the new status
        job_status_tracker[job_name] = job_status

        # Return True if the job is still active (running/pending)
        return job_status in ["running", "pending", "created", "canceling"]

    return False


def handle_job_status_change(cursor, job_name, job_id, previous_status, current_status):
    """Handle transitions between job statuses and trigger external scripts."""
    cursor.execute("START TRANSACTION;")
    if current_status in ["canceled", "failed"]:
        logging.info(f"{job_name} - {job_id} transitioned to {current_status}. Calling DB to update the status...")
        sql_query = generate_sql_params(CURRENT_PIPELINE_ID, job_name)
        logging.info(f"SQL Query condition is {sql_query} and current status is {current_status}")
        update_build_status(cursor, current_status, sql_query)

    elif previous_status in ["canceled", "failed"] and current_status in ["running", "pending"]:
        logging.info(f"{job_name} - {job_id} resumed from {previous_status} to {current_status}. Calling DB to update the status...")
        sql_query = generate_sql_params(CURRENT_PIPELINE_ID,job_name)
        logging.info(f"SQL Query condition is {sql_query} and current status is {current_status}")
        update_build_status(cursor, current_status, sql_query)
    cursor.execute("COMMIT;")


def call_external_script():
    """Call the external Python script."""
    try:
        # subprocess.run(["python3", SCRIPT_PATH], check=True)
        logging.info("External script executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error calling external script: {e}")


def monitor_jobs(cursor):
    """Main loop to fetch and monitor jobs."""
    while True:
        jobs_data = fetch_jobs()

        running_jobs = [job["name"] for job in jobs_data if process_job(cursor,job)]

        # If there are no running or pending jobs, exit the loop
        if not running_jobs:
            logging.info("All jobs have finished.")
            break

        # Print running jobs and wait before checking again
        logging.info(f"Jobs still running or pending: {', '.join(running_jobs)}. Checking again in {SLEEP_DURATION // 60} minutes...")
        time.sleep(SLEEP_DURATION)


# if __name__ == "__main__":
#     monitor_jobs()
