import time
import re
import requests
import sys
import json
import subprocess
import logging

# Configuration Section
GITLAB_API_URL = "https://gitlab.com/api/v4"  # Replace with your GitLab instance URL
ACCESS_TOKEN = "glpat-i8-QE8S2j_Z7HSbZCYWs"  # Replace with your GitLab access token
CURRENT_PIPELINE_ID = "1476375124"
PROJECT_ID = "62117590"  # Replace with your GitLab project ID
JOB_NAME_PATTERN = re.compile(r".*check_(?!.*docker_build_status).*_build_status")
SLEEP_DURATION = 20  # 10 minutes in seconds
SCRIPT_PATH = "path_to_your_script.py"  # Replace with the actual path to your script

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


def process_job(job):
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
            handle_job_status_change(job_name, job_id, previous_status, job_status)

        # Track the new status
        job_status_tracker[job_name] = job_status

        # Return True if the job is still active (running/pending)
        return job_status in ["running", "pending", "created", "canceling"]

    return False


def handle_job_status_change(job_name, job_id, previous_status, current_status):
    """Handle transitions between job statuses and trigger external scripts."""
    if current_status in ["canceled", "failed"]:
        logging.warning(f"{job_name} - {job_id} transitioned to {current_status}. Calling external script...")
        call_external_script()

    elif previous_status in ["canceled", "failed"] and current_status in ["running", "pending"]:
        logging.info(f"{job_name} - {job_id} resumed from {previous_status} to {current_status}. Calling external script...")
        call_external_script()


def call_external_script():
    """Call the external Python script."""
    try:
        # subprocess.run(["python3", SCRIPT_PATH], check=True)
        logging.info("External script executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error calling external script: {e}")


def monitor_jobs():
    """Main loop to fetch and monitor jobs."""
    while True:
        jobs_data = fetch_jobs()

        running_jobs = [job["name"] for job in jobs_data if process_job(job)]

        # If there are no running or pending jobs, exit the loop
        if not running_jobs:
            logging.info("All jobs have finished.")
            break

        # Print running jobs and wait before checking again
        logging.info(f"Jobs still running or pending: {', '.join(running_jobs)}. Checking again in {SLEEP_DURATION // 60} minutes...")
        time.sleep(SLEEP_DURATION)


if __name__ == "__main__":
    monitor_jobs()
