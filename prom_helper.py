import os

import requests
from mysql.connector import Error
import logging
import time
import config
from slack_notification import post_notification
from datetime import datetime, timedelta
from sql_helpers import update_queue_status, check_same_priority_queue

# Constants for Prometheus API
HEALTHY_ENDPOINT = "/-/healthy"
QUERY_API = "/api/v1/query"


def get_prometheus_url(cursor, namespace="occne-infra"):
    """Retrieves the Prometheus URL for the specified namespace from the database."""
    try:
        cursor.execute("SELECT url FROM prometheus WHERE namespace = %s LIMIT 1", (namespace,))
        result = cursor.fetchone()
        url = result[0] if result and result[0] else None
        if url:
            logging.info(f"Retrieved Prometheus URL: {url}")
        else:
            logging.warning("Prometheus URL not found.")
        return url
    except Error as e:
        logging.error(f"Database error while retrieving URL: {e}")
        return None


def is_url_reachable(prometheus_url, timeout=5):
    """Checks if the Prometheus URL is reachable by querying its /-/healthy endpoint."""
    test_endpoint = f"{prometheus_url}{HEALTHY_ENDPOINT}"
    ip_address = prometheus_url.split("//")[1].split(":")[0]

    # Configure no_proxy directly in the request
    proxies = {
        "http": None,
        "https": None,
        "no_proxy": ip_address  # Add IP to bypass proxy for this specific request
    }

    try:
        response = requests.get(test_endpoint, timeout=timeout, proxies=proxies)
        response.raise_for_status()
        logging.info(f"Prometheus URL is reachable: {prometheus_url}")
        return True
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as e:
        logging.error(f"Prometheus URL is not reachable: {e}")
        return False


def fetch_dynamic_prometheus_url():
    """Fetch dynamic Prometheus URL from Django."""
    try:
        url = f"http://{config.DJANGO_HOST}:{config.DJANGO_PORT}/{config.DJANGO_URI}"

        proxies = {
            "http": None,
            "https": None,
            "no_proxy": config.DJANGO_HOST  # Add IP to bypass proxy for this specific request
        }

        response = requests.get(url, timeout=60, proxies=proxies)
        if response.status_code == 200:
            data = response.json()
            prometheus_url = data.get("prometheus_url")
            if prometheus_url:
                logging.info(f"Updated Promethues URL is {prometheus_url}")
                return prometheus_url
            else:
                logging.error("Prometheus URL not found in the JSON response.")
        else:
            logging.error(f"Failed to retrieve URL. HTTP Status Code: {response.status_code}")

    except requests.RequestException as e:
        logging.error(f"Error while calling dynamic URL: {e}")
    return None


def fetch_total_cpu_requests_from_prometheus(prometheus_url, timeout=10):
    """Fetches the total CPU requests from Prometheus."""
    query = 'sum(kube_pod_container_resource_requests{resource="cpu",node=~".*"})'
    api_url = f"{prometheus_url}{QUERY_API}"
    params = {'query': query}

    ip_address = prometheus_url.split("//")[1].split(":")[0]

    # Configure no_proxy directly in the request
    proxies = {
        "http": None,
        "https": None,
        "no_proxy": ip_address  # Add IP to bypass proxy for this specific request
    }
    try:
        response = requests.get(api_url, params=params, timeout=timeout, proxies=proxies)
        response.raise_for_status()
        data = response.json()

        if data.get('status') != 'success':
            logging.error(f"Error in Prometheus query: {data.get('error')}")
            return None

        result = data['data']['result']

        if result:
            total_cpu_request = float(result[0]['value'][1])
            logging.info(f"Fetched total CPU requests: {total_cpu_request} cores.")
            return total_cpu_request
        else:
            logging.warning("No data returned by Prometheus query.")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Prometheus: {e}")
        return None


def fetch_sum_estimate_cpu(cursor):
    """Fetches the sum of estimated CPU from the database for assigned namespaces."""
    query = """
        SELECT SUM(cpu_estimate) 
        FROM namespace_status 
        WHERE status = 'ASSIGNED' 
        AND (ats_status IS NULL );
    """
    try:
        cursor.execute(query)
        sum_cpu = cursor.fetchone()[0] or 0
        logging.info(f"Sum of estimated CPU for assigned namespaces: {sum_cpu} cores.")
        return sum_cpu
    except Error as e:
        logging.error(f"Database error while fetching sum of estimated CPU: {e}")
        return 0


def recheck_priority(cursor, s_no):
    """Fetches the priority using the s_no"""
    query = """
        SELECT priority, queue_date FROM namespace_status WHERE s_no = %s;
    """
    try:
        cursor.execute(query, (s_no,))
        priority = cursor.fetchone()[0]
        return priority
    except Error as e:
        logging.error(f"Database error while fetching priority: {e}")
        return 0


def fetch_priority_job_estimate_cpu(cursor, priorities_to_check):
    """Fetches the sum of estimated CPU for priority jobs in the queue."""
    if not priorities_to_check:
        return 0

    placeholders = ', '.join(['%s'] * len(priorities_to_check))
    query = f"""
        SELECT SUM(cpu_estimate)
        FROM namespace_status
        WHERE status IN ('YET TO ASSIGN', 'QUEUED') 
        AND priority IN ({placeholders})
        AND date >= NOW() - INTERVAL {config.PRIORITY_CHECK_INTERVAL_MINUTES} MINUTE
    """
    try:
        cursor.execute(query, priorities_to_check)
        priority_cpu = cursor.fetchone()[0] or 0
        logging.info(f"Sum of estimated CPU for priority jobs: {priority_cpu} cores.")
        return priority_cpu
    except Error as e:
        logging.error(f"Database error while fetching priority job estimate CPU: {e}")
        return 0


def check_priority_condition(cursor, current_build_priority):
    """Checks if higher priority jobs exist in the queue."""
    priorities_to_check = get_priorities_to_check(current_build_priority)

    if not priorities_to_check:
        logging.info("No higher priority jobs to check.")
        return False

    placeholders = ', '.join(['%s'] * len(priorities_to_check))
    query = f"""
        SELECT 1
        FROM namespace_status
        WHERE status = 'YET TO ASSIGN'
        AND priority IN ({placeholders})
        AND date >= NOW() - INTERVAL {config.PRIORITY_CHECK_INTERVAL_MINUTES} MINUTE
        LIMIT 1
    """
    try:
        cursor.execute(query, priorities_to_check)
        exists = cursor.fetchone() is not None
        logging.info(f"Higher priority jobs exist: {exists}")
        return exists
    except Error as e:
        logging.error(f"Database error while checking priority condition: {e}")
        return False


def get_priorities_to_check(current_build_priority):
    """Returns a tuple of priorities to check based on the current build priority."""
    priority_mapping = {
        "Low": ("CRITICAL", "HIGH", "MEDIUM"),
        "Medium": ("CRITICAL", "HIGH"),
        "High": ("CRITICAL",)
    }
    priorities = priority_mapping.get(current_build_priority, ())
    logging.info(f"Priorities to check for current build ({current_build_priority}): {priorities}")
    return priorities


# def fetch_total_cpu_requests_with_validation(cursor, current_build_priority, current_report, current_release_tag, current_ats_release_tag):
def fetch_total_cpu_requests_with_validation(cursor, s_no, kwargs: dict):
    """
    Fetches total CPU requests from Prometheus and validates it against certain thresholds.
    Sends Slack notifications when specific limits are exceeded, ensuring messages are sent only once per loop iteration.
    Returns the total CPU requests if it passes validation, otherwise None.
    """

    current_report = kwargs["official_build"]
    current_release_tag = kwargs["release_tag"]
    current_ats_release_tag = kwargs["ats_release_tag"]
    current_custom_message = kwargs["custom_message"]

    prometheus_url = get_prometheus_url(cursor) or fetch_dynamic_prometheus_url()
    pipeline_url = os.getenv('CI_PIPELINE_URL', '')
    if not prometheus_url or not is_url_reachable(prometheus_url):
        logging.error("Unable to retrieve a reachable Prometheus URL.")
        return None

    def wait_and_retry(message):
        """Logs a warning, rolls back the transaction, and sleeps before retrying."""
        logging.warning(message)
        cursor.execute("ROLLBACK;")
        time.sleep(config.SLEEP_DURATION)

    # Flags to ensure Slack notifications are sent only once per condition
    high_priority_notified = False
    oci_limit_notified = False
    four_hour_notification_sent = False
    status_updated = False

    # Track start time of the loop
    loop_start_time = datetime.now()

    while True:
        try:
            total_cpu_requests = fetch_total_cpu_requests_from_prometheus(prometheus_url)
            if total_cpu_requests is None:
                logging.error("Failed to fetch total CPU requests. Exiting validation loop.")
                return None

            # Start a transaction to ensure data consistency
            cursor.execute("START TRANSACTION;")

            # Fetch sum of estimated CPU and calculate combined CPU usage
            sum_estimate_cpu = fetch_sum_estimate_cpu(cursor)
            combined_cpu = total_cpu_requests + sum_estimate_cpu
            current_build_priority, current_build_queue_date = recheck_priority(cursor, s_no)

            if current_build_queue_date:  # Check if current_build_queue_date is not empty or None
                check_same_priority = check_same_priority_queue(cursor, s_no, current_build_priority, current_build_queue_date)
            else:
                check_same_priority = False  # Or handle the case when it's empty/null

            # Check priority conditions and higher priority jobs
            priority_condition_check = check_priority_condition(cursor, current_build_priority)
            if priority_condition_check:
                priority_job_estimate_cpu = fetch_priority_job_estimate_cpu(cursor, get_priorities_to_check(current_build_priority))

            # Check if the loop has been running for more than 4 hours
            elapsed_time = datetime.now() - loop_start_time
            if elapsed_time >= timedelta(hours=4) and not four_hour_notification_sent:
                four_hour_msg = (f"Build {current_release_tag} has been queued for over 4 hours. Please investigate further."
                                 f"\n{current_custom_message}\nCheck at: {pipeline_url}")
                post_notification(current_report, current_release_tag, current_ats_release_tag, four_hour_msg)
                four_hour_notification_sent = True  # Ensure this message is sent only once

            # High CPU limit check
            if (total_cpu_requests > config.CPU_LIMIT_HIGH or
                    combined_cpu > config.CPU_LIMIT_HIGH or
                    (priority_condition_check and combined_cpu + priority_job_estimate_cpu > config.CPU_LIMIT_HIGH)):

                if not oci_limit_notified:
                    oci_limit_msg = (f"Deployment is queued for build {current_release_tag}. There is no more space in OCI for this deployment. {current_custom_message}.\n "
                                     f"Please check at: {pipeline_url}")
                    post_notification(current_report, current_release_tag, current_ats_release_tag, oci_limit_msg)
                    oci_limit_notified = True

                if not status_updated:
                    update_queue_status(cursor, s_no, "QUEUED")
                    status_updated = True

                # If same priority jobs exist, retry with wait
                if check_same_priority:
                    wait_and_retry(f"Found same priority jobs with earlier QueueDate. Retrying in {config.SLEEP_DURATION / 60} minutes...")
                    continue

                wait_and_retry(f"Total or combined CPU requests exceed high limit ({config.CPU_LIMIT_HIGH}). Retrying in {config.SLEEP_DURATION / 60} minutes...")
                continue

            # Medium CPU limit check: Notify once if exceeded
            if (total_cpu_requests > config.CPU_LIMIT_MEDIUM or
                    combined_cpu > config.CPU_LIMIT_MEDIUM or
                    (combined_cpu + priority_job_estimate_cpu > config.CPU_LIMIT_MEDIUM if priority_condition_check else False)):

                if not high_priority_notified and current_build_priority.lower() not in ['high', 'critical']:
                    priority_msg = (
                        f"Deployment is queued for build {current_release_tag}. If it is a high priority job, please contact DevOps to potentially prioritise deployment. {current_custom_message}.\n"
                        f"Please check at: {pipeline_url}")
                    post_notification(current_report, current_release_tag, current_ats_release_tag, priority_msg)
                    high_priority_notified = True

                if not status_updated:
                    update_queue_status(cursor, s_no, "QUEUED")
                    status_updated = True

                if current_build_priority.lower() not in ['high', 'critical'] and check_same_priority:
                    wait_and_retry(f"Total or combined CPU requests ({combined_cpu}) exceed medium limit ({config.CPU_LIMIT_MEDIUM}). "
                                   f"Only 'High' and 'Critical' priority jobs are allowed. Retrying...")
                    continue

            # Commit the transaction and return total CPU requests if all checks pass
            cursor.execute("COMMIT;")
            logging.info("CPU requests are within limits. Proceeding with allocation.")
            return total_cpu_requests

        except Exception as e:
            cursor.execute("ROLLBACK;")
            logging.error(f"Error while validating total CPU requests: {e}")
            time.sleep(config.SLEEP_DURATION)


def get_total_cpu_requests_from_user():
    """Gets total CPU requests from user input or Prometheus."""
    try:
        user_input = input("Please enter a number: ")
        return int(user_input)
    except ValueError:
        logging.error("Invalid input. Please enter a valid number.")
        return None

# def fetch_total_cpu_requests_with_validation(cursor, current_build_priority):
#     """
#     Fetches total CPU requests from Prometheus and validates it against certain thresholds.
#     Returns the total CPU requests if it passes validation, otherwise None.
#     """
#
#     prometheus_url = get_prometheus_url(cursor)
#
#     if not prometheus_url or not is_url_reachable(prometheus_url):
#         logging.warning("Prometheus URL from DB is not reachable. Attempting to fetch dynamic URL")
#
#         prometheus_url = fetch_dynamic_prometheus_url()
#         if prometheus_url or  is_url_reachable(prometheus_url):
#             logging.info("Successfully retrieved and verfied the dynamic prometheus URL")
#         else:
#             logging.error("Failed to reterieve or reach Prometheus URL")
#             return None
#     while True:
#         try:
#             # Fetch total CPU requests from user
#             total_cpu_requests = fetch_total_cpu_requests_from_prometheus(prometheus_url)
#             #total_cpu_requests = get_total_cpu_requests_from_user()
#             if total_cpu_requests is None:
#                 logging.error("Failed to fetch total CPU requests. Exiting validation loop.")
#                 return None
#
#             # Check if total CPU requests are within the acceptable limit
#             if total_cpu_requests > config.CPU_LIMIT_HIGH:
#                 logging.warning(f"Total CPU requests ({total_cpu_requests}) exceeded limit ({config.CPU_LIMIT_HIGH}). Waiting for {config.SLEEP_DURATION / 60} minutes before rechecking...")
#                 time.sleep(config.SLEEP_DURATION)
#                 continue
#
#             # Start a transaction to ensure data consistency
#             cursor.execute("START TRANSACTION;")
#
#             # Fetch the sum of estimated CPU from the database
#             sum_estimate_cpu = fetch_sum_estimate_cpu(cursor)
#             if total_cpu_requests + sum_estimate_cpu > config.CPU_LIMIT_HIGH:
#                 logging.warning(f"Combined CPU Requests: {total_cpu_requests + sum_estimate_cpu} exceeds limit {config.CPU_LIMIT_HIGH}. Waiting...")
#                 cursor.execute("ROLLBACK;")
#                 time.sleep(config.SLEEP_DURATION)
#                 continue
#
#             # Check priority conditions if any
#             priority_condition_check = check_priority_condition(cursor, current_build_priority)
#             if priority_condition_check:
#                 priority_job_estimate_cpu = fetch_priority_job_estimate_cpu(cursor, get_priorities_to_check(current_build_priority))
#                 if total_cpu_requests + sum_estimate_cpu + priority_job_estimate_cpu > config.CPU_LIMIT_HIGH:
#                     logging.warning(f"Combined CPU requests exceed {config.CPU_LIMIT_HIGH} cores and higher priority jobs exist. Waiting...")
#                     cursor.execute("ROLLBACK;")
#                     time.sleep(config.SLEEP_DURATION)
#                     continue
#
#             # If all conditions are satisfied, commit the transaction and return the total CPU requests
#             cursor.execute("COMMIT;")
#             logging.info("CPU requests are within limits. Proceeding with allocation.")
#             return total_cpu_requests
#
#         except Exception as e:
#             cursor.execute("ROLLBACK;")  # Rollback on error
#             logging.error(f"Error while validating total CPU requests: {e}")
#             time.sleep(config.SLEEP_DURATION)  # Sleep before retrying
