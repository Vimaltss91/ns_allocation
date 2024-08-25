import requests
from mysql.connector import Error
import logging
import time
import config
from datetime import datetime, timedelta
from helpers import priority_check, determine_policy_mode, get_assigned_status


def get_prometheus_url_from_db(cursor, namespace="occne-infra"):
    """Retrieves the Prometheus URL for the specified namespace from the database."""
    try:
        cursor.execute("SELECT url FROM prometheus WHERE namespace = %s LIMIT 1", (namespace,))
        result = cursor.fetchone()

        if result and result[0]:
            logging.info(f"Retrieved Prometheus URL from the database: {result[0]}")
            return result[0]
        else:
            logging.warning("Prometheus URL not found in the database.")
            return None
    except Error as e:
        logging.error(f"Database error while retrieving URL: {e}")
        return None


def check_prometheus_url_reachable(prometheus_url, timeout=5):
    """Checks if the Prometheus URL is reachable by querying its /-/healthy endpoint."""
    test_endpoint = f"{prometheus_url}/-/healthy"
    try:
        response = requests.get(test_endpoint, timeout=timeout)
        response.raise_for_status()
        logging.info(f"Prometheus URL is reachable: {prometheus_url}")
        return True
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as e:
        logging.error(f"Prometheus URL is not reachable: {e}")
        return False


def fetch_total_cpu_requests_from_prometheus(prometheus_url, timeout=10):
    """Fetches the total CPU requests from Prometheus."""
    query = 'sum(kube_pod_container_resource_requests{resource="cpu",node=~".*"})'
    api_url = f"{prometheus_url}/api/v1/query"
    params = {'query': query}

    try:
        response = requests.get(api_url, params=params, timeout=timeout)
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
        AND (ats_status != 'scale_down' OR ats_status IS NULL);
    """
    try:
        cursor.execute(query)
        sum_cpu = cursor.fetchone()[0] or 0
        logging.info(f"Sum of estimated CPU for assigned namespaces: {sum_cpu} cores.")
        return sum_cpu
    except Error as e:
        logging.error(f"Database error while fetching sum of estimated CPU: {e}")
        return 0


def fetch_priority_job_estimate_cpu(cursor, priorities_to_check):
    """Fetches the sum of estimated CPU for priority jobs in the queue."""
    if not priorities_to_check:
        return 0

    placeholders = ', '.join(['%s'] * len(priorities_to_check))
    query = f"""
        SELECT SUM(cpu_estimate)
        FROM namespace_status
        WHERE status = 'YET TO ASSIGN'
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


def fetch_total_cpu_requests_with_validation(cursor, current_build_priority):
    """
    Fetches total CPU requests from Prometheus and validates it against certain thresholds.
    Returns the total CPU requests if it passes validation, otherwise None.
    """
    prometheus_url = get_prometheus_url_from_db(cursor)

    # if not prometheus_url or not check_prometheus_url_reachable(prometheus_url):
    #     logging.error("Failed to retrieve or reach Prometheus URL.")
    #     return None

    while True:
        #total_cpu_requests = fetch_total_cpu_requests_from_prometheus(prometheus_url)
        total_cpu_requests = get_total_cpu_requests_from_user()
        if total_cpu_requests is None:
            logging.error("Failed to fetch total CPU requests. Exiting validation loop.")
            return None

        if total_cpu_requests > config.CPU_LIMIT_HIGH:
            logging.warning(f"Total CPU requests exceeded {config.CPU_LIMIT_HIGH} cores. Waiting for {config.SLEEP_DURATION / 60} minutes before rechecking...")
            time.sleep(config.SLEEP_DURATION)
            continue

        sum_estimate_cpu = fetch_sum_estimate_cpu(cursor)
        if total_cpu_requests + sum_estimate_cpu > config.CPU_LIMIT_HIGH:
            logging.info(f"Combined CPU Requests: {total_cpu_requests + sum_estimate_cpu} exceeds limit {config.CPU_LIMIT_HIGH}. Waiting...")
            time.sleep(config.SLEEP_DURATION)
            continue

        priority_condition_check = check_priority_condition(cursor, current_build_priority)
        if priority_condition_check:
            priority_job_estimate_cpu = fetch_priority_job_estimate_cpu(cursor, get_priorities_to_check(current_build_priority))
            if total_cpu_requests + sum_estimate_cpu + priority_job_estimate_cpu > config.CPU_LIMIT_HIGH:
                logging.warning(f"Combined CPU requests exceed {config.CPU_LIMIT_HIGH} cores and higher priority jobs exist. Waiting...")
                time.sleep(config.SLEEP_DURATION)
                continue

        logging.info("CPU requests are within limits. Proceeding with allocation.")
        return total_cpu_requests

def get_total_cpu_requests_from_user():
    """Gets total CPU requests from user input or Prometheus."""
    try:
        user_input = input("Please enter a number: ")
        return int(user_input)
    except ValueError:
        logging.error("Invalid input. Please enter a valid number.")
        return None
