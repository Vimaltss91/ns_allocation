import requests
from mysql.connector import Error
import logging
import time
import config
from datetime import datetime, timedelta
from helpers import priority_check, determine_policy_mode, get_assigned_status


def get_prometheus_url_from_db(cursor):
    try:
        # Use a cursor that returns results as a dictionary
        cursor.execute("SELECT url FROM prometheus WHERE namespace = %s LIMIT 1", ('occne-infra',))
        result = cursor.fetchone()

        # Check if the result is not None and fetch the first element from the tuple (the 'url')
        if result and result[0]:
            print(f"Retrieved Prometheus URL from the database: {result[0]}")
            return result[0]
        else:
            print("Prometheus URL not found in the database.")
            return None
    except Error as e:
        print(f"Database error while retrieving URL: {e}")
        return None


def check_prometheus_url_reachable(prometheus_url, timeout=5):
    test_endpoint = f"{prometheus_url}/-/healthy"
    try:
        response = requests.get(test_endpoint, timeout=timeout)
        response.raise_for_status()
        print(f"Prometheus URL is reachable: {prometheus_url}")
        return True
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout) as e:
        print(f"Prometheus URL is not reachable: {e}")
        return False


def fetch_total_cpu_requests(prometheus_url, timeout=10):
    query = 'sum(kube_pod_container_resource_requests{resource="cpu",node=~".*"})'
    api_url = f"{prometheus_url}/api/v1/query"
    params = {'query': query}

    try:
        response = requests.get(api_url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if data.get('status') != 'success':
            print("Error in Prometheus query:", data.get('error'))
            return None

        result = data['data']['result']

        if result:
            total_cpu_request = float(result[0]['value'][1])
            return total_cpu_request
        else:
            print("No data returned by query")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Prometheus: {e}")
        return None


# def fetch_total_cpu_requests_with_validation(cursor):
#     # Step 1: Try to retrieve Prometheus URL from the database
#     prometheus_url = get_prometheus_url_from_db(cursor)
#
#     # Step 2: Check if the retrieved URL is reachable
#     # if prometheus_url and check_prometheus_url_reachable(prometheus_url):
#     #     print("Using Prometheus URL from the database.")
#     # else:
#     #     print("Discovering Prometheus URL dynamically.")
#     #     # Add logic here to discover the URL if needed
#     #     return None
#
#     # Step 5: Fetch total CPU requests from Prometheus
#     total_cpu_requests = fetch_total_cpu_requests(prometheus_url)
#     return total_cpu_requests


def fetch_total_cpu_requests_with_validation(cursor, current_build_priority):
    """
    Fetches total CPU requests from Prometheus and validates it against certain thresholds.

    Args:
        cursor: Database cursor to execute SQL queries.
        current_build_priority: Priority of the current build (Low, Medium, High).

    Returns:
        int: The total CPU requests if it passes validation, otherwise None.
    """
    prometheus_url = get_prometheus_url_from_db(cursor)

    # Validate Prometheus URL (Optional Step)
    # if prometheus_url and check_prometheus_url_reachable(prometheus_url):
    #     logging.info("Using Prometheus URL from the database.")
    # else:
    #     logging.error("Failed to retrieve or reach Prometheus URL.")
    #     return None

    while True:
        total_cpu_requests = get_total_cpu_requests_from_user()

        if total_cpu_requests is None:
            logging.error("Failed to fetch total CPU requests.")
            return None

        if total_cpu_requests > config.CPU_LIMIT_HIGH:
            logging.warning(f"Total CPU requests exceeded {config.CPU_LIMIT_HIGH} cores. Waiting for {config.SLEEP_DURATION / 60} minutes before rechecking...")
            time.sleep(config.SLEEP_DURATION)
            continue  # Recheck the total CPU requests after the wait

        elif total_cpu_requests > config.CPU_LIMIT_MEDIUM:
            sum_estimate_cpu = fetch_sum_estimate_cpu(cursor)
            priority_condition_check = check_priority_condition(cursor, current_build_priority)
            if total_cpu_requests + sum_estimate_cpu > config.CPU_LIMIT_HIGH:

                logging.info(f"Combined CPU Requests: {total_cpu_requests + sum_estimate_cpu} exceeds limit {config.CPU_LIMIT_HIGH}")
                time.sleep(config.SLEEP_DURATION)


            elif priority_condition_check:

                priorities_to_check = get_priorities_to_check(current_build_priority)
                priority_job_estimate_cpu = fetch_priority_job_estimate_cpu(cursor, priorities_to_check)
                if total_cpu_requests + sum_estimate_cpu + priority_job_estimate_cpu > config.CPU_LIMIT_HIGH:
                    logging.warning(f"Combined CPU requests exceed {config.CPU_LIMIT_HIGH} cores and higher priority jobs exist. Waiting for {config.SLEEP_DURATION / 60} minutes before rechecking...")
                    time.sleep(config.SLEEP_DURATION)
                else:
                    return total_cpu_requests
            else:
                logging.info("No higher priority jobs. Returning total CPU requests.")
                return total_cpu_requests
        else:
            return total_cpu_requests


def get_total_cpu_requests_from_user():
    """Gets total CPU requests from user input or Prometheus."""
    try:
        user_input = input("Please enter a number: ")
        return int(user_input)
    except ValueError:
        logging.error("Invalid input. Please enter a valid number.")
        return None


def fetch_sum_estimate_cpu(cursor):
    """Fetches the sum of estimated CPU from the database."""
    query = """
        SELECT SUM(cpu_estimate) 
        FROM namespace_status 
        WHERE status = 'ASSIGNED' 
        AND (ats_status != 'scale_down' OR ats_status IS NULL);
    """
    cursor.execute(query)
    return cursor.fetchone()[0] or 0


def check_priority_condition(cursor, current_build_priority):
    """Checks if higher priority jobs exist in the queue."""

    priorities_to_check = get_priorities_to_check(current_build_priority)

    print("priority check is ", priorities_to_check)
    if not priorities_to_check:
        return False  # No higher priorities to check, so return True

    placeholders = ', '.join(['%s'] * len(priorities_to_check))
    query = f"""
        SELECT 1
        FROM namespace_status
        WHERE status = 'YET TO ASSIGN'
        AND priority IN ({placeholders})
        AND date >= NOW() - INTERVAL 1 HOUR
        LIMIT 1
    """
    # AND date >= NOW() - INTERVAL 1 HOUR
    cursor.execute(query, priorities_to_check)
    return cursor.fetchone() is not None

def get_priorities_to_check(current_build_priority):
    """Returns a tuple of priorities to check based on the current build priority."""
    priority_mapping = {
        "Low": ("CRITICAL", "HIGH", "MEDIUM"),
        "Medium": ("CRITICAL", "HIGH"),
        "High": ("CRITICAL",)
    }
    return priority_mapping.get(current_build_priority, ())


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
        AND date >= NOW() - INTERVAL 1 HOUR
    """
    cursor.execute(query, priorities_to_check)
    return cursor.fetchone()[0] or 0
