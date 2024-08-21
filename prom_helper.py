import requests
from mysql.connector import Error

def get_prometheus_url_from_db(cursor):
    try:
        # Use a cursor that returns results as a dictionary
        cursor.execute("SELECT url FROM prometheus WHERE namespace = %s LIMIT 1", ('occne-infra',))
        result = cursor.fetchone()
        print ("result values is ", result)
        print ("type is result is" ,type(result))
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

def fetch_total_cpu_requests_with_validation(cursor):
    # Step 1: Try to retrieve Prometheus URL from the database
    prometheus_url = get_prometheus_url_from_db(cursor)

    # Step 2: Check if the retrieved URL is reachable
    # if prometheus_url and check_prometheus_url_reachable(prometheus_url):
    #     print("Using Prometheus URL from the database.")
    # else:
    #     print("Discovering Prometheus URL dynamically.")
    #     # Add logic here to discover the URL if needed
    #     return None

    # Step 5: Fetch total CPU requests from Prometheus
    total_cpu_requests = fetch_total_cpu_requests(prometheus_url)
    return total_cpu_requests
