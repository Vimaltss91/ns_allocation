import re
import logging
import os
from mysql.connector import Error
import config

import requests
from requests.exceptions import RequestException, ConnectionError, HTTPError, Timeout

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def priority_check(official_build: str, release_tag: str, upg_rollback: str) -> str:
    """
    Determines the priority based on official build, release tag, and upgrade rollback status.
    """
    if official_build == 'YES':
        if upg_rollback == 'YES':
            return 'Medium'
        elif re.match(r'^\d{2}\.\d{1,2}\.\d{1,2}$', release_tag):
            return 'Critical'
        elif re.match(r'^\d{2}\.\d{1,2}\.\d{1,2}-ocngf-\d+.*', release_tag):
            return 'Low'
        else:
            return 'High'
    elif 'ocngf-pre-dev' in release_tag:
        return 'Medium'
    else:
        return config.DEFAULT_PRIORITY


def determine_policy_mode(variables: dict) -> tuple:
    """
    Determines the policy mode based on provided variables.
    """
    policy_mode = variables.get('POLICY_MODE', '').lower()
    is_pcf = 'YES' if policy_mode == 'pcf' else 'NO'
    is_converged = 'YES' if policy_mode == 'occnp' else 'NO'
    is_pcrf = 'YES' if variables.get('TEST_POLICY_SUITE', '') == "CN-PCRF" else 'NO'
    is_occ = 'YES' if variables.get('INCLUDE_OCC_FEATURES', '').lower() == 'true' else 'NO'
    return is_pcf, is_converged, is_occ, is_pcrf


def get_namespace_prefix(nf_type: str) -> str:
    """
    Returns the namespace prefix based on the nf_type.
    """
    prefixes = {
        'policy': 'o-devops-pol',
        'bsf': 'o-devops-bsf'
    }
    return prefixes.get(nf_type, None)


def update_namespace_in_env(namespace_name: str):
    """
    Updates the NAMESPACE variable in the environment file.
    """
    env_file = config.ENV_FILE
    try:
        lines = read_lines(env_file)
        with open(env_file, 'w') as file:
            updated = False
            for line in lines:
                if line.startswith("NAMESPACE="):
                    file.write(f"NAMESPACE={namespace_name}\n")
                    updated = True
                else:
                    file.write(line)

            if not updated:
                file.write(f"NAMESPACE={namespace_name}\n")

        logging.info(f"NAMESPACE updated to '{namespace_name}' in {env_file}")

    except IOError as e:
        logging.error(f"Error updating NAMESPACE in {env_file}: {e}")


def read_lines(file_path: str) -> list:
    """
    Reads lines from a file.
    """
    with open(file_path, 'r') as file:
        return file.readlines()


def check_bastion_ip():
    bastion_ip = os.getenv("BASTION_IP")
    oci_bastion_host = os.getenv("OCI_BASTION_HOST")

    if bastion_ip != oci_bastion_host:
        logging.error("Environment variable mismatch: BASTION_IP and OCI_BASTION_HOST are not the same.")
        return False
    logging.info("Environment variable check passed: BASTION_IP and OCI_BASTION_HOST are the same.")
    return True


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
