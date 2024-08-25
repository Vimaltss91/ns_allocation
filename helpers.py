import re
import logging
import config
import os
from mysql.connector import Error
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
    is_occ = 'YES' if variables.get('INCLUDE_OCC_FEATURES', '').lower() == 'true' else 'NO'
    return is_pcf, is_converged, is_occ


# Allocate namespace helper Section Start

def get_assigned_status(cursor, nf_type: str, release_tag: str, ats_release_tag: str, is_csar: str, is_asm: str,
                        is_tgz: str, is_internal_ats: str, is_occ: str, is_pcf: str, is_converged: str,
                        upg_rollback: str, official_build: str, custom_message: str):
    """
    Fetches assigned status for the given parameters.
    """
    cursor.execute("""
        SELECT s_no, status, namespace, priority FROM namespace_status
        WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
        AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
        AND is_pcf = %s AND is_converged = %s AND upg_rollback = %s 
        AND official_build = %s AND custom_message = %s
    """, (nf_type, release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats, is_occ, is_pcf, is_converged,
          upg_rollback, official_build, custom_message))
    return cursor.fetchone()


def find_and_lock_available_namespace(cursor, nf_type: str) -> str:
    """
    Finds and locks an available namespace for allocation.
    """
    namespace_prefix = get_namespace_prefix(nf_type)
    if not namespace_prefix:
        return None

    cursor.execute("""
        SELECT namespace FROM namespace 
        WHERE status = 'Available' AND allocation_lock = 'NO' 
          AND namespace LIKE %s 
        LIMIT 1
    """, (f'{namespace_prefix}%',))
    available_namespace = cursor.fetchone()

    if available_namespace:
        namespace_name = available_namespace[0]
        lock_namespace(cursor, namespace_name)
        return namespace_name
    return None


def get_namespace_prefix(nf_type: str) -> str:
    """
    Returns the namespace prefix based on the nf_type.
    """
    prefixes = {
        'policy': 'o-devops-pol',
        'bsf': 'o-devops-bsf'
    }
    return prefixes.get(nf_type, None)


def lock_namespace(cursor, namespace_name: str):
    """
    Locks a namespace for allocation.
    """
    cursor.execute("UPDATE namespace SET allocation_lock = 'YES' WHERE namespace = %s", (namespace_name,))
    logging.info(f"Namespace '{namespace_name}' is now locked.")


def update_status_and_lock(connection, cursor, namespace_name: str, nf_type: str, release_tag: str, ats_release_tag: str,
                           is_csar: str, is_asm: str, is_tgz: str, is_internal_ats: str, is_occ: str, is_pcf: str,
                           is_converged: str, upg_rollback: str, official_build: str, custom_message: str):
    """
    Updates the namespace status and commits the transaction.
    """
    try:
        cursor.execute("""
            UPDATE namespace_status
            SET namespace = %s, status = 'ASSIGNED', allocation_lock = 'NO'
            WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
            AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
            AND is_pcf = %s AND is_converged = %s AND upg_rollback = %s 
            AND official_build = %s AND custom_message = %s
        """, (namespace_name, nf_type, release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats, is_occ,
              is_pcf, is_converged, upg_rollback, official_build, custom_message))
        cursor.execute("UPDATE namespace SET status = 'In-Use', allocation_lock = 'NO' WHERE namespace = %s",
                       (namespace_name,))
        connection.commit()
        logging.info(f"Namespace '{namespace_name}' has been allocated to release_tag '{release_tag}'")

    except Error as e:
        connection.rollback()
        logging.error(f"Failed to update namespace and status: {e}")


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

# Allocate namespace helper Section Ends

# Delete namespace helper Section Start

def delete_namespace_from_status(cursor, namespace_name: str):
    """
    Delete the row from namespace_status where namespace matches.
    """
    cursor.execute("""
        DELETE FROM namespace_status
        WHERE namespace = %s
    """, (namespace_name,))
    logging.info(f"Deleted namespace '{namespace_name}' from namespace_status table.")


def update_namespace_status(cursor, namespace_name: str, status: str = 'Available', lock: str = 'NO'):
    """
    Update the status of the namespace in the namespace table.
    """
    cursor.execute("""
        UPDATE namespace
        SET status = %s, allocation_lock = %s
        WHERE namespace = %s
    """, (status, lock, namespace_name))
    logging.info(f"Namespace '{namespace_name}' status updated to '{status}' with lock '{lock}'.")

# Delete namespace helper Section Ends
