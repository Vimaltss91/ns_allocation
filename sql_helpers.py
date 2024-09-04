import logging
from mysql.connector import Error
from helpers import get_namespace_prefix
import config
import time
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_query(cursor, query, params=()):
    """Utility function to execute a query with error handling."""
    try:
        cursor.execute(query, params)
    except Error as e:
        logging.error(f"Database error: {e}")
        raise


def update_namespace_pool_status(cursor, namespace_name: str, status: str, lock: str):
    """
    Update the status and lock of a namespace in the namespace table.
    """
    query = """
        UPDATE namespace
        SET status = %s, allocation_lock = %s
        WHERE namespace = %s
    """
    params = (status, lock, namespace_name)
    execute_query(cursor, query, params)
    logging.info(f"Namespace '{namespace_name}' status updated to '{status}' with lock '{lock}'.")


def update_existing_status(cursor, s_no: int) -> None:
    """Updates the status of an existing namespace record."""
    query = """
        UPDATE namespace_status
        SET status = 'YET TO ASSIGN', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
        WHERE s_no = %s
    """
    params = (s_no,)
    execute_query(cursor, query, params)
    logging.info(f"Updated row with s_no {s_no} to 'YET TO ASSIGN' and unlocked.")


def insert_new_status(cursor, kwargs: dict) -> None:
    """Inserts a new namespace status record into the database."""
    query = """
        INSERT INTO namespace_status (
            nf_type, release_tag, ats_release_tag, namespace, is_csar, is_asm, is_tgz, is_internal_ats,
            is_occ, is_pcf, is_converged, is_pcrf, tls_version, upg_rollback, official_build, priority, status, allocation_lock, 
            owner, custom_message, cpu_estimate
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NO', %s, %s, %s
        )
    """
    params = (
        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['namespace'],
        kwargs['is_csar'], kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
        kwargs['is_pcf'], kwargs['is_converged'], kwargs['is_pcrf'], kwargs['tls_version'], kwargs['upg_rollback'], kwargs['official_build'],
        kwargs['priority'], kwargs['status'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate']
    )
    execute_query(cursor, query, params)
    logging.info(f"Added NF '{kwargs['nf_type']}' for release tag '{kwargs['release_tag']}' in database.")


def get_existing_status(cursor, kwargs: dict) -> tuple:
    """Retrieves the existing status for a namespace from the database."""
    query = """
        SELECT s_no, status, namespace, priority FROM namespace_status
        WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
        AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
        AND is_pcf = %s AND is_converged = %s AND is_pcrf = %s AND tls_Version = %s
        AND upg_rollback = %s AND official_build = %s AND custom_message = %s
    """
    params = (
        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
        kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
        kwargs['is_converged'], kwargs['is_pcrf'], kwargs['tls_version'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
    )
    execute_query(cursor, query, params)
    return cursor.fetchone()


def update_status_and_lock(connection, cursor, namespace_name: str, pipeline_url: str, kwargs: dict):
    """
    Updates the namespace status and commits the transaction.
    """
    try:
        update_query = """
            UPDATE namespace_status
            SET namespace = %s, pipeline = %s, status = 'ASSIGNED', allocation_lock = 'NO'
            WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
            AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
            AND is_pcf = %s AND is_converged = %s AND is_pcrf = %s AND tls_Version = %s 
            AND upg_rollback = %s AND official_build = %s AND custom_message = %s
        """
        update_params = (
            namespace_name, pipeline_url, kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
            kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
            kwargs['is_converged'], kwargs['is_pcrf'], kwargs['tls_version'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
        )
        execute_query(cursor, update_query, update_params)

        update_pool_query = "UPDATE namespace SET status = 'In-Use', allocation_lock = 'NO' WHERE namespace = %s"
        execute_query(cursor, update_pool_query, (namespace_name,))

        connection.commit()
        logging.info(f"Namespace '{namespace_name}' has been allocated to release_tag '{kwargs['release_tag']}'")

    except Error as e:
        connection.rollback()
        logging.error(f"Failed to update namespace and status: {e}")


def delete_namespace_from_status(cursor, namespace_name: str):
    """
    Deletes a row from namespace_status where namespace matches.
    """
    query = "DELETE FROM namespace_status WHERE namespace = %s"
    execute_query(cursor, query, (namespace_name,))
    logging.info(f"Deleted namespace '{namespace_name}' from namespace_status table.")


def get_existing_namespace_hardcoded_ns(cursor, namespace: str):
    """
    Fetches assigned status for the given namespace.
    """
    query = "SELECT s_no, status, namespace, priority FROM namespace_status WHERE namespace = %s"
    execute_query(cursor, query, (namespace,))
    return cursor.fetchone()


def update_namespace_status_hardcoded_ns(cursor, kwargs: dict) -> None:
    """Updates the status of an existing namespace."""
    query = """
        UPDATE namespace_status
        SET nf_type = %s, release_tag = %s, ats_release_tag = %s, is_csar = %s, is_asm = %s, is_tgz = %s, 
            is_internal_ats = %s, is_occ = %s, is_pcf = %s, is_converged = %s, is_pcrf = %s, tls_version = %s, upg_rollback = %s, 
            official_build = %s, priority = %s, owner = %s, custom_message = %s, cpu_estimate = %s, 
            status = 'ASSIGNED', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
        WHERE namespace = %s
    """
    params = (
        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'],
        kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
        kwargs['is_pcf'], kwargs['is_converged'],  kwargs['is_pcrf'], kwargs['tls_version'], kwargs['upg_rollback'],
        kwargs['official_build'],kwargs['priority'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate'], kwargs['namespace']
    )
    execute_query(cursor, query, params)
    logging.info(f"Updated namespace '{kwargs['namespace']}' to 'ASSIGNED'.")


def find_and_lock_available_namespace(cursor, nf_type: str) -> Optional[str]:
    """
    Tries to find and lock an available namespace for allocation, with retries and waiting.
    """
    namespace_prefix = get_namespace_prefix(nf_type)
    if not namespace_prefix:
        return None

    while True:
        query = """
            SELECT namespace FROM namespace 
            WHERE status = 'Available' AND allocation_lock = 'NO' 
              AND namespace LIKE %s 
            LIMIT 1
        """
        params = (f'{namespace_prefix}%',)
        execute_query(cursor, query, params)
        available_namespace = cursor.fetchone()

        if available_namespace:
            namespace_name = available_namespace[0]
            lock_namespace(cursor, namespace_name)
            return namespace_name
        else:
            logging.info(f"No available namespaces. Retrying in {config.SLEEP_DURATION} minutes...")
            time.sleep(config.SLEEP_DURATION)



def lock_namespace(cursor, namespace_name: str):
    """
    Locks a namespace for allocation.
    """
    query = "UPDATE namespace SET allocation_lock = 'YES' WHERE namespace = %s"
    execute_query(cursor, query, (namespace_name,))
    logging.info(f"Namespace '{namespace_name}' is now locked.")
