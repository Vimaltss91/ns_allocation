import os
import logging
from mysql.connector import Error
from helpers import (
    get_assigned_status,
    find_and_lock_available_namespace,
    update_status_and_lock,
    update_namespace_in_env,
    get_existing_namespace,
    delete_namespace_from_status,
    update_namespace_status
)
from pars_helper import (
    _extract_from_yaml,
    _extract_from_env,
    _parse_variables
)
from prom_helper import fetch_total_cpu_requests_with_validation
from db_connection import DatabaseConnection


class NamespaceAllocator:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    def extract_args(self, source_type: str, yaml_file: str = None) -> dict:
        """Extract the variables from env or yaml file."""
        if source_type not in ['yaml', 'env']:
            raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")

        if source_type == 'yaml':
            if not yaml_file:
                raise ValueError("YAML file must be provided when source_type is 'yaml'")
            variables = _extract_from_yaml(yaml_file)
        else:
            variables = _extract_from_env()

        return _parse_variables(variables)

    def insert_or_update_status(self, **kwargs) -> None:
        """Inserts or updates the namespace status in the database."""
        with self.db_connection.get_cursor() as cursor:
            try:
                if kwargs['namespace'].startswith("o-devops-"):
                    self._handle_hardcoded_namespace(cursor, kwargs)
                else:
                    self._handle_dynamic_namespace(cursor, kwargs)

                self.db_connection.commit()
            except Error as e:
                logging.error(f"Error during status insertion or update: {e}")

    def _handle_hardcoded_namespace(self, cursor, kwargs: dict) -> None:
        """Handles the insertion or update of hardcoded namespaces."""
        existing_ns = get_existing_namespace(cursor, kwargs['namespace'])
        kwargs['status'] = 'ASSIGNED'

        if existing_ns:
            self._update_namespace_status(cursor, kwargs)
        else:
            self._insert_new_status(cursor, kwargs)

        self._update_namespace_pool_status(cursor, kwargs['namespace'])

    def _handle_dynamic_namespace(self, cursor, kwargs: dict) -> None:
        """Handles the insertion or update of dynamic namespaces."""
        existing_row = self._get_existing_status(cursor, kwargs)

        if existing_row :
            s_no, status, namespace = existing_row
            if status == 'ASSIGNED':
                logging.info(f"Namespace already allocated. Row s_no {s_no} has status 'ASSIGNED'.")
            else:
                self._update_existing_status(cursor, s_no)
        else:
            kwargs['status'] = 'YET TO ASSIGN'
            kwargs['namespace'] = ''
            self._insert_new_status(cursor, kwargs)

    def _update_namespace_status(self, cursor, kwargs: dict) -> None:
        """Updates the status of an existing namespace."""
        cursor.execute("""
            UPDATE namespace_status
            SET nf_type = %s, release_tag = %s, ats_release_tag = %s, is_csar = %s, is_asm = %s, is_tgz = %s, 
                is_internal_ats = %s, is_occ = %s, is_pcf = %s, is_converged = %s, upg_rollback = %s, 
                official_build = %s, priority = %s, owner = %s, custom_message = %s, cpu_estimate = %s, 
                status = 'ASSIGNED', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
            WHERE namespace = %s
        """, (
            kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'],
            kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
            kwargs['is_pcf'], kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'],
            kwargs['priority'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate'], kwargs['namespace']
        ))
        logging.info(f"Updated namespace '{kwargs['namespace']}' to 'ASSIGNED'.")

    def _update_namespace_pool_status(self, cursor, namespace: str) -> None:
        """Updates the status of a namespace in the namespace pool."""
        cursor.execute("""
            UPDATE namespace
            SET status = 'IN-USE'
            WHERE namespace = %s AND status != 'IN-USE'
        """, (namespace,))
        logging.info(f"Namespace '{namespace}' set to 'IN-USE' in namespace pool.")

    def _get_existing_status(self, cursor, kwargs: dict) -> tuple:
        """Retrieves the existing status for a namespace from the database."""
        cursor.execute("""
            SELECT s_no, status, namespace FROM namespace_status
            WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
            AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
            AND is_pcf = %s AND is_converged = %s AND upg_rollback = %s 
            AND official_build = %s AND custom_message = %s
        """, (
            kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
            kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
            kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
        ))
        return cursor.fetchone()

    def _update_existing_status(self, cursor, s_no: int) -> None:
        """Updates the status of an existing namespace record."""
        cursor.execute("""
            UPDATE namespace_status
            SET status = 'YET TO ASSIGN', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
            WHERE s_no = %s
        """, (s_no,))
        logging.info(f"Updated row with s_no {s_no} to 'YET TO ASSIGN' and unlocked.")

    def _insert_new_status(self, cursor, kwargs: dict) -> None:
        """Inserts a new namespace status record into the database."""
        cursor.execute("""
            INSERT INTO namespace_status (
                nf_type, release_tag, ats_release_tag, namespace, is_csar, is_asm, is_tgz, is_internal_ats,
                is_occ, is_pcf, is_converged, upg_rollback, official_build, priority, status, allocation_lock, 
                owner, custom_message, cpu_estimate
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NO', %s, %s, %s
            )
        """, (
            kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['namespace'],
            kwargs['is_csar'], kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
            kwargs['is_pcf'], kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'],
            kwargs['priority'], kwargs['status'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate']
        ))
        logging.info(f"Added NF '{kwargs['nf_type']}' for release tag '{kwargs['release_tag']}' in database.")

    def allocate_namespace(self, **kwargs):
        print("Starting allocate_namespace")
        try:
            with self.db_connection.get_cursor() as cursor:
                assigned_status = get_assigned_status(
                    cursor, kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                    kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                    kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                )

                if assigned_status and assigned_status[1] == 'ASSIGNED':  # Access by index if tuple is used
                    logging.info(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
                    return assigned_status[3]

                logging.info(f"Assinged status is{assigned_status}")
                # total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, assigned_status[3])
                total_cpu_requests = 400

                if total_cpu_requests is not None:
                    logging.info(f"Total CPU requests: {total_cpu_requests} cores")
                else:
                    logging.error("Failed to fetch total CPU requests.")
                    return None

                namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])
                pipeline_url = os.getenv("CI_PIPELINE_URL")
                if namespace_name:
                    update_status_and_lock(
                        self.db_connection.connection,  # Use the connection attribute directly
                        cursor, namespace_name, pipeline_url,
                        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                        kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                        kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                    )
                    # update_namespace_in_env (namespace_name)
                    return namespace_name
                else:
                    logging.warning("No available namespaces or they are locked.")
                    return None

        except Error as e:
            logging.error(f"Error during namespace allocation: {e}")

    def delete_namespace(self, namespace_name):
        try:
            with self.db_connection.get_cursor() as cursor:
                delete_namespace_from_status(cursor, namespace_name)
                update_namespace_status(cursor, namespace_name)
                self.db_connection.connection.commit()
                logging.info(f"Deleted namespace '{namespace_name}' from namespace_status and updated status to 'Available'.")
        except Error as e:
            print(f"Error during namespace deletion: {e}")
