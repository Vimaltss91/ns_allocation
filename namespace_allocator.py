import os
import logging
from mysql.connector import Error
from typing import Optional, Dict
import config
from enum import Enum
from monitor_jobs import  monitor_jobs

from sql_helpers import (
    find_and_lock_available_namespace,
    get_existing_namespace_hardcoded_ns,
    update_status_and_lock,
    delete_namespace_from_status,
    update_namespace_status_hardcoded_ns,
    get_existing_status,
    update_namespace_pool_status,
    update_existing_status,
    insert_new_status,
    get_upgrade_status,
    update_pipeline_url
)

from helpers import update_namespace_in_env
from pars_helper import extract_from_yaml, extract_from_env, parse_variables
from prom_helper import fetch_total_cpu_requests_with_validation
from db_connection import DatabaseConnection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NamespaceStatus(Enum):
    HARDCODE = "HARDCODE"
    ASSIGNED = "ASSIGNED"
    YET_TO_ASSIGN = "YET TO ASSIGN"

class NamespaceAllocationError(Exception):
    """Custom exception for namespace allocation errors."""
    pass


class NamespaceAllocator:
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    def _get_env_variable(self, var_name: str) -> Optional[str]:
        """
        Retrieves an environment variable.
        """
        value = os.getenv(var_name)
        if not value:
            logging.warning(f"Environment variable '{var_name}' is not set.")
        return value

    def extract_args(self, source_type: str, yaml_file: Optional[str] = None) -> Dict:
        """
        Extracts the variables from the environment or YAML file.
        """
        if source_type not in ['yaml', 'env']:
            raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")

        if source_type == 'yaml':
            if not yaml_file:
                raise ValueError("YAML file must be provided when source_type is 'yaml'")
            variables = extract_from_yaml(yaml_file)
        else:
            variables = extract_from_env()

        return parse_variables(variables)

    def insert_or_update_status(self, **kwargs) -> None:
        """
        Inserts or updates the namespace status in the database.
        """
        try:
            with self.db_connection.get_cursor() as cursor:
                if kwargs['namespace'].startswith("o-devops-"):
                    self._handle_hardcoded_namespace(cursor, kwargs)
                else:
                    self._handle_dynamic_namespace(cursor, kwargs)

                self.db_connection.commit()
        except Error as e:
            logging.error(f"Error during status insertion or update: {e}")
            raise NamespaceAllocationError("Failed to insert or update namespace status.")

    def _handle_hardcoded_namespace(self, cursor, kwargs: Dict) -> None:
        """
        Handles the insertion or update of hardcoded namespaces.
        """
        existing_ns = get_existing_namespace_hardcoded_ns(cursor, kwargs['namespace'])
        kwargs['status'] = 'HARDCODE'
        logging.info(f"Exiting namespace is {existing_ns}")
        if existing_ns:
            logging.info(f"Existing namespace {kwargs['namespace']}. Continue with Hardcode Update")
            update_namespace_status_hardcoded_ns(cursor, kwargs)
        else:
            logging.info(f"Inserting with new Update")
            insert_new_status(cursor, kwargs)

        update_namespace_pool_status(cursor, kwargs['namespace'], "IN-USE", "NO")

    def _handle_dynamic_namespace(self, cursor, kwargs: Dict) -> None:
        """
        Handles the insertion or update of dynamic namespaces.
        """
        existing_row = get_existing_status(cursor, kwargs)

        if existing_row:
            s_no, status, namespace, priority , report, release_tag, ats_release_tag = existing_row
            if status == 'ASSIGNED':
                logging.info(f"Namespace {namespace} has already been assigned.")
            elif status == 'HARDCODE':
                logging.info(f"Namespace {namespace} has already been Hardcoded.")
            else:
                update_existing_status(cursor, s_no, "YET TO ASSIGN")
        else:
            kwargs['status'] = 'YET TO ASSIGN'
            kwargs['namespace'] = ''
            insert_new_status(cursor, kwargs)

    # def allocate_namespace(self, **kwargs) -> Optional[str]:
    #     """
    #     Allocates a namespace for the given parameters.
    #     """
    #     try:
    #         with self.db_connection.get_cursor() as cursor:
    #             # clear_unread_results(cursor)
    #             pipeline_url = self._get_env_variable("CI_PIPELINE_URL")
    #             if kwargs['play_id']:
    #                 upg_status = get_upgrade_status(cursor, kwargs)
    #
    #                 upg_rollback = upg_status[1]
    #                 namespace_value = upg_status[2]
    #
    #                 # Check if upg_rollback is 'YES'
    #                 if upg_rollback == 'YES':
    #                     if namespace_value:
    #                         # Call the hardcoded namespace update function and exit
    #                         kwargs['namespace'] = namespace_value
    #                         kwargs['status'] = 'ASSIGNED'
    #                         update_namespace_status_hardcoded_ns(cursor, kwargs)
    #                         self.db_connection.commit()
    #                         update_namespace_in_env(namespace_value)
    #                         logging.info(f"Namespace {namespace_value} is already set for release tag {kwargs['release_tag']}")
    #                         return None
    #
    #             assigned_status = get_existing_status(cursor, kwargs)
    #             logging.info("Continue with auto assignment")
    #             s_no = assigned_status[0]
    #             update_pipeline_url(cursor, pipeline_url, s_no)
    #             if assigned_status and assigned_status[1] == 'ASSIGNED':  # Access by index if tuple is used
    #                 logging.info(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
    #                 update_namespace_in_env(assigned_status[2])
    #                 return assigned_status[3]
    #             if assigned_status and assigned_status[1] == 'HARDCODE':
    #                 total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, s_no, kwargs)
    #                 if total_cpu_requests is not None:
    #                     logging.info(f"Total CPU requests: {total_cpu_requests} cores")
    #                 else:
    #                     logging.error("Failed to fetch total CPU requests.")
    #                     return None
    #                 logging.info(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
    #                 update_existing_status(cursor, s_no, "ASSIGNED")
    #                 self.db_connection.commit()
    #                 update_namespace_in_env(assigned_status[2])
    #                 return assigned_status[3]
    #
    #             total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, s_no, kwargs)
    #             #total_cpu_requests = 400
    #
    #             if total_cpu_requests is not None:
    #                 logging.info(f"Total CPU requests: {total_cpu_requests} cores")
    #             else:
    #                 logging.error("Failed to fetch total CPU requests.")
    #                 return None
    #
    #             namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])
    #
    #             #pipeline_url = "https://example.com"
    #             if namespace_name:
    #                 update_status_and_lock(self.db_connection.connection, cursor, namespace_name, pipeline_url, kwargs)
    #                 update_namespace_in_env (namespace_name)
    #                 return namespace_name
    #             else:
    #                 logging.warning("No available namespaces or they are locked.")
    #                 return None
    #
    #     except Error as e:
    #         logging.error(f"Error during namespace allocation: {e}")
    #         raise NamespaceAllocationError("Namespace allocation failed.")

    def allocate_namespace(self, **kwargs) -> Optional[str]:
        """
        Allocates a namespace for the given parameters.
        """
        try:
            with self.db_connection.get_cursor() as cursor:
                pipeline_url = self._get_env_variable("CI_PIPELINE_URL")
                if kwargs['play_id']:
                    if self._handle_upgrade(cursor, kwargs):
                        return None  # Exit if the upgrade was handled

                assigned_status = get_existing_status(cursor, kwargs)
                logging.info("Continue with auto assignment")

                if assigned_status:
                    namespace_name = self._handle_assigned_status(cursor, assigned_status, pipeline_url, kwargs)
                    if namespace_name:
                        return namespace_name

                total_cpu_requests = self._fetch_and_log_cpu_requests(cursor, assigned_status, kwargs)
                if total_cpu_requests is None:
                    return None

                namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])
                if namespace_name:
                    update_status_and_lock(self.db_connection.connection, cursor, namespace_name, pipeline_url, kwargs)
                    update_namespace_in_env (namespace_name)
                    return namespace_name
                else:
                    logging.warning("No available namespaces or they are locked.")
                    return None

        except Error as e:
            logging.error(f"Error during namespace allocation: {e}")
            raise NamespaceAllocationError("Namespace allocation failed.")

    def _handle_upgrade(self, cursor, kwargs) -> bool:
        """
        Handles the upgrade status and updates the namespace if needed.
        """
        upgrade_status = get_upgrade_status(cursor, kwargs)
        upg_rollback, namespace_value = upgrade_status[1], upgrade_status[2]

        if upg_rollback == 'YES' and namespace_value:
            kwargs['namespace'] = namespace_value
            kwargs['status'] = NamespaceStatus.ASSIGNED.value
            update_namespace_status_hardcoded_ns(cursor, kwargs)
            self.db_connection.commit()
            update_namespace_in_env(namespace_value)
            logging.info(f"Namespace {namespace_value} is already set for release tag {kwargs['release_tag']}")
            return True  # Indicates that upgrade handling was successful
        return False  # Indicates no action taken

    def _handle_assigned_status(self, cursor, assigned_status, pipeline_url, kwargs) -> Optional[str]:
        """
        Handles the case when an assigned status is found.
        """
        s_no = assigned_status[0]
        update_pipeline_url(cursor, pipeline_url, s_no)

        if assigned_status[1] == NamespaceStatus.ASSIGNED.value:
            logging.info(
                f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
            update_namespace_in_env(assigned_status[2])
            return assigned_status[3]

        if assigned_status[1] == NamespaceStatus.HARDCODE.value:
            total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, s_no, kwargs)
            if total_cpu_requests is not None:
                logging.info(f"Total CPU requests: {total_cpu_requests} cores")
            else:
                logging.error("Failed to fetch total CPU requests.")
                return None

            logging.info(f"Namespace '{assigned_status[2]}' is now marked as 'ASSIGNED'")
            update_existing_status(cursor, s_no, NamespaceStatus.ASSIGNED.value)
            self.db_connection.commit()
            update_namespace_in_env(assigned_status[2])
            return assigned_status[3]

        return None  # No relevant status to return

    def _fetch_and_log_cpu_requests(self, cursor, assigned_status, kwargs) -> Optional[int]:
        """
        Fetches total CPU requests and logs the result.
        """
        s_no = assigned_status[0] if assigned_status else kwargs.get('s_no')
        total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, s_no, kwargs)

        if total_cpu_requests is not None:
            logging.info(f"Total CPU requests: {total_cpu_requests} cores")
            return total_cpu_requests

        logging.error("Failed to fetch total CPU requests.")
        return None


    # def _fetch_total_cpu_requests(self, cursor, assigned_priority: str, assigned_report: str, assigned_release_tag: str, assigned_ats_release_tag: str) -> Optional[int]:
    #     """
    #     Fetches the total CPU requests from Prometheus or another data source.
    #     """
    #     try:
    #         total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, assigned_priority, assigned_report, assigned_release_tag, assigned_ats_release_tag )
    #         logging.info(f"Total CPU requests: {total_cpu_requests} cores")
    #         return total_cpu_requests
    #     except Exception as e:
    #         logging.error(f"Error fetching total CPU requests: {e}")
    #         return None

    def delete_namespace(self, namespace_name: str) -> None:
        """
        Deletes the namespace from the database and marks it as available.
        """
        try:
            with self.db_connection.get_cursor() as cursor:
                delete_namespace_from_status(cursor, namespace_name)
                update_namespace_pool_status(cursor, namespace_name, "Available", "NO")
                self.db_connection.connection.commit()
                logging.info(f"Deleted namespace '{namespace_name}' and updated status to 'Available'.")
        except Error as e:
            logging.error(f"Error during namespace deletion: {e}")
            raise NamespaceAllocationError("Failed to delete namespace.")

    def monitor_jobs(self):
        """
        Deletes the namespace from the database and marks it as available.
        """
        try:
            with self.db_connection.get_cursor() as cursor:
                monitor_jobs(cursor)
                self.db_connection.commit()
        except Error as e:
            logging.error(f"Error during status insertion or update: {e}")
            raise NamespaceAllocationError("Failed to insert or update namespace status.")