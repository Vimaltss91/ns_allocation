import os
import logging
from mysql.connector import Error
from typing import Optional, Dict, Any
import time
import config

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
    get_upgrade_status

)
from pars_helper import extract_from_yaml, extract_from_env, parse_variables

from prom_helper import fetch_total_cpu_requests_with_validation
from db_connection import DatabaseConnection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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



    # def extract_args(self, source_type: str, yaml_file: Optional[str] = None) -> Dict:
    #     """
    #     Extracts the variables from the environment or YAML file.
    #     """
    #     if source_type not in ['yaml', 'env']:
    #         raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")
    #
    #     if source_type == 'yaml':
    #         if not yaml_file:
    #             raise ValueError("YAML file must be provided when source_type is 'yaml'")
    #         variables = extract_from_yaml(yaml_file)
    #     else:
    #         variables = extract_from_env()
    #
    #     return parse_variables(variables)

    def extract_args(self, source_type: str, yaml_file: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Extracts the variables from the environment or YAML file.
        """
        if source_type not in ['yaml', 'env']:
            raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")

        if source_type == 'yaml':
            if not yaml_file:
                raise ValueError("YAML file must be provided when source_type is 'yaml'")
            stage_variables = extract_from_yaml(yaml_file)

            # Ensure that the extracted variables are dictionaries
            parsed_stage_variables = {}
            for stage, vars in stage_variables.items():
                if isinstance(vars, dict):  # Ensure vars is a dictionary
                    parsed_stage_variables[stage] = parse_variables(vars)
                else:
                    logging.warning(f"Variables for stage '{stage}' are not in dictionary format.")

            return parsed_stage_variables

        else:
            variables = extract_from_env()
            parsed_variables = parse_variables(variables)
            return {'default': parsed_variables}  # Return as a single-stage dict

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
        kwargs['status'] = 'ASSIGNED'

        if existing_ns:
            update_namespace_status_hardcoded_ns(cursor, kwargs)
        else:
            insert_new_status(cursor, kwargs)

        update_namespace_pool_status(cursor, kwargs['namespace'], "IN-USE", "NO")

    def _handle_dynamic_namespace(self, cursor, kwargs: Dict) -> None:
        """
        Handles the insertion or update of dynamic namespaces.
        """
        existing_row = get_existing_status(cursor, kwargs)

        if existing_row:
            s_no, status, namespace, priority = existing_row
            if status == 'ASSIGNED':
                logging.info(f"Namespace {namespace} has already been assigned.")
            else:
                update_existing_status(cursor, s_no)
        else:
            kwargs['status'] = 'YET TO ASSIGN'
            kwargs['namespace'] = ''
            insert_new_status(cursor, kwargs)

    def allocate_namespace(self, **kwargs) -> Optional[str]:
        """
        Allocates a namespace for the given parameters.
        """
        try:
            with self.db_connection.get_cursor() as cursor:

                upgrade_status = get_upgrade_status(cursor, kwargs)
                assigned_status = get_existing_status(cursor, kwargs)

                if assigned_status and assigned_status[1] == 'ASSIGNED':  # Access by index if tuple is used
                    logging.info(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
                    return assigned_status[3]

                # total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, assigned_status[3])
                #total_cpu_requests = self._fetch_total_cpu_requests(cursor, assigned_status[3])
                total_cpu_requests = 400

                if total_cpu_requests is not None:
                    logging.info(f"Total CPU requests: {total_cpu_requests} cores")
                else:
                    logging.error("Failed to fetch total CPU requests.")
                    return None

                namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])

                #pipeline_url = self._get_env_variable("CI_PIPELINE_URL")
                pipeline_url = "https://example.com"
                if namespace_name:
                    update_status_and_lock(self.db_connection.connection, cursor, namespace_name, pipeline_url, kwargs)
                    # update_namespace_in_env (namespace_name)
                    return namespace_name
                else:
                    logging.warning("No available namespaces or they are locked.")
                    return None

        except Error as e:
            logging.error(f"Error during namespace allocation: {e}")
            raise NamespaceAllocationError("Namespace allocation failed.")

    def _fetch_total_cpu_requests(self, cursor, assigned_priority: str) -> Optional[int]:
        """
        Fetches the total CPU requests from Prometheus or another data source.
        """
        try:
            total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, assigned_priority)
            logging.info(f"Total CPU requests: {total_cpu_requests} cores")
            return total_cpu_requests
        except Exception as e:
            logging.error(f"Error fetching total CPU requests: {e}")
            return None


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
