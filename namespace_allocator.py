import os
import yaml
import logging
from mysql.connector import Error
import config
from helpers import (
    priority_check,
    determine_policy_mode,
    get_assigned_status,
    find_and_lock_available_namespace,
    update_status_and_lock,
    update_namespace_in_env,
    delete_namespace_from_status,
    update_namespace_status
)
from prom_helper import fetch_total_cpu_requests_with_validation
from db_connection import DatabaseConnection


class NamespaceAllocator:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def extract_args(self, source_type, yaml_file=None):
        if source_type not in ['yaml', 'env']:
            raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")

        if source_type == 'yaml':
            if not yaml_file:
                raise ValueError("YAML file must be provided when source_type is 'yaml'")
            variables = self._extract_from_yaml(yaml_file)
        else:
            variables = self._extract_from_env()

        return self._parse_variables(variables)

    def _extract_from_yaml(self, yaml_file):
        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)

        first_section = next((key for key in data if key != 'stages'), None)
        if not first_section:
            raise ValueError("No valid section found after 'stages' in the YAML file.")

        return data.get(first_section, {}).get('variables', {})

    def _extract_from_env(self):
        return {key: os.getenv(key, '') for key in config.ENV_VARS}

    def _parse_variables(self, variables):
        build_nf = variables.get('BUILD_NF', '').lower()
        release_tag = variables.get('POLICY_RELEASE_TAG', '') if build_nf == 'policy' else variables.get('BSF_RELEASE_TAG', '')
        upg_rollback = 'YES' if any(variables.get(f'UPG_FEATURE_{i}', '').lower() == 'true' for i in range(1, 5)) else 'NO'

        if build_nf == 'bsf':
            is_pcf, is_converged, is_occ = ('NO', 'NO', 'NO')
        else:
            is_pcf, is_converged, is_occ = determine_policy_mode(variables)

        cpu_estimate = config.POLICY_ESTIMATE_CPU if build_nf == 'policy' else config.BSF_ESTIMATE_CPU

        priority = priority_check(
            'YES' if variables.get('REPORT', 'false').lower() == 'true' else 'NO',
            release_tag,
            upg_rollback
        )

        return {
            'nf_type': build_nf,
            'release_tag': release_tag,
            'ats_release_tag': variables.get('ATS_RELEASE_TAG', ''),
            'is_csar': 'YES' if variables.get('CSAR_DEPLOYMENT', '').lower() == 'true' else 'NO',
            'is_asm': 'YES' if variables.get('ENABLE_ISTIO_INJECTION', '').lower() == 'true' else 'NO',
            'is_tgz': 'YES' if variables.get('USE_EXTERNAL_DOCKER_REGISTRY', '').strip().lower() == 'false' else 'NO',
            'is_internal_ats': 'YES' if variables.get('INCLUDE_INTERNAL_ATS_FEATURES', '').lower() == 'true' else 'NO',
            'is_occ': is_occ,
            'is_pcf': is_pcf,
            'is_converged': is_converged,
            'upg_rollback': upg_rollback,
            'official_build': 'YES' if variables.get('REPORT', 'false').lower() == 'true' else 'NO',
            'priority': priority,
            'owner': os.getenv('GITLAB_USER_LOGIN'),
            'custom_message': variables.get('CUSTOM_NOTIFICATION_MESSAGE'),
            'cpu_estimate': cpu_estimate
        }

    def insert_or_update_status(self, **kwargs):
        cursor = self.db_connection.get_cursor()
        try:
            existing_row = self._get_existing_status(cursor, kwargs)
            if existing_row:
                s_no, status, namespace = existing_row
                if status == 'ASSIGNED':
                    logging.info(f"Namespace already allocated. Row s_no {s_no} has status 'ASSIGNED'.")
                else:
                    self._update_existing_status(cursor, s_no)
            else:
                self._insert_new_status(cursor, kwargs)

            self.db_connection.commit()
        except Error as e:
            logging.error(f"Error during status insertion or update: {e}")
        finally:
            cursor.close()

    def _get_existing_status(self, cursor, kwargs):
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

    def _update_existing_status(self, cursor, s_no):
        cursor.execute("""
            UPDATE namespace_status
            SET status = 'YET TO ASSIGN', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
            WHERE s_no = %s
        """, (s_no,))
        logging.info(f"Updated row with s_no {s_no} to 'YET TO ASSIGN' and unlocked.")

    def _insert_new_status(self, cursor, kwargs):
        cursor.execute("""
            INSERT INTO namespace_status (
                nf_type, release_tag, ats_release_tag, namespace, is_csar, is_asm, is_tgz, is_internal_ats,
                is_occ, is_pcf, is_converged, upg_rollback, official_build, priority, status, allocation_lock, 
                owner, custom_message, cpu_estimate
            ) VALUES (
                %s, %s, %s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'YET TO ASSIGN', 'NO', %s, %s, %s
            )
        """, (
            kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'],
            kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
            kwargs['is_pcf'], kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'],
            kwargs['priority'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate']
        ))
        logging.info("Inserted new row into namespace_status.")

    def allocate_namespace(self, **kwargs):
        try:
            with self.db_connection.get_cursor() as cursor:
                assigned_status = get_assigned_status(
                    cursor, kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                    kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                    kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                )

                if assigned_status and assigned_status[1] == 'ASSIGNED':  # Access by index if tuple is used
                    logging.info(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")
                    return assigned_status[2]

                total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor, assigned_status[3])

                if total_cpu_requests is not None:
                    logging.info(f"Total CPU requests: {total_cpu_requests} cores")
                else:
                    logging.error("Failed to fetch total CPU requests.")
                    return None

                namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])

                if namespace_name:
                    update_status_and_lock(
                        self.db_connection.connection,  # Use the connection attribute directly
                        cursor, namespace_name,
                        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                        kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                        kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                    )
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
