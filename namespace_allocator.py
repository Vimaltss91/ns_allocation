import re
import sys
import yaml
import os
import config
from mysql.connector import Error
from helpers import priority_check, determine_policy_mode, get_assigned_status, find_and_lock_available_namespace, update_status_and_lock, update_namespace_in_env, delete_namespace_from_status, \
    update_namespace_status
from prom_helper import fetch_total_cpu_requests_with_validation
from db_connection import DatabaseConnection


class NamespaceAllocator:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def extract_args(self, source_type, yaml_file=None):
        variables = {}

        if source_type == 'yaml':
            if not yaml_file:
                raise ValueError("YAML file must be provided when source_type is 'yaml'")

            with open(yaml_file, 'r') as file:
                data = yaml.safe_load(file)

            stages = data.get('stages', [])
            first_section = next((key for key in data if key != 'stages'), None)

            if not first_section:
                raise ValueError("No valid section found after 'stages' in the YAML file.")

            variables = data.get(first_section, {}).get('variables', {})

        elif source_type == 'env':
            variables = {key: os.getenv(key, '') for key in config.ENV_VARS}
        else:
            raise ValueError("Invalid source_type. Use 'yaml' or 'env'.")

        build_nf = variables.get('BUILD_NF', '').lower()
        release_tag = variables.get('POLICY_RELEASE_TAG', '') if build_nf == 'policy' else variables.get('BSF_RELEASE_TAG', '')
        upg_rollback = 'YES' if any(variables.get(f'UPG_FEATURE_{i}', '').lower() == 'true' for i in range(1, 5)) else 'NO'

        is_pcf, is_converged, is_occ = ('NO', 'NO', 'NO') if build_nf == 'bsf' else determine_policy_mode(variables)

        cpu_estimate = "90" if build_nf == 'policy' else "50"

        ats_release_tag = variables.get('ATS_RELEASE_TAG', '')
        official_build = 'YES' if variables.get('REPORT', 'false').lower() == 'true' else 'NO'
        priority = priority_check(official_build, release_tag, upg_rollback)

        use_external_docker_registry = variables.get('USE_EXTERNAL_DOCKER_REGISTRY', '').strip().lower()
        is_tgz = 'YES' if use_external_docker_registry == 'false' else 'NO'

        pipeline_owner = os.getenv('GITLAB_USER_LOGIN')

        custom_message = variables.get('CUSTOM_NOTIFICATION_MESSAGE')

        return {
            'nf_type': build_nf,
            'release_tag': release_tag,
            'ats_release_tag': ats_release_tag,
            'is_csar': 'YES' if variables.get('CSAR_DEPLOYMENT', '').lower() == 'true' else 'NO',
            'is_asm': 'YES' if variables.get('ENABLE_ISTIO_INJECTION', '').lower() == 'true' else 'NO',
            'is_tgz': is_tgz,
            'is_internal_ats': 'YES' if variables.get('INCLUDE_INTERNAL_ATS_FEATURES', '').lower() == 'true' else 'NO',
            'is_occ': is_occ,
            'is_pcf': is_pcf,
            'is_converged': is_converged,
            'upg_rollback': upg_rollback,
            'official_build': official_build,
            'priority': priority,
            'owner': pipeline_owner,
            'custom_message': custom_message,
            'cpu_estimate': cpu_estimate
        }

    def insert_or_update_status(self, **kwargs):
        cursor = self.db_connection.get_cursor()
        try:
            cursor.execute("""
                SELECT s_no, status, namespace FROM namespace_status
                WHERE nf_type = %s AND release_tag = %s AND ats_release_tag = %s AND is_csar = %s
                AND is_asm = %s AND is_tgz = %s AND is_internal_ats = %s AND is_occ = %s
                AND is_pcf = %s AND is_converged = %s AND upg_rollback = %s 
                AND official_build = %s AND custom_message = %s
            """, (kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                  kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                  kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']))

            existing_row = cursor.fetchone()

            if existing_row:
                s_no, status, namespace = existing_row
                if status == 'ASSIGNED':
                    print(f"Namespace already allocated. Row s_no {s_no} has status 'ASSIGNED'.")
                else:
                    cursor.execute("""
                        UPDATE namespace_status
                        SET status = 'YET TO ASSIGN', allocation_lock = 'NO', date = CURRENT_TIMESTAMP
                        WHERE s_no = %s
                    """, (s_no,))
                    print(f"Updated row with s_no {s_no} to 'YET TO ASSIGN' and unlocked.")
            else:
                cursor.execute("""
                    INSERT INTO namespace_status (
                        nf_type, release_tag, ats_release_tag, namespace, is_csar, is_asm, is_tgz, is_internal_ats,
                        is_occ, is_pcf, is_converged, upg_rollback, official_build, priority, status, allocation_lock, 
                        owner, custom_message , cpu_estimate
                    ) VALUES (
                        %s, %s, %s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'YET TO ASSIGN', 'NO', %s, %s, %s
                    )
                """, (kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'],
                      kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'],
                      kwargs['is_pcf'], kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'],
                      kwargs['priority'], kwargs['owner'], kwargs['custom_message'], kwargs['cpu_estimate']))
                print("Inserted new row into namespace_status.")

            self.db_connection.commit()

        except Error as e:
            print(f"Error during status insertion or update: {e}")
        finally:
            cursor.close()

    def allocate_namespace(self, **kwargs):
        try:
            with self.db_connection.get_cursor() as cursor:

                # Fetch and validate total CPU requests
                total_cpu_requests = fetch_total_cpu_requests_with_validation(cursor)

                # total_cpu_requests = "2000"
                #
                if total_cpu_requests is not None:
                    print(f"Total CPU requests: {total_cpu_requests} cores")
                else:
                    print("Failed to fetch total CPU requests.")

                assigned_status = get_assigned_status(
                    cursor, kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                    kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                    kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                )

                if assigned_status and assigned_status[1] == 'ASSIGNED':  # Access by index if tuple is used
                    print(f"Namespace '{assigned_status[2]}' is already assigned for release_tag '{kwargs['release_tag']}'")

                    # update the namespace in the environment file ## Uncomment this for pipeline
                    # update_namespace_in_env(namespace_name=assigned_status[0])
                    return assigned_status[0]

                namespace_name = find_and_lock_available_namespace(cursor, kwargs['nf_type'])

                print("namespace name is", namespace_name)

                if namespace_name:
                    update_status_and_lock(
                        self.db_connection.connection,  # Use the connection attribute directly
                        cursor, namespace_name,
                        kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'], kwargs['is_asm'],
                        kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
                        kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
                    )

                    # update_namespace_in_env(namespace_name)
                    return namespace_name
                else:
                    print("No available namespaces or they are locked.")
                    return None

        except Error as e:
            print(f"Error during namespace allocation: {e}")

    def delete_namespace(self, namespace_name):
        try:
            with self.db_connection.get_cursor() as cursor:
                delete_namespace_from_status(cursor, namespace_name)
                update_namespace_status(cursor, namespace_name)
                self.db_connection.connection.commit()
                print(f"Deleted namespace '{namespace_name}' from namespace_status and updated status to 'Available'.")
        except Error as e:
            print(f"Error during namespace deletion: {e}")
