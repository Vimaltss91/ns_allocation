import re
import config
import os
from mysql.connector import Error


def priority_check(official_build, release_tag, upg_rollback):
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


def determine_policy_mode(variables):
    policy_mode = variables.get('POLICY_MODE', '').lower()
    is_pcf = 'YES' if policy_mode == 'pcf' else 'NO'
    is_converged = 'YES' if policy_mode == 'occnp' else 'NO'
    is_occ = 'YES' if variables.get('INCLUDE_OCC_FEATURES', '').lower() == 'true' else 'NO'
    return is_pcf, is_converged, is_occ


# Allocate namespace helper Section Start
def get_assigned_status(cursor, release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats,
                        is_occ, is_pcf, is_converged, upg_rollback, nf_type):
    cursor.execute("""
        SELECT namespace, status FROM namespace_status 
        WHERE release_tag = %s AND ats_release_tag = %s
          AND is_csar = %s AND is_asm = %s 
          AND is_tgz = %s AND is_internal_ats = %s 
          AND is_occ = %s AND is_pcf = %s 
          AND is_converged = %s AND upg_rollback = %s 
          AND nf_type = %s
    """, (release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats, is_occ, is_pcf, is_converged,
          upg_rollback, nf_type))
    return cursor.fetchone()


def find_and_lock_available_namespace(cursor, nf_type):
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
        namespace_name = available_namespace[0]  # Access by index if tuple is used
        print(f"Locking namespace '{namespace_name}' for allocation")
        cursor.execute("UPDATE namespace SET allocation_lock = 'YES' WHERE namespace = %s", (namespace_name,))
        return namespace_name
    return None


def get_namespace_prefix(nf_type):
    if nf_type == 'policy':
        return 'o-devops-pol'
    elif nf_type == 'bsf':
        return 'o-devops-bsf'
    else:
        print("Invalid nf_type")
        return None


def update_status_and_lock(connection, cursor, release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats,
                           is_occ, is_pcf, is_converged, upg_rollback, nf_type, namespace_name):
    cursor.execute("""
        UPDATE namespace_status
        SET namespace = %s, status = 'ASSIGNED', allocation_lock = 'NO'
        WHERE release_tag = %s AND ats_release_tag = %s
          AND is_csar = %s AND is_asm = %s 
          AND is_tgz = %s AND is_internal_ats = %s 
          AND is_occ = %s AND is_pcf = %s 
          AND is_converged = %s AND upg_rollback = %s
          AND nf_type = %s
    """, (namespace_name, release_tag, ats_release_tag, is_csar, is_asm, is_tgz, is_internal_ats, is_occ, is_pcf,
          is_converged, upg_rollback, nf_type))
    cursor.execute("UPDATE namespace SET status = 'In-Use', allocation_lock = 'NO' WHERE namespace = %s",
                   (namespace_name,))
    connection.commit()  # Use the connection object to commit
    print(f"Namespace '{namespace_name}' has been allocated to release_tag '{release_tag}'")


def update_namespace_in_env(namespace_name):
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

        print(f"NAMESPACE updated to '{namespace_name}' in {env_file}")

    except Exception as e:
        print(f"Error updating NAMESPACE in {env_file}: {e}")


def read_lines(file_path):
    with open(file_path, 'r') as file:
        return file.readlines()


# Allocate namespace helper Section Ends

# Delete namespace helper Section Start
def delete_namespace_from_status(cursor, namespace_name):
    """
    Delete the row from namespace_status where namespace matches.
    """
    cursor.execute("""
        DELETE FROM namespace_status
        WHERE namespace = %s
    """, (namespace_name,))


def update_namespace_status(cursor, namespace_name):
    """
    Update the status of the namespace in the namespace table to 'Available'.
    """
    cursor.execute("""
        UPDATE namespace
        SET status = 'Available', allocation_lock = 'NO'
        WHERE namespace = %s
    """, (namespace_name,))
# Delete namespace helper Section Ends
