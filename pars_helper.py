import yaml
import os
import logging
import config
from helpers import determine_policy_mode, priority_check
from typing import Dict, Any

# Constants for reusability
YES = 'YES'
NO = 'NO'
DEFAULT_CUSTOM_MESSAGE = 'NULL'


def parse_variables( variables: dict) -> dict:
    build_nf = variables.get('BUILD_NF', '').lower()
    release_tag = _get_release_tag(build_nf, variables)
    upg_rollback = YES if _any_upg_features_true(variables) else NO

    is_pcf, is_converged, is_occ, is_pcrf = _determine_policy_mode(build_nf, variables)

    cpu_estimate = config.POLICY_ESTIMATE_CPU if build_nf == 'policy' else config.BSF_ESTIMATE_CPU

    priority = priority_check(
        YES if variables.get('REPORT', 'false').lower() == 'true' else NO,
        release_tag,
        upg_rollback
    )
    namespace = variables.get('NAMESPACE', '')
    bastion_ip = variables.get('BASTION_IP', '')
    upg_phase = variables.get('UPG_PHASE', '')

    return {
        'nf_type': build_nf,
        'release_tag': release_tag,
        'ats_release_tag': variables.get('ATS_RELEASE_TAG', ''),
        'is_csar': _get_boolean_as_yes_no(variables, 'CSAR_DEPLOYMENT'),
        'is_asm': _get_boolean_as_yes_no(variables, 'ENABLE_ISTIO_INJECTION'),
        'is_tgz': _get_boolean_as_yes_no(variables, 'USE_EXTERNAL_DOCKER_REGISTRY', invert=True),
        'is_internal_ats': _get_boolean_as_yes_no(variables, 'INCLUDE_INTERNAL_ATS_FEATURES'),
        'is_occ': is_occ,
        'is_pcf': is_pcf,
        'is_pcrf': is_pcrf,
        'is_converged': is_converged,
        'upg_rollback': upg_rollback,
        'tls_version': variables.get('TLS_VERSION', NO),
        'official_build': YES if variables.get('REPORT', 'false').lower() == 'true' else NO,
        'priority': priority,
        'owner': os.getenv('GITLAB_USER_LOGIN'),
        'custom_message': variables.get('CUSTOM_NOTIFICATION_MESSAGE', DEFAULT_CUSTOM_MESSAGE),
        'cpu_estimate': cpu_estimate,
        'namespace': namespace,
        'bastion_ip': bastion_ip,
        'upg_phase': upg_phase,
        'play_id': os.getenv('CI_JOB_ID')
    }


def _get_release_tag(build_nf: str, variables: dict) -> str:
    return variables.get('POLICY_RELEASE_TAG', '') if build_nf == 'policy' else variables.get('BSF_RELEASE_TAG', '')


def _any_upg_features_true(variables: dict) -> bool:
    return any(variables.get(f'UPG_FEATURE_{i}', '').lower() == 'true' for i in range(1, 5))


def _determine_policy_mode(build_nf: str, variables: dict) -> tuple:
    if build_nf == 'bsf':
        return NO, NO, NO, NO
    else:
        return determine_policy_mode(variables)


def _get_boolean_as_yes_no(variables: dict, key: str, invert: bool = False) -> str:
    value = variables.get(key, '').lower() == 'true'
    return NO if value and invert else YES if value else NO


# def extract_from_yaml(yaml_file: str) -> dict:
#     try:
#         with open(yaml_file, 'r') as file:
#             data = yaml.safe_load(file)
#
#         first_section = next((key for key in data if key != 'stages'), None)
#         if not first_section:
#             raise ValueError("No valid section found after 'stages' in the YAML file.")
#
#         return data.get(first_section, {}).get('variables', {})
#     except FileNotFoundError:
#         logging.error(f"YAML file '{yaml_file}' not found.")
#         raise
#     except yaml.YAMLError as e:
#         logging.error(f"Error parsing YAML file: {e}")
#         raise


def extract_from_yaml(yaml_file: str) -> Dict[str, Dict[str, Any]]:
    try:
        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)

        # Initialize an empty dictionary to store stage variables
        stage_variables = {}

        if not isinstance(data, dict):
            raise ValueError("YAML data is not in the expected dictionary format.")

        for job_name, job_details in data.items():
            if isinstance(job_details, dict):
                stage = job_details.get('stage')
                if stage:
                    variables = job_details.get('variables', {})
                    if isinstance(variables, dict):  # Ensure variables is a dictionary
                        stage_variables[stage] = variables
                    else:
                        logging.warning(f"Variables for stage '{stage}' are not in dictionary format.")
        return stage_variables

    except FileNotFoundError:
        logging.error(f"YAML file '{yaml_file}' not found.")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        raise
    except ValueError as e:
        logging.error(f"Value error: {e}")
        raise



def extract_from_env() -> dict:
    return {key: os.getenv(key, '') for key in config.ENV_VARS}

