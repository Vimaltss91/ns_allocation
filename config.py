import os

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Supported actions
ACTIONS = ['insert_or_update', 'allocate_namespace', 'delete', 'monitor']

ENV_VARS = [
    'BUILD_NF', 'POLICY_RELEASE_TAG', 'BSF_RELEASE_TAG', 'UPG_FEATURE_1', 'UPG_FEATURE_2',
    'UPG_FEATURE_3', 'UPG_FEATURE_4', 'POLICY_MODE', 'ATS_RELEASE_TAG', 'REPORT',
    'CSAR_DEPLOYMENT', 'ENABLE_ISTIO_INJECTION', 'USE_EXTERNAL_DOCKER_REGISTRY',
    'INCLUDE_OCC_FEATURES', 'INCLUDE_INTERNAL_ATS_FEATURES', 'GITLAB_USER_LOGIN'
    'CUSTOM_NOTIFICATION_MESSAGE', 'TLS_VERSION', 'TEST_POLICY_SUITE', 'UPG_PHASE', 'CI_JOB_ID'
]

ENV_FILE = "bastion_parameters.env"


DEFAULT_PRIORITY = "Low"

# ESTIMATE CPU
POLICY_ESTIMATE_CPU = 80
BSF_ESTIMATE_CPU = 50

#CPU LIMITS
MAX_CPU_LIMIT = 800
CPU_LIMIT_HIGH = MAX_CPU_LIMIT - 200
CPU_LIMIT_MEDIUM = MAX_CPU_LIMIT - 500
SLEEP_DURATION = 20  # 10 minutes
PRIORITY_CHECK_INTERVAL_MINUTES = 60


# GitLab API configuration
GITLAB_API_URL = "https://gitlab.com/api/v4"  # Replace with your GitLab instance URL
ACCESS_TOKEN = os.getenv("GITLAB_ACCESS_TOKEN")  # Replace with your GitLab access token
CURRENT_PIPELINE_ID = "1476898681"
PROJECT_ID = "62117590"  # Replace with your GitLab project ID

# Job name pattern (excluding 'docker_build_status')
JOB_NAME_PATTERN = r".*check_(?!.*docker_build_status).*_build_status"




