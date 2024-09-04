import os

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Supported actions
ACTIONS = ['insert_or_update', 'allocate_namespace', 'delete']

ENV_VARS = [
    'BUILD_NF', 'POLICY_RELEASE_TAG', 'BSF_RELEASE_TAG', 'UPG_FEATURE_1', 'UPG_FEATURE_2',
    'UPG_FEATURE_3', 'UPG_FEATURE_4', 'POLICY_MODE', 'ATS_RELEASE_TAG', 'REPORT',
    'CSAR_DEPLOYMENT', 'ENABLE_ISTIO_INJECTION', 'USE_EXTERNAL_DOCKER_REGISTRY',
    'INCLUDE_OCC_FEATURES', 'INCLUDE_INTERNAL_ATS_FEATURES', 'GITLAB_USER_LOGIN'
    'CUSTOM_NOTIFICATION_MESSAGE', 'TLS_VERSION', 'TEST_POLICY_SUITE'
]

ENV_FILE = "bastion_parameters.env"


DEFAULT_PRIORITY = "Low"

# ESTIMATE CPU
POLICY_ESTIMATE_CPU = 80
BSF_ESTIMATE_CPU = 50

#CPU LIMITS
MAX_CPU_LIMIT = 2200
CPU_LIMIT_HIGH = MAX_CPU_LIMIT - 200
CPU_LIMIT_MEDIUM = MAX_CPU_LIMIT - 500
SLEEP_DURATION = 10  # 10 minutes
PRIORITY_CHECK_INTERVAL_MINUTES = 60



