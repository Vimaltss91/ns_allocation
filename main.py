import argparse
import config
import logging
import os  # Import os module to access environment variables
from namespace_allocator import NamespaceAllocator
from db_connection import DatabaseConnection

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

def check_bastion_ip():
    bastion_ip = os.getenv("BASTION_IP")
    oci_bastion_host = os.getenv("OCI_BASTION_HOST")

    if bastion_ip != oci_bastion_host:
        logging.error("Environment variable mismatch: BASTION_IP and OCI_BASTION_HOST are not the same.")
        return False
    logging.info("Environment variable check passed: BASTION_IP and OCI_BASTION_HOST are the same.")
    return True

def main():
    setup_logging()

    # Check if the environment variables BASTION_IP and OCI_BASTION_HOST are the same
    if not check_bastion_ip():
        logging.error("Exiting due to environment variable mismatch.")
        return

    parser = argparse.ArgumentParser(description="Manage namespace allocation and status.")
    parser.add_argument('action', choices=config.ACTIONS, help="Action to perform: insert_or_update, allocate, delete")
    parser.add_argument('--source', choices=['yaml', 'env'], required=False, help="Source of parameters: yaml or env")
    parser.add_argument('--file', help="YAML file containing parameters (required if source is yaml)")
    parser.add_argument('--namespace', help="Namespace to delete (required if action is delete)")

    args = parser.parse_args()

    db_connection = DatabaseConnection()
    allocator = NamespaceAllocator(db_connection)

    if args.action == 'insert_or_update':
        parameters = allocator.extract_args(args.source, args.file)
        logging.info("Parameters are %s", parameters)
        allocator.insert_or_update_status(**parameters)

    elif args.action == 'allocate_namespace':
        parameters = allocator.extract_args(args.source, args.file)
        logging.info("Parameters are %s", parameters)
        allocator.allocate_namespace(**parameters)

    elif args.action == 'delete':
        if not args.namespace:
            logging.error("Error: --namespace is required when action is 'delete'")
            return
        allocator.delete_namespace(args.namespace)

    db_connection.close()

if __name__ == "__main__":
    main()
