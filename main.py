import argparse
import config
import logging
import os  # Import os module to access environment variables
from namespace_allocator import NamespaceAllocator
from db_connection import DatabaseConnection
from helpers import check_bastion_ip, setup_logging


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
