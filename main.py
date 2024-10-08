import argparse
import config
from namespace_allocator import NamespaceAllocator
from db_connection import DatabaseConnection


def main():
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
        allocator.insert_or_update_status(**parameters)

    # Placeholder for other actions like 'allocate' and 'delete'
    elif args.action == 'allocate_namespace':
        parameters = allocator.extract_args(args.source, args.file)
        allocator.allocate_namespace(**parameters)

    elif args.action == 'delete':
        if not args.namespace:
            print("Error: --namespace is required when action is 'delete'")
            return
        allocator.delete_namespace(args.namespace)

    db_connection.close()


if __name__ == "__main__":
    main()
