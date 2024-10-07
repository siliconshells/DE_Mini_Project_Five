import sys
import argparse
from my_lib.extract import extract
from my_lib.transform import transform_n_load
from my_lib.crud import (
    read_data,
    read_all_data,
    save_data,
    delete_data,
    update_data,
    get_table_columns,
)


def handle_arguments(args):
    """add action based on inital calls"""
    parser = argparse.ArgumentParser(description="DE ETL And Query Script")
    parser.add_argument(
        "Functions",
        choices=[
            "extract",
            "transform_n_load",
            "read_data",
            "read_all_data",
            "save_data",
            "delete_data",
            "update_data",
        ],
    )

    args = parser.parse_args(args[:1])
    print(args.Functions)
    if args.Functions == "extract":
        parser.add_argument("url")
        parser.add_argument("file_name")

    elif args.Functions == "transform_n_load":
        parser.add_argument("local_dataset")
        parser.add_argument("database_name")
        parser.add_argument("new_data_tables", type=dict)
        parser.add_argument("new_lookup_tables", type=dict)
        parser.add_argument("column_attributes", type=dict)
        parser.add_argument("column_map", type=dict)

    elif args.Functions == "read_data":
        parser.add_argument("database_name")
        parser.add_argument("table_name")
        parser.add_argument("data_id", type=int)

    elif args.Functions == "read_all_data":
        parser.add_argument("database_name")
        parser.add_argument("table_name")

    elif args.Functions == "save_data":
        parser.add_argument("database_name")
        parser.add_argument("table_name")
        parser.add_argument("row", type=list)

    elif args.Functions == "update_data":
        parser.add_argument("database_name")
        parser.add_argument("table_name")
        parser.add_argument("data_id", type=int)
        parser.add_argument("things_to_update", type=dict)

    elif args.Functions == "delete_data":
        parser.add_argument("database_name")
        parser.add_argument("table_name")
        parser.add_argument("data_id", type=int)

    elif args.Functions == "get_table_columns":
        parser.add_argument("database_name")
        parser.add_argument("table_name")

    # parse again
    return parser.parse_args(sys.argv[1:])


def main():
    """handles all the cli commands"""

    args = handle_arguments(sys.argv[1:])

    if args.Functions == "extract":
        print("Extracting data...")
        print(extract(args.url, args.file_name))

    elif args.Functions == "transform_n_load":
        print("Transforming and loading data...")
        print(
            transform_n_load(
                args.local_dataset,
                args.database_name,
                args.new_data_tables,
                args.new_lookup_tables,
                args.column_attributes,
                args.column_map,
            )
        )

    elif args.Functions == "read_data":
        print(read_data(args.database_name, args.table_name, args.data_id))

    elif args.Functions == "read_all_data":
        print(read_all_data(args.database_name, args.table_name))

    elif args.Functions == "save_data":
        print(
            save_data(
                args.database_name,
                args.table_name,
                args.row,
            )
        )

    elif args.action == "update_data":
        print(
            update_data(
                args.database_name, args.table_name, args.data_id, args.things_to_update
            )
        )

    elif args.Functions == "delete_data":
        print(delete_data(args.database_name, args.table_name, args.data_id))

    elif args.Functions == "get_table_columns":
        print(get_table_columns(args.database_name, args.table_name))

    else:
        print(f"Unknown function: {args.action}")


if __name__ == "__main__":
    main()
