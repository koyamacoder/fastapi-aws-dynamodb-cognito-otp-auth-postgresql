import csv
import json
from datetime import datetime


def clean_value(value):
    """Clean and format values for SQL insert"""
    if value is None or value.strip() == "":
        return "NULL"
    try:
        # Try to convert to float for numeric values
        int_val = int(value)
        return str(int_val)
    except ValueError:
        try:
            float_val = float(value)
            return str(float_val)
        except Exception:
            pass

        # Handle JSON strings
        if value.startswith("{") and value.endswith("}"):
            try:
                # Validate JSON and return escaped string
                json.loads(value)
                return f"'{value.replace("'", "''")}'"
            except json.JSONDecodeError:
                pass
        # Handle dates
        try:
            date = datetime.strptime(value, "%Y-%m-%d")
            print(date)
            return f"'{date}'"
        except ValueError:
            pass
        # Default string handling
        return f"'{value.replace("'", "''")}'"


def generate_insert_queries(csv_file_path, table_name):
    """Generate SQL insert queries from CSV data"""
    with open(csv_file_path, "r") as file:
        reader = csv.reader(file, delimiter="|")

        # Column names based on the data structure
        # |id|query_title|resource_id|payer_account_id|usage_account_id|payer_account_name|usage_account_name|product_code|year|month|potentials_saving_percentage|potential_savings_usd|unblended_cost|amortized_cost|query_date|achieved_savings_usd|current_config| recommended_config|implementation_details|last_updated|Source|

        columns = [
            "id",
            "query_title",
            "resource_id",
            "payer_account_id",
            "usage_account_id",
            "payer_account_name",
            "usage_account_name",
            "product_code",
            "year",
            "month",
            "potentials_saving_percentage",
            "potential_savings_usd",
            "unblended_cost",
            "amortized_cost",
            "query_date",
            "achieved_savings_usd",
            "current_config",
            "recommended_config",
            "implementation_details",
            "last_updated",
            "Source",
        ]

        insert_queries = []
        unique_ids = set()
        for row in reader:
            if len(row) != len(columns):  # Skip malformed rows
                print("Skipping row")
                continue

            values = [clean_value(val) for val in row]
            values[3] = f"'800679018866'"
            values[4] = f"'{values[4]}'"
            values[-2] = f"{values[-2][:11]} {values[-2][11:]}"
            if values[0] in unique_ids:
                print(f"Skipping duplicate ID: {values[0]}")
                continue
            unique_ids.add(values[0])
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            insert_queries.append(query)

    return insert_queries


def main():
    input_file = "data.txt"  # Input CSV file
    table_name = "cost_optimization_recommendations"  # Target table name

    try:
        queries = generate_insert_queries(input_file, table_name)

        # Write queries to output file
        output_file = "insert_queries.sql"
        with open(output_file, "w") as f:
            f.write("BEGIN;\n\n")  # Start transaction
            for query in queries:
                f.write(query + "\n")
            f.write("\nCOMMIT;")  # End transaction

        print(f"Successfully generated {len(queries)} insert queries in {output_file}")

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
