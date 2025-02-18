#!/bin/bash

# Ensure all required parameters are provided
if [ $# -ne 5 ]; then
    echo -e "\nUsage: $0 DATABASE_NAME DDL_FILE CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_HOST\n"
    echo "Example: $0 my_database ddl-hits.sql my_user my_password https://my-clickhouse.com:8443"
    exit 1
fi

# Assign required parameters
DATABASE_NAME="$1"
DDL_FILE="$2"
CLICKHOUSE_USER="$3"
CLICKHOUSE_PASSWORD="$4"
CLICKHOUSE_HOST="$5"

# Create the database
echo -e "\nCreating database: $DATABASE_NAME on $CLICKHOUSE_HOST"
curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary "CREATE DATABASE IF NOT EXISTS $DATABASE_NAME" \
  "$CLICKHOUSE_HOST"

# Check if the DDL file exists
if [ ! -f "$DDL_FILE" ]; then
    echo "Error: DDL file '$DDL_FILE' not found!"
    exit 1
fi

# Create the table inside the database using the correct `database` parameter
echo -e "\nCreating table inside database: $DATABASE_NAME"
curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary @"$DDL_FILE" \
  "$CLICKHOUSE_HOST/?database=$DATABASE_NAME"

echo -e "\nDatabase and table setup completed successfully."