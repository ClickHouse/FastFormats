#!/bin/bash

# Ensure all required parameters are provided
if [ $# -ne 4 ]; then
    echo -e "\nUsage: $0 DATABASE_NAME CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_HOST\n"
    echo "Example: $0 my_database my_user my_password https://my-clickhouse.com:8443"
    exit 1
fi

# Assign required parameters
DATABASE_NAME="$1"
CLICKHOUSE_USER="$2"
CLICKHOUSE_PASSWORD="$3"
CLICKHOUSE_HOST="$4"

# Drop the database
echo -e "\nDropping database: $DATABASE_NAME on $CLICKHOUSE_HOST"
RESPONSE=$(curl --silent --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary "DROP DATABASE IF EXISTS $DATABASE_NAME" \
  "$CLICKHOUSE_HOST")

# Check for errors in the response
if echo "$RESPONSE" | grep -q "Exception"; then
    echo "Error occurred while dropping database:"
    echo "$RESPONSE"
    exit 1
fi

echo -e "\nDatabase '$DATABASE_NAME' has been dropped successfully."