import asyncio
from typing import Any, Dict, List, Optional, AsyncGenerator


import boto3
from botocore.exceptions import ClientError, BotoCoreError

from trucost.core.services.base import BaseService


class DynamoDBService(BaseService):
    """
    Service for DynamoDB operations with basic caching for read operations.
    """

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ):
        # Initialize DynamoDB client with credentials
        self.dynamodb_client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    async def put_item(self, table_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Put an item into a DynamoDB table.

        Args:
            table_name: The name of the table
            item: The item to put into the table

        Returns:
            Response from DynamoDB

        Raises:
            ClientError: If there's an error with the request
            BotoCoreError: If there's an AWS service error
        """
        try:
            response = await asyncio.to_thread(
                self.dynamodb_client.put_item, TableName=table_name, Item=item
            )
            return response
        except (ClientError, BotoCoreError) as e:
            print(f"Error putting item in DynamoDB: {str(e)}")
            raise

    async def get_item(
        self, table_name: str, key: Dict[str, Any], consistent_read: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get an item from a DynamoDB table.

        Args:
            table_name: The name of the table
            key: The primary key of the item to get
            consistent_read: Whether to use strongly consistent reads

        Returns:
            The item if found, None otherwise
        """
        cache_key = f"{table_name}:{str(key)}"

        # Check cache first for non-consistent reads
        if not consistent_read and cache_key in self.cache:
            return self.cache[cache_key]

        try:
            response = await asyncio.to_thread(
                self.dynamodb_client.get_item,
                TableName=table_name,
                Key=key,
                ConsistentRead=consistent_read,
            )

            item = response.get("Item")
            if item and not consistent_read:
                self.cache[cache_key] = item

            return item
        except (ClientError, BotoCoreError) as e:
            print(f"Error getting item from DynamoDB: {str(e)}")
            raise

    async def query(
        self,
        table_name: str,
        key_condition_expression: str,
        expression_attribute_values: Dict[str, Any],
        index_name: Optional[str] = None,
        filter_expression: Optional[str] = None,
        consistent_read: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Query items from a DynamoDB table.

        Args:
            table_name: The name of the table
            key_condition_expression: The key condition expression
            expression_attribute_values: The expression attribute values
            index_name: Optional name of the index to query
            filter_expression: Optional filter expression
            consistent_read: Whether to use strongly consistent reads

        Returns:
            List of items matching the query
        """
        try:
            query_params = {
                "TableName": table_name,
                "KeyConditionExpression": key_condition_expression,
                "ExpressionAttributeValues": expression_attribute_values,
                "ConsistentRead": consistent_read,
            }

            if index_name:
                query_params["IndexName"] = index_name
            if filter_expression:
                query_params["FilterExpression"] = filter_expression

            response = await asyncio.to_thread(
                self.dynamodb_client.query, **query_params
            )

            return response.get("Items", [])
        except (ClientError, BotoCoreError) as e:
            print(f"Error querying DynamoDB: {str(e)}")
            raise

    async def update_item(
        self,
        table_name: str,
        key: Dict[str, Any],
        update_expression: str,
        expression_attribute_values: Dict[str, Any],
        condition_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an item in a DynamoDB table.

        Args:
            table_name: The name of the table
            key: The primary key of the item to update
            update_expression: The update expression
            expression_attribute_values: The expression attribute values
            condition_expression: Optional condition expression

        Returns:
            Response from DynamoDB
        """
        try:
            update_params = {
                "TableName": table_name,
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeValues": expression_attribute_values,
                "ReturnValues": "ALL_NEW",
            }

            if condition_expression:
                update_params["ConditionExpression"] = condition_expression

            response = await asyncio.to_thread(
                self.dynamodb_client.update_item, **update_params
            )

            # Invalidate cache for this item
            cache_key = f"{table_name}:{str(key)}"
            self.cache.pop(cache_key, None)

            return response
        except (ClientError, BotoCoreError) as e:
            print(f"Error updating item in DynamoDB: {str(e)}")
            raise

    async def delete_item(
        self,
        table_name: str,
        key: Dict[str, Any],
        condition_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Delete an item from a DynamoDB table.

        Args:
            table_name: The name of the table
            key: The primary key of the item to delete
            condition_expression: Optional condition expression

        Returns:
            Response from DynamoDB
        """
        try:
            delete_params = {
                "TableName": table_name,
                "Key": key,
            }

            if condition_expression:
                delete_params["ConditionExpression"] = condition_expression

            response = await asyncio.to_thread(
                self.dynamodb_client.delete_item, **delete_params
            )

            # Invalidate cache for this item
            cache_key = f"{table_name}:{str(key)}"
            self.cache.pop(cache_key, None)

            return response
        except (ClientError, BotoCoreError) as e:
            print(f"Error deleting item from DynamoDB: {str(e)}")
            raise

    async def batch_write_items(
        self,
        table_name: str,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Write multiple items to a DynamoDB table in batch.

        Args:
            table_name: The name of the table
            items: List of items to write

        Returns:
            Response from DynamoDB
        """
        try:
            # DynamoDB batch write has a limit of 25 items per request
            batch_size = 25
            request_items = []

            for i in range(0, len(items), batch_size):
                batch = items[i : i + batch_size]
                put_requests = [{"PutRequest": {"Item": item}} for item in batch]

                request_items.append({table_name: put_requests})

            responses = []
            for request in request_items:
                response = await asyncio.to_thread(
                    self.dynamodb_client.batch_write_item, RequestItems=request
                )
                responses.append(response)

            return responses
        except (ClientError, BotoCoreError) as e:
            print(f"Error batch writing items to DynamoDB: {str(e)}")
            raise

    async def scan(
        self,
        table_name: str,
        filter_expression: Optional[str] = None,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        select: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Scan a DynamoDB table with optional filtering.

        Args:
            table_name: The name of the table to scan
            filter_expression: Optional filter expression (e.g., "age > :age AND begins_with(email, :email)")
            expression_attribute_values: Values for the filter expression (e.g., {":age": {"N": "21"}, ":email": {"S": "test"}})
            expression_attribute_names: Attribute name mappings for reserved words (e.g., {"#yr": "year"})
            index_name: Optional secondary index to scan
            limit: Maximum number of items to return
            select: The attributes to return (ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT)

        Returns:
            List of items matching the scan criteria
        """
        try:
            scan_params = {
                "TableName": table_name,
            }

            if filter_expression:
                scan_params["FilterExpression"] = filter_expression
            if expression_attribute_values:
                scan_params["ExpressionAttributeValues"] = expression_attribute_values
            if expression_attribute_names:
                scan_params["ExpressionAttributeNames"] = expression_attribute_names
            if index_name:
                scan_params["IndexName"] = index_name
            if limit:
                scan_params["Limit"] = limit
            if select:
                scan_params["Select"] = select

            all_items = []
            last_evaluated_key = None

            while True:
                if last_evaluated_key:
                    scan_params["ExclusiveStartKey"] = last_evaluated_key

                response = await asyncio.to_thread(
                    self.dynamodb_client.scan, **scan_params
                )

                items = response.get("Items", [])
                all_items.extend(items)

                # Handle pagination
                last_evaluated_key = response.get("LastEvaluatedKey")

                # Stop if we've hit the limit or no more items
                if not last_evaluated_key or (limit and len(all_items) >= limit):
                    break

            return all_items[:limit] if limit else all_items

        except (ClientError, BotoCoreError) as e:
            print(f"Error scanning DynamoDB table: {str(e)}")
            raise

    async def get_account_id_by_domain(
        self, table_name: str, domain_name: str
    ) -> Optional[str]:
        """
        Get the account ID associated with a specific domain name.

        Args:
            table_name: The name of the DynamoDB table
            domain_name: The domain name to search for

        Returns:
            The account ID if found, None otherwise
        """
        try:
            items = await self.scan(
                table_name=table_name,
                filter_expression="domain_name = :domain",
                expression_attribute_values={":domain": {"S": domain_name}},
                limit=1,  # Since we expect one match, limit to 1 for efficiency
            )

            # Return the first matching account ID if found
            if items:
                return items[0].get("accountid", {}).get("S")
            return None

        except (ClientError, BotoCoreError) as e:
            print(f"Error getting account ID for domain {domain_name}: {str(e)}")
            raise

    async def get_all_records(
        self,
        table_name: str,
        select: Optional[str] = None,
        index_name: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all records from a DynamoDB table.

        Args:
            table_name: The name of the DynamoDB table
            select: Optional - The attributes to return (ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT)
            index_name: Optional - Name of the index to use
            batch_size: Optional - Number of items to fetch in each batch (for pagination)

        Returns:
            List of all items in the table
        """
        try:
            return await self.scan(
                table_name=table_name,
                select=select,
                index_name=index_name,
                limit=batch_size,
            )

        except (ClientError, BotoCoreError) as e:
            print(f"Error fetching all records from table {table_name}: {str(e)}")
            raise

    async def get_all_records_paginated(
        self,
        table_name: str,
        page_size: int = 100,
        select: Optional[str] = None,
        index_name: Optional[str] = None,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Fetch all records from a DynamoDB table with pagination.
        This is a generator that yields batches of records.

        Args:
            table_name: The name of the DynamoDB table
            page_size: Number of items to fetch in each batch
            select: Optional - The attributes to return
            index_name: Optional - Name of the index to use

        Yields:
            Batches of items from the table
        """
        try:
            scan_params = {
                "TableName": table_name,
            }

            if select:
                scan_params["Select"] = select
            if index_name:
                scan_params["IndexName"] = index_name
            if page_size:
                scan_params["Limit"] = page_size

            last_evaluated_key = None

            while True:
                if last_evaluated_key:
                    scan_params["ExclusiveStartKey"] = last_evaluated_key

                response = await asyncio.to_thread(
                    self.dynamodb_client.scan, **scan_params
                )

                items = response.get("Items", [])
                if items:
                    yield items

                # Handle pagination
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break

        except (ClientError, BotoCoreError) as e:
            print(f"Error in paginated fetch from table {table_name}: {str(e)}")
            raise

    async def get_accounts_by_domains(
        self, table_name: str, domains: List[str]
    ) -> List[str]:
        """
        Fetch all account IDs that match the given domain names.
        Silently ignores domains that don't have matching records.

        Args:
            table_name: The name of the DynamoDB table
            domains: List of domain names to search for

        Returns:
            List of account IDs for the matching domains
        """
        try:
            # Create the filter expression for multiple domains
            filter_expression = (
                "domain_name IN ("
                + ", ".join([f":domain{i}" for i in range(len(domains))])
                + ")"
            )

            # Create expression attribute values
            expression_values = {
                f":domain{i}": {"S": domain} for i, domain in enumerate(domains)
            }

            # Fetch all matching records
            items = await self.scan(
                table_name=table_name,
                filter_expression=filter_expression,
                expression_attribute_values=expression_values,
            )

            # Extract just the account IDs from the items
            account_ids = [
                item["accountid"]["S"] for item in items if "accountid" in item
            ]
            return account_ids

        except (ClientError, BotoCoreError) as e:
            print(f"Error fetching accounts for domains {domains}: {str(e)}")
            raise

    async def get_accounts_by_domains_batch(
        self, table_name: str, domains: List[str], batch_size: int = 100
    ) -> AsyncGenerator[List[str], None]:
        """
        Fetch all account IDs that match the given domain names in batches.
        Silently ignores domains that don't have matching records.
        Use this for a large number of domains to avoid memory issues.

        Args:
            table_name: The name of the DynamoDB table
            domains: List of domain names to search for
            batch_size: Number of domains to process in each batch

        Yields:
            Batches of account IDs for the matching domains
        """
        try:
            # Process domains in batches to avoid DynamoDB expression size limits
            for i in range(0, len(domains), batch_size):
                batch_domains = domains[i : i + batch_size]

                # Create the filter expression for this batch
                filter_expression = (
                    "domain_name IN ("
                    + ", ".join([f":domain{j}" for j in range(len(batch_domains))])
                    + ")"
                )

                # Create expression attribute values for this batch
                expression_values = {
                    f":domain{j}": {"S": domain}
                    for j, domain in enumerate(batch_domains)
                }

                # Fetch matching records for this batch
                items = await self.scan(
                    table_name=table_name,
                    filter_expression=filter_expression,
                    expression_attribute_values=expression_values,
                )

                if items:
                    # Extract just the account IDs from the items
                    account_ids = [
                        item["accountid"]["S"] for item in items if "accountid" in item
                    ]
                    if account_ids:
                        yield account_ids

        except (ClientError, BotoCoreError) as e:
            print(f"Error in batch fetching accounts for domains: {str(e)}")
            raise
