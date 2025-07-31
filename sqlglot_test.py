import sqlglot
from sqlglot.expressions import Select, Column


# query = (
#     """
# SELECT
# bill_payer_account_id,
# line_item_usage_account_id,
# line_item_product_code,
# product_region,
# line_item_usage_type,
# line_item_operation,
# SPLIT_PART(line_item_resource_id, ':', 6) AS line_item_resource_id,
# sum_line_item_usage_amount,
# CAST(cost_per_resource AS DECIMAL(16, 8)) AS sum_line_item_unblended_cost
# FROM
#   (
#   SELECT
#     line_item_resource_id,
#     product_region,
#     line_item_operation,
#     pricing_unit,
#     line_item_usage_account_id,
#     line_item_usage_type,
#     line_item_product_code,
#     bill_payer_account_id,
#     SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
#     SUM(SUM(line_item_unblended_cost)) OVER (PARTITION BY line_item_resource_id) AS cost_per_resource,
#     SUM(SUM(line_item_usage_amount)) OVER (PARTITION BY line_item_resource_id, pricing_unit) AS usage_per_resource_and_pricing_unit,
#     COUNT(pricing_unit) OVER (PARTITION BY line_item_resource_id) AS pricing_unit_per_resource
#   FROM
#   ${table_name}$
# WHERE
# year in (${year}$) and month in (${month}$)
# and line_item_product_code = 'AWSELB'

#     AND line_item_line_item_type = 'Usage'
#   GROUP BY
#     bill_payer_account_id,
# line_item_usage_account_id,
# line_item_product_code,
# product_region,
# pricing_unit,
# line_item_usage_type,
# line_item_resource_id,
# line_item_operation
#   )
# WHERE

#   usage_per_resource_and_pricing_unit > 24
#   AND pricing_unit_per_resource = 1
# ORDER BY
#   cost_per_resource DESC;
# """.replace("${table_name}$", "table_name")
#     .replace("${year}$", "2024")
#     .replace("${month}$", "01")
# )


query = """
Select sum(line_item_unblended_cost) AS sum_line_item_unblended_cost,month(bill_billing_period_start_date) AS month from trucostcur group by month(bill_billing_period_start_date)
"""


def get_top_level_select_columns(sql: str):
    # Parse the query
    parsed = sqlglot.parse_one(sql)

    # Find only the top-level SELECT (ignore subqueries)
    top_level_select = parsed.find(Select)

    # Extract column aliases or names
    columns = []
    for expr in top_level_select.expressions:
        alias = expr.alias
        print(f"{expr=}")
        if alias:
            columns.append(alias)
        elif isinstance(expr, Column):
            columns.append(expr.name)
        # else:
        #     columns.append(expr.sql())  # what will
    return columns


print(get_top_level_select_columns(query))
