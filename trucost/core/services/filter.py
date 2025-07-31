from typing import List, Any, Dict

from sqlalchemy import func, and_, or_, asc, desc, Select

from trucost.core.models.common.filter import FilterOperator, GroupByConfig, SortConfig


class Filter:
    def apply_filters(
        self, models: List[Any], query: Select, filters: List[FilterOperator]
    ) -> Select:
        """Apply filter conditions to the query"""
        # Group filters by field
        filters_by_field: Dict[str, List[FilterOperator]] = {}
        for filter_op in filters:
            if filter_op.field not in filters_by_field:
                filters_by_field[filter_op.field] = []
            filters_by_field[filter_op.field].append(filter_op)

        # Process each field's filters,
        # add the field matching in the first available model,
        # rest is ignored
        filter_conditions = []
        for field, field_filters in filters_by_field.items():
            for model in models:
                column = getattr(model, field, None)
                if column is not None:
                    break

            field_conditions = []

            for filter_op in field_filters:
                if filter_op.operator == "eq":
                    field_conditions.append(column == filter_op.value)
                elif filter_op.operator == "ne":
                    field_conditions.append(column != filter_op.value)
                elif filter_op.operator == "gt":
                    field_conditions.append(column > filter_op.value)
                elif filter_op.operator == "lt":
                    field_conditions.append(column < filter_op.value)
                elif filter_op.operator == "gte":
                    field_conditions.append(column >= filter_op.value)
                elif filter_op.operator == "lte":
                    field_conditions.append(column <= filter_op.value)
                elif filter_op.operator == "in":
                    field_conditions.append(column.in_(filter_op.value))
                elif filter_op.operator == "like":
                    field_conditions.append(column.like(f"%{filter_op.value}%"))
                elif filter_op.operator == "is":
                    field_conditions.append(column.is_(filter_op.value))

            # Combine conditions for the same field with OR
            if field_conditions:
                filter_conditions.append(or_(*field_conditions))

        # Apply all field conditions with AND
        if filter_conditions:
            query = query.filter(and_(*filter_conditions))
        return query

    def apply_grouping(
        self, model: Any, query: Select, group_config: GroupByConfig
    ) -> Select:
        """Apply grouping and aggregation to the query"""
        # Add group by columns
        group_columns = [getattr(model, field) for field in group_config.fields]

        # Add aggregations
        aggregations = []
        for field, agg_func in group_config.aggregations.items():
            column = getattr(model, field)
            if agg_func == "sum":
                aggregations.append(func.sum(column).label(f"{field}_sum"))
            elif agg_func == "avg":
                aggregations.append(func.avg(column).label(f"{field}_avg"))
            elif agg_func == "min":
                aggregations.append(func.min(column).label(f"{field}_min"))
            elif agg_func == "max":
                aggregations.append(func.max(column).label(f"{field}_max"))
            elif agg_func == "count":
                aggregations.append(func.count(column).label(f"{field}_count"))

        # Add group by columns to select
        for col in group_columns:
            aggregations.append(col)

        # Modify the existing query to include aggregations and group by
        query = query.with_only_columns(*aggregations)
        query = query.group_by(*group_columns)

        return query

    def apply_sorting(
        self, model: Any, query: Select, sort_config: List[SortConfig]
    ) -> Select:
        """Apply sorting to the query"""
        for sort in sort_config:
            column = getattr(model, sort.field)

            if sort.order == "desc":
                query = query.order_by(desc(column))
            else:
                query = query.order_by(asc(column))

        return query
