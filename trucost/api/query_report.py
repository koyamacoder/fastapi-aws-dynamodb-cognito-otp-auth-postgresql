import asyncio
from enum import Enum
from typing import Annotated, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse

from trucost.api.auth import get_current_user
from trucost.core.models.query_report import (
    AthenaQueryExecutionCreate,
    AthenaQueryExecutionCreateWithUser,
    AthenaQueryExecutionResponse,
    AthenaQueryExecutionCreateBatch,
    AthenaQueryExecutionResponseWithQuery,
    AthenaQueryExecutionPagination,
    AthenaQueryExecutionCreateBatchDashboard,
)
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.user import User
from trucost.core.models.query_report import QueryExecutionStatus
from trucost.core.injector import get_services, get_settings
from trucost.core.settings import Metaservices, MetaSettings
from trucost.utilities import result_to_excel

router = APIRouter(prefix="/executions", tags=["Executions"])


class Format(str, Enum):
    excel = "excel"
    json = "json"


@router.post("/", response_model=AthenaQueryExecutionResponse)
async def create_query_execution(
    execution: AthenaQueryExecutionCreate,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Create a new query execution"""
    async with services.db.get_session() as session:
        try:
            # Get the query
            query = await services.athena_query_repo.get_by_id(
                session, execution.query_id
            )

            if query is None:
                raise HTTPException(status_code=404, detail="Query not found")

            # Get the user settings
            user_settings = await services.user_settings_repo.get_by_id(
                session, execution.user_settings_id, user.id
            )

            if user_settings is None:
                raise HTTPException(status_code=404, detail="User settings not found")

            # Get the athena client
            try:
                athena_client = await services.athena_factory.get_client(
                    **user_settings.client_kwargs()
                )
            except Exception:
                raise HTTPException(
                    status_code=401,
                    detail="Failed to connect to AWS - please check your credentials",
                )

            # Execute the query
            err = ""
            try:
                (
                    execution_id,
                    executed_query,
                ) = await athena_client.execute_query(
                    query.query,
                    {"years": execution.years, "months": execution.months},
                    user_settings.athena_database,
                    user_settings.athena_table,
                    user_settings.athena_location,
                )
            except Exception as e:
                err = e
                execution_id = str(uuid4())
                executed_query = ""

            return await services.query_report_repo.create(
                session,
                AthenaQueryExecutionCreateWithUser(
                    query_id=execution.query_id,
                    user_id=user.id,
                    execution_id=execution_id,
                    user_settings_id=execution.user_settings_id,
                    error_message=str(err),
                    executed_query=executed_query,
                ),
            )
        except Exception:
            raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/batch", response_model=List[AthenaQueryExecutionResponse])
async def create_query_execution_batch(
    batch: AthenaQueryExecutionCreateBatch,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Create a new query execution"""
    async with services.db.get_session() as session:
        # Get the queries
        if batch.all_queries:
            queries = await services.athena_query_repo.get_by_all(  # TODO: rename to get_by_category
                session, batch.category
            )
        else:
            queries = await services.athena_query_repo.get_by_ids(
                session, batch.query_ids
            )

            found_queries = [query.id for query in queries]
            missing_queries = set(batch.query_ids) - set(found_queries)

            if missing_queries:
                raise HTTPException(
                    status_code=404, detail=f"Queries not found: {missing_queries}"
                )

        # Get the user settings
        user_settings = await services.user_settings_repo.get_by_id(
            session, batch.user_settings_id, user.id
        )

        if user_settings is None:
            raise HTTPException(status_code=404, detail="User settings not found")

        # Get the athena client
        try:
            athena_client = await services.athena_factory.get_client(
                **user_settings.client_kwargs()
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail="Failed to connect to AWS - please check your credentials",
            )

        # Execute the queries
        f_executions = []
        for query in queries:
            f_execution = athena_client.execute_query(
                query.query,
                {"years": batch.years, "months": batch.months},
                user_settings.athena_database,
                user_settings.athena_table,
                user_settings.athena_location,
            )
            f_executions.append(f_execution)

        # list of execution ids or exceptions
        _executions = await asyncio.gather(*f_executions, return_exceptions=True)

        executions = []
        batch_id = str(uuid4())
        for execution, query in zip(_executions, queries):
            if isinstance(execution, Exception):
                executions.append(
                    AthenaQueryExecutionCreateWithUser(
                        batch_id=batch_id,
                        user_id=user.id,
                        query_id=query.id,
                        user_settings_id=batch.user_settings_id,
                        execution_id=str(uuid4()),
                        error_message=str(execution),
                        executed_query="",
                    )
                )
            else:
                executions.append(
                    AthenaQueryExecutionCreateWithUser(
                        batch_id=batch_id,
                        user_id=user.id,
                        query_id=query.id,
                        user_settings_id=batch.user_settings_id,
                        execution_id=execution[0],
                        executed_query=execution[1],
                    )
                )
        return await services.query_report_repo.create_many(session, executions)


@router.post("/batch/dashboard", response_model=List[AthenaQueryExecutionResponse])
async def create_query_execution_batch_dashboard(
    batch: AthenaQueryExecutionCreateBatchDashboard,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Create a new query execution"""
    async with services.db.get_session() as session:
        # Get the query ids
        query_ids = set([query.query_id for query in batch.queries])

        # Get the queries
        found_queries = await services.athena_query_repo.get_by_ids(
            session, list(query_ids)
        )

        # Check if the queries exist
        found_query_ids = set([query.id for query in found_queries])
        missing_query_ids = query_ids - found_query_ids

        if missing_query_ids:
            raise HTTPException(
                status_code=404, detail=f"Queries not found: {missing_query_ids}"
            )

        # Get the user settings
        user_settings = await services.user_settings_repo.get_by_id(
            session, batch.user_settings_id, user.id
        )

        if user_settings is None:
            raise HTTPException(status_code=404, detail="User settings not found")

        # Get the athena client
        try:
            athena_client = await services.athena_factory.get_client(
                **user_settings.client_kwargs()
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail="Failed to connect to AWS - please check your credentials",
            )

        # id:query (actual sql query string)
        found_query_map = {query.id: query.query for query in found_queries}

        # Execute the queries
        f_executions = []
        for query in batch.queries:
            f_execution = athena_client.execute_query(
                found_query_map[query.query_id],
                {"years": query.years, "months": query.months},
                user_settings.athena_database,
                user_settings.athena_table,
                user_settings.athena_location,
            )
            f_executions.append(f_execution)

        # list of execution ids or exceptions
        execution_results = await asyncio.gather(*f_executions, return_exceptions=True)

        executions = []
        batch_id = str(uuid4())
        for execution, query in zip(execution_results, batch.queries):
            if isinstance(execution, Exception):
                executions.append(
                    AthenaQueryExecutionCreateWithUser(
                        batch_id=batch_id,
                        user_id=user.id,
                        query_id=query.query_id,
                        user_settings_id=batch.user_settings_id,
                        execution_id=str(uuid4()),
                        error_message=str(execution),
                        executed_query="",
                        query_template_assignment_id=query.query_template_assignment_id,
                    )
                )
            else:
                executions.append(
                    AthenaQueryExecutionCreateWithUser(
                        batch_id=batch_id,
                        user_id=user.id,
                        query_id=query.query_id,
                        user_settings_id=batch.user_settings_id,
                        execution_id=execution[0],
                        executed_query=execution[1],
                        query_template_assignment_id=query.query_template_assignment_id,
                    )
                )
        return await services.query_report_repo.create_many(session, executions)


@router.get("/batch/{batch_id}/result", response_class=StreamingResponse)
async def get_batch_query_execution_result(
    batch_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Get the results of a query execution"""

    async with services.db.get_session() as session:
        # Get the executions
        executions = await services.query_report_repo.get_by_batch_id(
            session, batch_id, user.id
        )

        if not executions:
            raise HTTPException(status_code=404, detail="Query execution not found")

        if any(
            execution.status
            in [
                QueryExecutionStatus.PENDING,
                QueryExecutionStatus.RUNNING,
            ]
            for execution in executions
        ):
            raise HTTPException(
                status_code=404,
                detail="Some query executions are still pending or running",
            )

        # Get the user settings, any of the executions will have the same user settings
        user_settings = await services.user_settings_repo.get_by_id(
            session, executions[0].user_settings_id, user.id
        )

        if user_settings is None:
            raise HTTPException(status_code=404, detail="User settings not found")

        # Get the athena client
        try:
            athena_client = await services.athena_factory.get_client(
                **user_settings.client_kwargs()
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail="Failed to connect to AWS - please check your credentials",
            )

        # Get the results of the query executions
        results = await asyncio.gather(
            *[
                athena_client.get_query_results(execution.execution_id)
                for execution in executions
            ],
            return_exceptions=True,
        )

        sheet_to_result_map = dict(
            zip(
                [execution.query.query_subtype for execution in executions],
                [
                    result if not isinstance(result, Exception) else []
                    for result in results
                ],
            )
        )

        excel_file = await asyncio.to_thread(
            result_to_excel, settings.template_path, sheet_to_result_map
        )

        return StreamingResponse(
            iter([excel_file]),  # Wrap bytes in an iterator
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={batch_id}.xlsx"},
        )


@router.get("/user", response_model=AthenaQueryExecutionPagination)
async def list_user_executions(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 100,
):
    """List all query executions for a specific user"""

    # Calculate offset from page number
    offset = (page - 1) * page_size

    async with services.db.get_session() as session:
        result, total = await services.query_report_repo.list_by_user(
            session, user.id, offset, page_size
        )

        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        prev_page = page - 1 if page > 1 else None

        return AthenaQueryExecutionPagination(
            data=result,
            pagination=PaginationMetadata(
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                next_page=next_page,
                prev_page=prev_page,
            ),
        )


@router.get("/query/{query_id}", response_model=AthenaQueryExecutionPagination)
async def list_query_executions(
    query_id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 100,
):
    """List all executions of a specific query"""

    # Calculate offset from page number
    offset = (page - 1) * page_size

    async with services.db.get_session() as session:
        result, total = await services.query_report_repo.list_by_query(
            session, query_id, user.id, offset, page_size
        )

        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        prev_page = page - 1 if page > 1 else None

        return AthenaQueryExecutionPagination(
            data=result,
            pagination=PaginationMetadata(
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                next_page=next_page,
                prev_page=prev_page,
            ),
        )


@router.get(
    "/{execution_id}/status", response_model=AthenaQueryExecutionResponseWithQuery
)
async def get_query_execution_by_athena_id(
    execution_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Status for a query execution"""
    async with services.db.get_session() as session:
        # Get the execution
        execution = await services.query_report_repo.get_by_execution_id(
            session, execution_id, user.id
        )
        if execution is None:
            raise HTTPException(status_code=404, detail="Query execution not found")

        # The execution is already done
        if execution.status != QueryExecutionStatus.PENDING:
            return execution

        # Get the user settings
        user_settings = await services.user_settings_repo.get_by_id(
            session, execution.user_settings_id, user.id
        )

        if user_settings is None:
            raise HTTPException(status_code=404, detail="User settings not found")

        # Get the athena client
        try:
            athena_client = await services.athena_factory.get_client(
                **user_settings.client_kwargs()
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail="Failed to connect to AWS - please check your credentials",
            )

        # Get the status of the query execution
        res = await athena_client.get_query_execution_status(execution_id)
        if res["status"] in [
            QueryExecutionStatus.SUCCEEDED,
            QueryExecutionStatus.FAILED,
            QueryExecutionStatus.CANCELLED,
        ]:
            execution.status = res["status"]
            execution.error_message = res["failure_reason"]
            await session.commit()

        return execution


@router.get("/{execution_id}/result")
async def get_query_execution_result(
    execution_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
    format: Format = Query("excel", description="Response format - 'excel' or 'json'"),
):
    """Get the results of a query execution

    Args:
        execution_id: The ID of the query execution
        user: The current authenticated user
        services: Application services
        settings: Application settings
        format: Response format - 'excel' or 'json'
    """
    async with services.db.get_session() as session:
        # Get the execution
        execution = await services.query_report_repo.get_by_execution_id(
            session, execution_id, user.id
        )
        if execution is None:
            raise HTTPException(status_code=404, detail="Query execution not found")

        if execution.status in [
            QueryExecutionStatus.PENDING,
            QueryExecutionStatus.RUNNING,
        ]:
            return execution

        # Get the user settings
        user_settings = await services.user_settings_repo.get_by_id(
            session, execution.user_settings_id, user.id
        )

        if user_settings is None:
            raise HTTPException(status_code=404, detail="User settings not found")

        # Get the athena client
        try:
            athena_client = await services.athena_factory.get_client(
                **user_settings.client_kwargs()
            )
        except Exception:
            raise HTTPException(
                status_code=401,
                detail="Failed to connect to AWS - please check your credentials",
            )

        # Get the results of the query execution
        result = await athena_client.get_query_results(execution_id)

        # Return JSON if requested
        if format == Format.json:
            return {
                "query_type": execution.query.query_subtype,
                "data": result if not isinstance(result, Exception) else [],
            }

        # Otherwise return Excel (default)
        excel_file = await asyncio.to_thread(
            result_to_excel,
            settings.template_path,
            {
                execution.query.query_subtype: result
                if not isinstance(result, Exception)
                else []
            },
        )

        return StreamingResponse(
            iter([excel_file]),  # Wrap bytes in an iterator
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={execution_id}.xlsx"
            },
        )


@router.get("/{id}", response_model=AthenaQueryExecutionResponse)
async def get_query_execution(
    id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Get a query execution by ID"""
    async with services.db.get_session() as session:
        execution = await services.query_report_repo.get_by_id(session, id, user.id)
        if execution is None:
            raise HTTPException(status_code=404, detail="Query execution not found")
        return execution
