import asyncio
from typing import Annotated, List

from fastapi import APIRouter, HTTPException, Depends, UploadFile
from sqlalchemy.exc import SQLAlchemyError

from trucost.api.auth import get_current_user
from trucost.core.repositories.athena_query import (
    QueryNotFoundError,
    QueryAlreadyExistsError,
)
from trucost.core.models.athena_query import (
    AthenaQueryCreate,
    AthenaQueryUpdate,
    AthenaQueryDelete,
    AthenaQueryResponse,
    AthenaQueryPagination,
)
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.user import User
from trucost.core.injector import get_services
from trucost.core.settings import Metaservices
from trucost.utilities.read_queries import parse_csv_file, parse_excel_file


router = APIRouter(prefix="/query", tags=["Queries"])


@router.post("/", response_model=List[AthenaQueryResponse])
async def create_queries(
    queries: List[AthenaQueryCreate],
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Create queries from JSON input"""
    async with services.db.get_session() as session:
        try:
            return await services.athena_query_repo.create(session, queries)
        except QueryAlreadyExistsError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except SQLAlchemyError:
            raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/upload", response_model=List[AthenaQueryResponse])
async def upload_queries(
    # user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    csv_file: UploadFile | None = None,
    excel_file: UploadFile | None = None,
    sheet_name: str | None = None,
):
    """Create queries from CSV file upload"""
    queries = []
    if csv_file is not None:
        queries = await parse_csv_file(csv_file)
    elif excel_file is not None:
        queries = await asyncio.to_thread(parse_excel_file, excel_file, sheet_name)

    async with services.db.get_session() as session:
        try:
            return await services.athena_query_repo.create(session, queries)
        except QueryAlreadyExistsError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except SQLAlchemyError:
            raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/{id}", response_model=AthenaQueryResponse)
async def get_query(
    id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        query = await services.athena_query_repo.get_by_id(session, id)
        if query is None:
            raise HTTPException(status_code=404, detail="Query not found")
    return query


@router.put("/", response_model=List[AthenaQueryResponse])
async def update_queries(
    queries: List[AthenaQueryUpdate],
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        try:
            return await services.athena_query_repo.update(session, queries)
        except QueryNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except SQLAlchemyError as e:
            print(e)
            raise HTTPException(status_code=500, detail="Internal Server Error")


@router.delete("/{id}")
async def delete_query(
    id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        try:
            await services.athena_query_repo.delete(session, AthenaQueryDelete(id=id))
        except QueryNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except SQLAlchemyError:
            raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/", response_model=AthenaQueryPagination)
async def list_queries(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 10,
):
    """List athena queries with pagination"""

    # Calculate offset from page number
    offset = (page - 1) * page_size

    async with services.db.get_session() as session:
        result, total = await services.athena_query_repo.list(
            session, offset, page_size
        )

        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        prev_page = page - 1 if page > 1 else None

        return AthenaQueryPagination(
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
