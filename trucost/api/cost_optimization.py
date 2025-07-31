from typing import Annotated, List, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import or_, and_
from sqlalchemy.exc import SQLAlchemyError

from trucost.api.auth import get_current_user
from trucost.core.models.cost_optimization import (
    CostOptimizationPagination,
    GroupByConfig,
    FilterFacetsFilter,
    ErrorResponse,
    CostOptimizationNotificationPayload,
    CostOptimizeWithResourceOwner,
    CostOptimizationFilterWithIds,
)
from trucost.core.models.resource_owner import ResourceOwner, ResourceOwnerStatus
from trucost.core.models.user import User
from trucost.core.injector import get_services, get_settings
from trucost.core.settings import Metaservices, MetaSettings
from trucost.core.models.common.pagination import PaginationMetadata


router = APIRouter(prefix="/cost-optimization", tags=["Cost Optimization"])


@router.post("/")
async def get_all_cost_data(
    user: Annotated[User, Depends(get_current_user)],
    payload: CostOptimizationFilterWithIds,
    settings: Annotated[MetaSettings, Depends(get_settings)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 10,
):
    """Get all cost optimization data with pagination"""
    try:
        offset = (page - 1) * page_size

        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result, total = await services.cost_optimization_repo.get_all_cost_data(
                session,
                payload.filters,
                offset,
                page_size,
                payload.sort,
                payload.ids,
            )

            end_filters = [
                and_(
                    or_(
                        ResourceOwner.status != ResourceOwnerStatus.SUPRESSED,
                        ResourceOwner.status.is_(None),
                    ),
                    or_(
                        ResourceOwner.status != ResourceOwnerStatus.COMPLETED,
                        ResourceOwner.status.is_(None),
                    ),
                ),
            ]

            print("payload.ids", payload.ids)
            print("result", result)
            print("total", total)

            if payload.ids:
                end_filters.append(ResourceOwner.id.in_(payload.ids))

            total_potential_savings = (
                await services.cost_optimization_repo.get_aggregated_cost_data(
                    session,
                    payload.filters,
                    GroupByConfig(
                        fields=[],
                        aggregations={"unblended_cost": "sum"},
                    ),
                    end_filters=end_filters,
                )
            )

            end_filters = [
                and_(
                    or_(
                        ResourceOwner.status != ResourceOwnerStatus.SUPRESSED,
                        ResourceOwner.status.is_(None),
                    ),
                    ResourceOwner.status == ResourceOwnerStatus.COMPLETED,
                )
            ]

            if payload.ids:
                end_filters.append(ResourceOwner.id.in_(payload.ids))

            total_achieved_savings = (
                await services.cost_optimization_repo.get_aggregated_cost_data(
                    session,
                    payload.filters,
                    GroupByConfig(
                        fields=[],
                        aggregations={"unblended_cost": "sum"},
                    ),
                    end_filters=end_filters,
                )
            )

            # Calculate pagination metadata
            total_pages = (total + page_size - 1) // page_size
            next_page = page + 1 if page < total_pages else None
            prev_page = page - 1 if page > 1 else None

            return CostOptimizationPagination(
                data=result,
                cost_summary={
                    "total_potential_savings": total_potential_savings[0][
                        "unblended_cost_sum"
                    ]
                    if total_potential_savings
                    else 0,
                    "total_achieved_savings": total_achieved_savings[0][
                        "unblended_cost_sum"
                    ]
                    if total_achieved_savings
                    else 0,
                },
                pagination=PaginationMetadata(
                    total=total,
                    page=page,
                    page_size=page_size,
                    total_pages=total_pages,
                    next_page=next_page,
                    prev_page=prev_page,
                ),
            )
    except SQLAlchemyError as e:
        if '(1146, "Table' in e._message():
            return CostOptimizationPagination(
                error=ErrorResponse(
                    error_code=-1,
                    error_message="Data does not exist",
                ),
                data=[],
                cost_summary={},
                pagination=PaginationMetadata(
                    total=0, page=0, total_pages=0, next_page=None, prev_page=None
                ),
            )
        else:
            import traceback

            print("Unknown error: >")
            print(traceback.format_exc())
            print("Unknown error: <")

            return CostOptimizationPagination(
                error=ErrorResponse(
                    error_code=-2,
                    error_message="Internal Server Error",
                ),
                data=[],
                cost_summary={},
                pagination=PaginationMetadata(
                    total=0, page=0, total_pages=0, next_page=None, prev_page=None
                ),
            )


@router.post("/filter-facets")
async def get_filter_facets(
    user: Annotated[User, Depends(get_current_user)],
    filters: FilterFacetsFilter,
    settings: Annotated[MetaSettings, Depends(get_settings)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Get the unique values of each column for the filters"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.cost_optimization_repo.get_filter_facets(
                session, filters.filters
            )

            return result
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/notify-owners")
async def notify_owners(
    payload: CostOptimizationNotificationPayload,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Notify owners of cost optimization data"""

    result = await get_all_cost_data(
        payload=CostOptimizationFilterWithIds(
            filters=payload.filters.filters,
            sort=payload.filters.sort,
            group_by=payload.filters.group_by,
            ids=payload.ids,
        ),
        user=user,
        settings=settings,
        services=services,
        page=1,  # Get first page
        page_size=100,  # Get a reasonable batch size
    )

    if result.error:
        return {"message": f"Error fetching data: {result.error.error_message}"}

    if not result.data:
        return {"message": "No data to send"}

    def calculate_group_savings(resources: List[CostOptimizeWithResourceOwner]):
        """Calculate potential and achieved savings based on resource status."""
        potential_savings = 0
        achieved_savings = 0

        for r in resources:
            savings = r.unblended_cost or 0

            # Skip if no savings potential
            if savings <= 0:
                continue

            # Calculate based on status
            if not r.status or r.status in ["TODO", "WIP"]:
                # No status, TODO, or WIP count towards potential savings
                potential_savings += savings
            elif r.status == "COMPLETED":
                # Completed resources count as achieved savings
                achieved_savings += savings
            elif r.status == "SUPRESSED":
                # Suppressed resources are excluded from both calculations
                continue

        return {
            "total_potential_savings": potential_savings,
            "total_achieved_savings": achieved_savings,
        }

    # If specific email is provided, send all data to that email
    if payload.email:
        group_summary = calculate_group_savings(result.data)

        return await send_cost_optimization_email(
            email=payload.email,
            resources=result.data,
            cost_summary=group_summary,
            services=services,
        )

    # Otherwise, group by owner email and send separate emails
    owner_groups: Dict[str, List[CostOptimizeWithResourceOwner]] = {}
    for resource in result.data:
        if not resource.owner_email:
            continue
        if resource.owner_email not in owner_groups:
            owner_groups[resource.owner_email] = []
        owner_groups[resource.owner_email].append(resource)

    if not owner_groups:
        return {"message": "No owner emails found in the resources"}

    # Send email to each owner group
    responses = []
    for owner_email, resources in owner_groups.items():
        # Calculate status-based summaries for this group
        group_summary = calculate_group_savings(resources)

        response = await send_cost_optimization_email(
            email=owner_email,
            resources=resources,
            cost_summary=group_summary,
            services=services,
        )
        responses.append(response)

    return {
        "message": f"Notifications sent to {len(responses)} owners",
        "details": responses,
    }


async def send_cost_optimization_email(
    email: str,
    resources: List[CostOptimizeWithResourceOwner],
    cost_summary: dict,
    services: Metaservices,
):
    """Helper function to send cost optimization email."""

    html_body = (
        """
    <html>
    <head>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 20px;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            .money {
                text-align: right;
            }
            .summary {
                margin-bottom: 20px;
                padding: 10px;
                background-color: #f8f9fa;
                border: 1px solid #ddd;
                border-radius: 4px;
            }
        </style>
    </head>
    <body>
        <h2>Cost Optimization Recommendations</h2>
        <p>Dear Resource Owner,</p>
    """
        + f"""
        <div class="summary">
            <h3>Summary</h3>
            <p>Total Potential Savings: ${cost_summary.get("total_potential_savings", 0):,.2f}</p>
            <p>Total Achieved Savings: ${cost_summary.get("total_achieved_savings", 0):,.2f}</p>
        </div>
        
        <p>Below are the detailed cost optimization recommendations for your resources:</p>
        <table>
            <tr>
                <th>Resource ID</th>
                <th>Product Code</th>
                <th>Usage Account Name</th>
                <th>Usage Account ID</th>
                <th>Payer Account Name</th>
                <th>Payer Account ID</th>
                <th>Potential Savings ($)</th>
                <th>Achieved Savings ($)</th>
            </tr>
    """
    )

    # Add rows for each resource
    for resource in resources:
        html_body += f"""
            <tr>
                <td>{resource.resource_id}</td>
                <td>{resource.product_code}</td>
                <td>{resource.usage_account_name}</td>
                <td>{resource.usage_account_id}</td>
                <td>{resource.payer_account_name}</td>
                <td>{resource.payer_account_id}</td>
                <td class="money">${resource.potential_savings_usd:,.2f}</td>
                <td class="money">${resource.achieved_savings_usd:,.2f}</td>
            </tr>
        """

    html_body += """
        </table>
        <p>Please review these recommendations and take appropriate action to optimize costs.</p>
        <p>Best regards,<br>Cost Optimization Team</p>
    </body>
    </html>
    """

    # Create plain text version
    plain_text = f"""
Cost Optimization Recommendations

Dear Resource Owner,

Summary:
Total Potential Savings: ${cost_summary.get("total_potential_savings", 0):,.2f}
Total Achieved Savings: ${cost_summary.get("total_achieved_savings", 0):,.2f}

Please check the HTML version of this email for detailed resource information.

Best regards,
Cost Optimization Team
    """

    # Send email
    await services.email.send_email(
        to=email,
        subject="Cost Optimization Recommendations",
        body=plain_text,
        html_body=html_body,
    )

    return {"email": email, "resources_count": len(resources)}
