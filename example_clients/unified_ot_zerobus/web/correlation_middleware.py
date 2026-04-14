"""
Correlation ID Middleware for Request Tracking.

Assigns a unique correlation ID to each HTTP request for tracing through logs.
Useful for debugging and security incident investigation.

NIS2 Compliance: Article 21.2(b) - Incident Handling
"""

import logging
from aiohttp import web
from typing import Callable, Awaitable

from unified_connector.core.structured_logging import (
    generate_correlation_id,
    set_correlation_id,
    clear_correlation_id,
    get_correlation_id
)

logger = logging.getLogger(__name__)


@web.middleware
async def correlation_id_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.Response]]
) -> web.Response:
    """
    Middleware that assigns a correlation ID to each request.

    The correlation ID:
    - Is generated for each new request
    - Can be provided by client via X-Correlation-ID header
    - Is added to response headers
    - Is stored in context for logging
    - Enables request tracing across distributed systems

    Args:
        request: HTTP request
        handler: Request handler

    Returns:
        HTTP response with X-Correlation-ID header
    """
    # Check if client provided correlation ID
    correlation_id = request.headers.get('X-Correlation-ID')

    # Generate new ID if not provided
    if not correlation_id:
        correlation_id = generate_correlation_id()

    # Store in context for logging
    set_correlation_id(correlation_id)

    # Store in request for handlers to access
    request['correlation_id'] = correlation_id

    try:
        # Process request
        response = await handler(request)

        # Add correlation ID to response headers
        response.headers['X-Correlation-ID'] = correlation_id

        return response

    except Exception as e:
        # Log error with correlation ID
        logger.error(
            f"Request failed with correlation_id={correlation_id}: {str(e)}",
            exc_info=True,
            extra={'correlation_id': correlation_id}
        )
        raise

    finally:
        # Clean up context
        clear_correlation_id()


def get_request_correlation_id(request: web.Request) -> str:
    """
    Get correlation ID from request.

    Args:
        request: HTTP request

    Returns:
        Correlation ID
    """
    return request.get('correlation_id', 'unknown')


def get_current_correlation_id() -> str:
    """
    Get correlation ID from current context.

    Returns:
        Correlation ID or 'none' if not in request context
    """
    return get_correlation_id() or 'none'
