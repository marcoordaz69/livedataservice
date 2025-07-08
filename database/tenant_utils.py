#!/usr/bin/env python3
"""
Utility functions for tenant context management.
"""

import contextvars
import logging
import uuid
from typing import Optional

logger = logging.getLogger(__name__)

# Context variable to store the current tenant ID
current_user_id_var = contextvars.ContextVar('current_user_id', default=None)

def set_current_user_id(user_id: uuid.UUID) -> None:
    """Set the current user ID in the context."""
    if not isinstance(user_id, uuid.UUID) and user_id is not None:
        try:
            user_id = uuid.UUID(str(user_id))
        except (ValueError, TypeError):
            logger.error(f"Invalid user_id format: {user_id} ({type(user_id)})")
            raise TypeError(f"user_id must be UUID, got {type(user_id)}")
    current_user_id_var.set(user_id)

def get_current_user_id() -> Optional[uuid.UUID]:
    """Get the current user ID from the context."""
    return current_user_id_var.get()

def clear_current_user_id() -> None:
    """Clear the current user ID from the context."""
    current_user_id_var.set(None)

async def set_tenant_context_db(conn, user_id: Optional[uuid.UUID]) -> None:
    """Set the tenant context in the database connection."""
    if user_id is None:
        # Skip setting tenant context when no user ID is provided
        # This allows operations to proceed with NULL user_id
        pass
    else:
        # Use set_config directly instead of a wrapper function
        await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)") 