"""
Role-Based Access Control (RBAC) module for Web UI.

Implements three roles:
- Admin: Full system access (read, write, configure, manage users, start/stop, delete)
- Operator: Can view and control sources (read, write, start/stop)
- Viewer: Read-only access

NIS2 Compliance: Article 21.2(g) - Access Control
"""

import logging
from enum import Enum
from functools import wraps
from typing import List, Dict, Any, Set

from aiohttp import web

logger = logging.getLogger(__name__)


class Permission(Enum):
    """System permissions."""
    READ = "read"                    # View sources, configs, metrics
    WRITE = "write"                  # Modify source configurations
    CONFIGURE = "configure"          # Modify ZeroBus/system configuration
    MANAGE_USERS = "manage_users"    # Manage user roles (future)
    START_STOP = "start_stop"        # Start/stop sources and bridge
    DELETE = "delete"                # Delete sources


class Role(Enum):
    """User roles with associated permissions."""
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"


# Role to permissions mapping
ROLE_PERMISSIONS: Dict[Role, List[Permission]] = {
    Role.ADMIN: [
        Permission.READ,
        Permission.WRITE,
        Permission.CONFIGURE,
        Permission.MANAGE_USERS,
        Permission.START_STOP,
        Permission.DELETE
    ],
    Role.OPERATOR: [
        Permission.READ,
        Permission.WRITE,
        Permission.START_STOP
    ],
    Role.VIEWER: [
        Permission.READ
    ]
}


class User:
    """
    User with role and permissions.

    Determines role from OAuth groups and role mappings.
    """

    def __init__(
        self,
        email: str,
        name: str = None,
        groups: List[str] = None,
        role_mappings: Dict[str, str] = None,
        default_role: str = "viewer"
    ):
        """
        Initialize user with role determination.

        Args:
            email: User email address
            name: User display name
            groups: List of OAuth groups user belongs to
            role_mappings: Dict mapping group names to role names
            default_role: Default role if no group matches (default: viewer)
        """
        self.email = email
        self.name = name or email
        self.groups = groups or []
        self.role = self._determine_role(groups or [], role_mappings or {}, default_role)

        logger.info(f"User {email} assigned role: {self.role.value} (groups={self.groups})")

    def _determine_role(
        self,
        groups: List[str],
        role_mappings: Dict[str, str],
        default_role: str
    ) -> Role:
        """
        Determine user's role from their OAuth groups.

        Checks groups against role_mappings. First match wins.
        Falls back to default_role if no matches.

        Args:
            groups: List of OAuth groups
            role_mappings: Dict mapping group names to role names
            default_role: Fallback role name

        Returns:
            Role enum
        """
        # Check each group against mappings
        for group in groups:
            if group in role_mappings:
                role_name = role_mappings[group]
                try:
                    role = Role[role_name.upper()]
                    logger.debug(f"Matched group '{group}' to role '{role.value}'")
                    return role
                except KeyError:
                    logger.warning(f"Invalid role name in mapping: {role_name}")
                    continue

        # No match, use default role
        try:
            role = Role[default_role.upper()]
            logger.debug(f"Using default role: {role.value}")
            return role
        except KeyError:
            logger.warning(f"Invalid default role: {default_role}, using VIEWER")
            return Role.VIEWER

    def has_permission(self, permission: Permission) -> bool:
        """
        Check if user has a specific permission.

        Args:
            permission: Permission to check

        Returns:
            True if user has permission, False otherwise
        """
        return permission in ROLE_PERMISSIONS[self.role]

    def has_any_permission(self, permissions: List[Permission]) -> bool:
        """
        Check if user has any of the given permissions.

        Args:
            permissions: List of permissions to check

        Returns:
            True if user has at least one permission, False otherwise
        """
        user_permissions = ROLE_PERMISSIONS[self.role]
        return any(perm in user_permissions for perm in permissions)

    def has_all_permissions(self, permissions: List[Permission]) -> bool:
        """
        Check if user has all of the given permissions.

        Args:
            permissions: List of permissions to check

        Returns:
            True if user has all permissions, False otherwise
        """
        user_permissions = ROLE_PERMISSIONS[self.role]
        return all(perm in user_permissions for perm in permissions)

    def get_permissions(self) -> List[str]:
        """
        Get list of user's permissions as strings.

        Returns:
            List of permission names
        """
        return [perm.value for perm in ROLE_PERMISSIONS[self.role]]

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert user to dictionary for JSON serialization.

        Returns:
            User data dictionary
        """
        return {
            'email': self.email,
            'name': self.name,
            'groups': self.groups,
            'role': self.role.value,
            'permissions': self.get_permissions()
        }


def require_permission(permission: Permission):
    """
    Decorator to enforce permission on route handlers.

    Usage:
        @require_permission(Permission.WRITE)
        async def update_source(self, request):
            # Only users with WRITE permission can access
            ...

    Args:
        permission: Required permission

    Returns:
        Decorator function
    """
    def decorator(handler):
        @wraps(handler)
        async def wrapper(self, request):
            # Skip permission check if authentication is disabled
            auth_manager = request.app.get('auth_manager')
            if not auth_manager or not auth_manager.enabled:
                return await handler(self, request)

            # Get user from request (set by auth_middleware)
            user = request.get('user')

            if not user:
                logger.warning(f"Permission check failed: No user in request for {request.path}")
                raise web.HTTPUnauthorized(reason="Authentication required")

            if not user.has_permission(permission):
                logger.warning(
                    f"Permission denied: {user.email} (role={user.role.value}) "
                    f"lacks {permission.value} for {request.path}",
                    extra={
                        'user': user.email,
                        'action': 'access',
                        'result': 'denied',
                        'target': request.path,
                        'required_permission': permission.value,
                        'user_role': user.role.value,
                        'event_type': 'authorization'
                    }
                )
                raise web.HTTPForbidden(
                    reason=f"Permission '{permission.value}' required (you have role '{user.role.value}')"
                )

            # Permission granted, log for audit
            logger.debug(
                f"Permission granted: {user.email} (role={user.role.value}) "
                f"accessing {request.path}",
                extra={
                    'user': user.email,
                    'action': 'access',
                    'result': 'allowed',
                    'target': request.path,
                    'permission': permission.value,
                    'event_type': 'authorization'
                }
            )

            return await handler(self, request)
        return wrapper
    return decorator


def require_any_permission(*permissions: Permission):
    """
    Decorator to require at least one of multiple permissions.

    Usage:
        @require_any_permission(Permission.WRITE, Permission.CONFIGURE)
        async def some_operation(self, request):
            # User needs either WRITE or CONFIGURE permission
            ...

    Args:
        permissions: Required permissions (at least one)

    Returns:
        Decorator function
    """
    def decorator(handler):
        @wraps(handler)
        async def wrapper(self, request):
            user = request.get('user')

            if not user:
                raise web.HTTPUnauthorized(reason="Authentication required")

            if not user.has_any_permission(list(permissions)):
                perm_names = [p.value for p in permissions]
                logger.warning(
                    f"Permission denied: {user.email} lacks any of {perm_names}",
                    extra={
                        'user': user.email,
                        'action': 'access',
                        'result': 'denied',
                        'target': request.path,
                        'event_type': 'authorization'
                    }
                )
                raise web.HTTPForbidden(
                    reason=f"One of these permissions required: {', '.join(perm_names)}"
                )

            return await handler(self, request)
        return wrapper
    return decorator


def require_role(role: Role):
    """
    Decorator to require specific role.

    Usage:
        @require_role(Role.ADMIN)
        async def admin_only_operation(self, request):
            # Only admins can access
            ...

    Args:
        role: Required role

    Returns:
        Decorator function
    """
    def decorator(handler):
        @wraps(handler)
        async def wrapper(self, request):
            # Skip role check if authentication is disabled
            auth_manager = request.app.get('auth_manager')
            if not auth_manager or not auth_manager.enabled:
                return await handler(self, request)

            user = request.get('user')

            if not user:
                raise web.HTTPUnauthorized(reason="Authentication required")

            if user.role != role:
                logger.warning(
                    f"Role check failed: {user.email} has {user.role.value}, requires {role.value}",
                    extra={
                        'user': user.email,
                        'action': 'access',
                        'result': 'denied',
                        'target': request.path,
                        'event_type': 'authorization'
                    }
                )
                raise web.HTTPForbidden(
                    reason=f"Role '{role.value}' required (you have '{user.role.value}')"
                )

            return await handler(self, request)
        return wrapper
    return decorator


def get_role_info() -> Dict[str, Any]:
    """
    Get information about all roles and their permissions.

    Useful for documentation and UI display.

    Returns:
        Dictionary with role information
    """
    return {
        role.value: {
            'name': role.value.capitalize(),
            'permissions': [p.value for p in ROLE_PERMISSIONS[role]],
            'description': _get_role_description(role)
        }
        for role in Role
    }


def _get_role_description(role: Role) -> str:
    """Get human-readable description of role."""
    descriptions = {
        Role.ADMIN: "Full system access - can configure everything, manage users, and perform all operations",
        Role.OPERATOR: "Can view, modify, and control sources - cannot change system configuration",
        Role.VIEWER: "Read-only access - can view sources, metrics, and status but cannot make changes"
    }
    return descriptions.get(role, "")


# Convenience functions for checking permissions in templates/UI
def can_read(user: User) -> bool:
    """Check if user can read (all roles can)."""
    return user.has_permission(Permission.READ)


def can_write(user: User) -> bool:
    """Check if user can write (admin, operator)."""
    return user.has_permission(Permission.WRITE)


def can_configure(user: User) -> bool:
    """Check if user can configure system (admin only)."""
    return user.has_permission(Permission.CONFIGURE)


def can_start_stop(user: User) -> bool:
    """Check if user can start/stop sources (admin, operator)."""
    return user.has_permission(Permission.START_STOP)


def can_delete(user: User) -> bool:
    """Check if user can delete (admin only)."""
    return user.has_permission(Permission.DELETE)


def is_admin(user: User) -> bool:
    """Check if user is admin."""
    return user.role == Role.ADMIN


def is_operator(user: User) -> bool:
    """Check if user is operator."""
    return user.role == Role.OPERATOR


def is_viewer(user: User) -> bool:
    """Check if user is viewer."""
    return user.role == Role.VIEWER
