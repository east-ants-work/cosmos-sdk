"""
Dataset SDK 예외 클래스.
"""

from __future__ import annotations


class DatasetError(Exception):
    """Dataset SDK 기본 예외."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class AuthError(DatasetError):
    """인증 실패 (401)."""


class PermissionError(DatasetError):
    """권한 없음 (403)."""


class NotFoundError(DatasetError):
    """리소스 없음 (404)."""


class BatchError(DatasetError):
    """배치 작업 부분 실패."""

    def __init__(
        self,
        message: str,
        rows_completed: int = 0,
        total_rows: int = 0,
        status_code: int | None = None,
    ):
        super().__init__(message, status_code=status_code)
        self.rows_completed = rows_completed
        self.total_rows = total_rows
