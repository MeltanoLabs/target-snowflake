"""Test Configuration."""

from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    import pytest


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:  # noqa: ARG001
    """Session Finish."""
    import logging

    loggers: list[logging.Logger] = [
        logging.getLogger(),
        *list(logging.Logger.manager.loggerDict.values()),  # type: ignore[list-item]
    ]

    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
