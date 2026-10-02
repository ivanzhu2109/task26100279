"""Incremental cross-source fusion of fleet telemetry (speed/temp) on SQLite."""

from .engine import Fusion, Record

__all__ = ["Fusion", "Record"]
