"""Read-only Streamlit dashboard for the e-commerce analytics mart."""

from .config import DashboardConfig, load_dashboard_config
from .data import DashboardSnapshot, load_dashboard_snapshot

__all__ = [
    "DashboardConfig",
    "DashboardSnapshot",
    "load_dashboard_config",
    "load_dashboard_snapshot",
]
