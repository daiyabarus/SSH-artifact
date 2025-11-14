# ============================================================================
# FILE: utils/style/chart_styling.py
# Chart Styling Utilities
# ============================================================================


def chart_container(title: str = "", height: int = 400) -> dict:
    """Standard chart container config"""
    return {
        "title": title,
        "height": height,
        "template": "plotly_white",
        "font": {"family": "Plus Jakarta Sans Semibold"},
        "paper_bgcolor": "white",
        "plot_bgcolor": "white",
        "margin": {"t": 60, "b": 60, "l": 60, "r": 60},
    }


def chart_colors() -> dict:
    """Standard color palette for charts"""
    return {
        "primary": ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981"],
        "sequential": ["#eff6ff", "#dbeafe", "#bfdbfe", "#93c5fd", "#3b82f6"],
        "diverging": ["#ef4444", "#f59e0b", "#fbbf24", "#a3e635", "#10b981"],
        "categorical": [
            "#6366f1",
            "#8b5cf6",
            "#ec4899",
            "#f59e0b",
            "#10b981",
            "#06b6d4",
        ],
    }
