# ============================================================================
# FILE: utils/style/table_styling.py
# Table Styling Utilities
# ============================================================================


def styled_dataframe_css() -> tuple[str, bool]:
    """CSS for styled dataframes"""
    css = """
    <style>
    .styled-table {
        border-collapse: separate;
        border-spacing: 0;
        width: 100%;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    
    .styled-table thead tr {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        text-align: left;
        font-family: Plus Jakarta Sans Semibold;
    }
    
    .styled-table th {
        padding: 16px;
        font-weight: 600;
        font-size: 14px;
    }
    
    .styled-table td {
        padding: 12px 16px;
        border-bottom: 1px solid #e5e7eb;
        font-family: Plus Jakarta Sans Semibold;
    }
    
    .styled-table tbody tr {
        background: white;
        transition: background 0.2s ease;
    }
    
    .styled-table tbody tr:hover {
        background: #f9fafb;
    }
    
    .styled-table tbody tr:last-child td {
        border-bottom: none;
    }
    </style>
    """
    return css, True


def table_cell_color(
    value: float,
    min_val: float,
    max_val: float,
    color_start: str = "#ef4444",
    color_end: str = "#10b981",
) -> str:
    """Generate background color based on value range"""
    if max_val == min_val:
        return "#f3f4f6"

    # Normalize value between 0 and 1
    normalized = (value - min_val) / (max_val - min_val)

    # Simple gradient (you can enhance this)
    if normalized < 0.5:
        return "#fef2f2"  # Light red
    else:
        return "#f0fdf4"  # Light green
