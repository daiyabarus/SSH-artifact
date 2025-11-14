# ============================================================================
# FILE: utils/style/metric_cards.py
# Metric and KPI Card Components
# ============================================================================

from typing import Optional


def metric_card(
    label: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "green",
    icon: str = "",
    bg_color: str = "#ffffff",
    border_color: str = "#e5e7eb",
) -> tuple[str, bool]:
    """Styled metric card with optional delta"""
    delta_colors = {
        "green": "#10b981",
        "red": "#ef4444",
        "blue": "#3b82f6",
        "gray": "#6b7280",
    }

    delta_html = ""
    if delta:
        d_color = delta_colors.get(delta_color, delta_colors["green"])
        delta_html = f'<div style="color: {d_color}; font-size: 14px; margin-top: 8px; font-weight: 600;">{delta}</div>'

    icon_html = (
        f'<span style="font-size: 24px; margin-bottom: 10px; display: block;">{icon}</span>'
        if icon
        else ""
    )

    card_html = f"""
    <div style="
        background: {bg_color};
        padding: 24px;
        border-radius: 12px;
        border: 1px solid {border_color};
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
    ">
        {icon_html}
        <div style="color: #6b7280; font-size: 14px; font-weight: 600; margin-bottom: 8px; font-family: Plus Jakarta Sans Semibold;">
            {label}
        </div>
        <div style="color: #1f2937; font-size: 32px; font-weight: 700; font-family: Plus Jakarta Sans Semibold;">
            {value}
        </div>
        {delta_html}
    </div>
    """
    return card_html, True


def simple_metric(label: str, value: str, align: str = "center") -> tuple[str, bool]:
    """Simple metric without card"""
    html = f"""
    <div style="text-align: {align}; padding: 16px;">
        <div style="color: #6b7280; font-size: 12px; font-weight: 600; margin-bottom: 4px; font-family: Plus Jakarta Sans Semibold;">
            {label}
        </div>
        <div style="color: #1f2937; font-size: 28px; font-weight: 700; font-family: Plus Jakarta Sans Semibold;">
            {value}
        </div>
    </div>
    """
    return html, True


def progress_metric(
    label: str, value: float, total: float, color: str = "#6366f1"
) -> tuple[str, bool]:
    """Metric with progress bar"""
    percentage = (value / total * 100) if total > 0 else 0

    html = f"""
    <div style="padding: 16px; background: #ffffff; border-radius: 8px; border: 1px solid #e5e7eb;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span style="color: #6b7280; font-size: 14px; font-weight: 600; font-family: Plus Jakarta Sans Semibold;">{label}</span>
            <span style="color: #1f2937; font-size: 14px; font-weight: 700; font-family: Plus Jakarta Sans Semibold;">{value}/{total}</span>
        </div>
        <div style="background: #f3f4f6; border-radius: 9999px; height: 8px; overflow: hidden;">
            <div style="background: {color}; height: 100%; width: {percentage}%; transition: width 0.3s ease;"></div>
        </div>
    </div>
    """
    return html, True
