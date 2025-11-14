# ============================================================================
# FILE: utils/style/containers.py
# Container and Layout Components
# ============================================================================
from typing import Literal


def card_container(
    content: str, padding: str = "24px", bg_color: str = "#ffffff"
) -> tuple[str, bool]:
    """Basic card container"""
    html = f"""
    <div style="
        background: {bg_color};
        padding: {padding};
        border-radius: 12px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 20px;
    ">
        {content}
    </div>
    """
    return html, True


def info_box(
    content: str,
    box_type: Literal["info", "success", "warning", "error"] = "info",
    icon: str = "",
) -> tuple[str, bool]:
    """Information box with different types"""
    styles = {
        "info": {"bg": "#eff6ff", "border": "#3b82f6", "text": "#1e40af"},
        "success": {"bg": "#f0fdf4", "border": "#10b981", "text": "#065f46"},
        "warning": {"bg": "#fffbeb", "border": "#f59e0b", "text": "#92400e"},
        "error": {"bg": "#fef2f2", "border": "#ef4444", "text": "#991b1b"},
    }

    style = styles.get(box_type, styles["info"])
    icon_html = f'<span style="margin-right: 8px;">{icon}</span>' if icon else ""

    html = f"""
    <div style="
        background: {style["bg"]};
        border-left: 4px solid {style["border"]};
        padding: 16px 20px;
        border-radius: 8px;
        margin: 16px 0;
        color: {style["text"]};
        font-family: Plus Jakarta Sans Semibold;
    ">
        {icon_html}{content}
    </div>
    """
    return html, True


def divider(
    height: int = 1, color: str = "#e5e7eb", margin: str = "20px 0"
) -> tuple[str, bool]:
    """Horizontal divider"""
    html = f'<div style="height: {height}px; background: {color}; margin: {margin};"></div>'
    return html, True


def spacer(height: int = 20) -> tuple[str, bool]:
    """Vertical spacer"""
    html = f'<div style="height: {height}px;"></div>'
    return html, True
