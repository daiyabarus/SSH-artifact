# ============================================================================
# FILE: utils/style/text_styling.py
# Text and Typography Utilities
# ============================================================================

from typing import Literal


def styling(
    text: str,
    tag: Literal["h1", "h2", "h3", "h4", "h5", "h6", "p"] = "h2",
    text_align: Literal["left", "right", "center", "justify"] = "center",
    font_size: int = 32,
    font_family: str = "Plus Jakarta Sans Semibold",
    background_color: str = "transparent",
) -> tuple[str, bool]:
    """Base styling function for text elements"""
    style = f"text-align: {text_align}; font-size: {font_size}px; font-family: {font_family}; background-color: {background_color};"
    styled_text = f'<{tag} style="{style}">{text}</{tag}>'
    return styled_text, True


def title(text: str, align: str = "center", color: str = "#1f2937") -> tuple[str, bool]:
    """Dashboard title"""
    style = f"text-align: {align}; font-size: 48px; font-weight: 700; color: {color}; margin-bottom: 10px; font-family: Plus Jakarta Sans Semibold;"
    return f'<h1 style="{style}">{text}</h1>', True


def subtitle(
    text: str, align: str = "center", color: str = "#6b7280"
) -> tuple[str, bool]:
    """Dashboard subtitle"""
    style = f"text-align: {align}; font-size: 20px; color: {color}; margin-bottom: 30px; font-family: Plus Jakarta Sans Semibold;"
    return f'<p style="{style}">{text}</p>', True


def section_title(text: str, icon: str = "", align: str = "left") -> tuple[str, bool]:
    """Section header with optional icon"""
    style = f"text-align: {align}; font-size: 32px; font-weight: 600; color: #1f2937; margin: 30px 0 20px 0; font-family: Plus Jakarta Sans Semibold;"
    content = f"{icon} {text}" if icon else text
    return f'<h2 style="{style}">{content}</h2>', True


def text_label(
    text: str, size: int = 14, color: str = "#6b7280", bold: bool = False
) -> tuple[str, bool]:
    """Small text label"""
    weight = "600" if bold else "400"
    style = f"font-size: {size}px; color: {color}; font-weight: {weight}; font-family: Plus Jakarta Sans Semibold;"
    return f'<span style="{style}">{text}</span>', True


def highlight_text(
    text: str, bg_color: str = "#fef3c7", color: str = "#92400e"
) -> tuple[str, bool]:
    """Highlighted text with background"""
    style = f"background-color: {bg_color}; color: {color}; padding: 2px 8px; border-radius: 4px; font-family: Plus Jakarta Sans Semibold;"
    return f'<span style="{style}">{text}</span>', True
