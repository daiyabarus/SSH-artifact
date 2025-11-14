# ============================================================================
# FILE: utils/style/theme.py
# Global Theme Configuration
# ============================================================================


class DashboardTheme:
    """Centralized theme configuration"""

    COLORS = {
        "primary": "#f8f8fc",
        "secondary": "#dbdadf",
        "success": "#10b981",
        "warning": "#f59e0b",
        "danger": "#ef4444",
        "info": "#3b82f6",
        "dark": "#1f2937",
        "light": "#f9fafb",
        "border": "#e5e7eb",
        "text": "#1f2937",
        "text_muted": "#6b7280",
    }

    FONTS = {
        "primary": "Plus Jakarta Sans Semibold",
        "fallback": "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    }

    SPACING = {"xs": "8px", "sm": "12px", "md": "16px", "lg": "24px", "xl": "32px"}

    BORDER_RADIUS = {"sm": "4px", "md": "8px", "lg": "12px", "xl": "16px"}
