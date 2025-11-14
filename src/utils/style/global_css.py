# ============================================================================
# FILE: utils/style/global_css.py
# Global CSS Injection
# ============================================================================


def inject_global_css() -> tuple[str, bool]:
    """Inject global dashboard CSS"""
    css = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    
    * {
        font-family: 'Plus Jakarta Sans Semibold', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background: white;
    }
    
    .main .block-container {
        padding: 2rem 3rem;
        max-width: 1400px;
        background: rgba(255, 255, 255, 0.95);
        border-radius: 20px;
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
        margin: 2rem auto;
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
    }
    
    [data-testid="metric-container"] {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #e5e7eb;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px rgba(99, 102, 241, 0.4);
    }
    </style>
    """
    return css, True
