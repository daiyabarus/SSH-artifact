"""
Example Dashboard Implementation
Demonstrating usage of the utility files
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# Import utilities (sesuaikan dengan struktur folder Anda)
# from utils.text_styling import title, subtitle, section_title
from metric_cards import metric_card, progress_metric
# from utils.containers import info_box, divider, spacer
# from utils.global_css import inject_global_css
# from utils.theme import DashboardTheme

# ============================================================================
# SETUP PAGE
# ============================================================================

st.set_page_config(
    page_title="Sales Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject global CSS
# st.markdown(inject_global_css()[0], unsafe_allow_html=True)

# ============================================================================
# SAMPLE DATA GENERATION
# ============================================================================


@st.cache_data
def load_data():
    """Generate sample data"""
    dates = pd.date_range(end=datetime.now(), periods=30, freq="D")
    data = pd.DataFrame(
        {
            "date": dates,
            "revenue": [10000 + i * 500 + (i % 7) * 1000 for i in range(30)],
            "orders": [100 + i * 5 + (i % 7) * 10 for i in range(30)],
            "customers": [50 + i * 2 + (i % 7) * 5 for i in range(30)],
        }
    )
    return data


df = load_data()

# ============================================================================
# HEADER SECTION
# ============================================================================

# st.markdown(title("Sales Performance Dashboard")[0], unsafe_allow_html=True)
# st.markdown(subtitle("Real-time monitoring and analytics")[0], unsafe_allow_html=True)
# st.markdown(divider()[0], unsafe_allow_html=True)

st.title("📊 Sales Performance Dashboard")
st.markdown("Real-time monitoring and analytics")
st.divider()

# ============================================================================
# KEY METRICS ROW
# ============================================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_revenue = df["revenue"].sum()
    st.markdown(
        metric_card(
            label="Total Revenue",
            value=f"${total_revenue:,.0f}",
            delta="↑ 12.5%",
            delta_color="green",
            icon="💰",
        )[0],
        unsafe_allow_html=True,
    )
    st.metric("Total Revenue", f"${total_revenue:,.0f}", "↑ 12.5%")

with col2:
    total_orders = df["orders"].sum()
    # st.markdown(metric_card(
    #     label="Total Orders",
    #     value=f"{total_orders:,}",
    #     delta="↑ 8.3%",
    #     delta_color="green",
    #     icon="🛒"
    # )[0], unsafe_allow_html=True)
    st.metric("Total Orders", f"{total_orders:,}", "↑ 8.3%")

with col3:
    total_customers = df["customers"].sum()
    # st.markdown(metric_card(
    #     label="Total Customers",
    #     value=f"{total_customers:,}",
    #     delta="↑ 15.2%",
    #     delta_color="green",
    #     icon="👥"
    # )[0], unsafe_allow_html=True)
    st.metric("Total Customers", f"{total_customers:,}", "↑ 15.2%")

with col4:
    avg_order = total_revenue / total_orders
    # st.markdown(metric_card(
    #     label="Avg Order Value",
    #     value=f"${avg_order:.2f}",
    #     delta="↓ 2.1%",
    #     delta_color="red",
    #     icon="💳"
    # )[0], unsafe_allow_html=True)
    st.metric("Avg Order Value", f"${avg_order:.2f}", "↓ 2.1%")

# st.markdown(spacer(30)[0], unsafe_allow_html=True)

st.divider()

# ============================================================================
# CHARTS SECTION
# ============================================================================

# st.markdown(section_title("Revenue Trends", icon="📈")[0], unsafe_allow_html=True)

st.subheader("📈 Revenue Trends")

col1, col2 = st.columns(2)

with col1:
    # Revenue over time
    fig_revenue = px.line(
        df,
        x="date",
        y="revenue",
        title="Daily Revenue",
        labels={"revenue": "Revenue ($)", "date": "Date"},
    )
    fig_revenue.update_traces(line_color="#6366f1", line_width=3)
    fig_revenue.update_layout(
        template="plotly_white",
        font={"family": "Plus Jakarta Sans Semibold"},
        height=400,
    )
    st.plotly_chart(fig_revenue, width="stretch")

with col2:
    # Orders over time
    fig_orders = px.bar(
        df,
        x="date",
        y="orders",
        title="Daily Orders",
        labels={"orders": "Orders", "date": "Date"},
    )
    fig_orders.update_traces(marker_color="#8b5cf6")
    fig_orders.update_layout(
        template="plotly_white",
        font={"family": "Plus Jakarta Sans Semibold"},
        height=400,
    )
    st.plotly_chart(fig_orders, width="stretch")

# ============================================================================
# PROGRESS METRICS SECTION
# ============================================================================

st.divider()
# st.markdown(section_title("Monthly Goals", icon="🎯")[0], unsafe_allow_html=True)

st.subheader("🎯 Monthly Goals")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        progress_metric(
            label="Revenue Target", value=total_revenue, total=500000, color="#6366f1"
        )[0],
        unsafe_allow_html=True,
    )
    st.progress(total_revenue / 500000)
    st.caption(f"Revenue Target: ${total_revenue:,.0f} / $500,000")

with col2:
    # st.markdown(progress_metric(
    #     label="Orders Target",
    #     value=total_orders,
    #     total=5000,
    #     color="#10b981"
    # )[0], unsafe_allow_html=True)
    st.progress(total_orders / 5000)
    st.caption(f"Orders Target: {total_orders:,} / 5,000")

with col3:
    # st.markdown(progress_metric(
    #     label="Customer Target",
    #     value=total_customers,
    #     total=2000,
    #     color="#f59e0b"
    # )[0], unsafe_allow_html=True)
    st.progress(total_customers / 2000)
    st.caption(f"Customer Target: {total_customers:,} / 2,000")

# ============================================================================
# INFO BOXES SECTION
# ============================================================================

st.divider()

col1, col2 = st.columns(2)

with col1:
    # st.markdown(info_box(
    #     content="<strong>Great Performance!</strong> Revenue is up 12.5% compared to last month.",
    #     box_type="success",
    #     icon="✅"
    # )[0], unsafe_allow_html=True)
    st.success("✅ **Great Performance!** Revenue is up 12.5% compared to last month.")

with col2:
    # st.markdown(info_box(
    #     content="<strong>Action Required:</strong> Average order value decreased by 2.1%. Consider upselling strategies.",
    #     box_type="warning",
    #     icon="⚠️"
    # )[0], unsafe_allow_html=True)
    st.warning(
        "⚠️ **Action Required:** Average order value decreased by 2.1%. Consider upselling strategies."
    )

# ============================================================================
# DATA TABLE SECTION
# ============================================================================

st.divider()
# st.markdown(section_title("Recent Transactions", icon="📋")[0], unsafe_allow_html=True)

st.subheader("📋 Recent Transactions")

# Display last 10 days
recent_df = df.tail(10).copy()
recent_df["date"] = recent_df["date"].dt.strftime("%Y-%m-%d")
recent_df = recent_df.rename(
    columns={
        "date": "Date",
        "revenue": "Revenue ($)",
        "orders": "Orders",
        "customers": "Customers",
    }
)

st.dataframe(recent_df, width="stretch", hide_index=True)

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

with st.sidebar:
    st.title("🔧 Dashboard Controls")

    # Date range filter
    st.subheader("Date Range")
    date_range = st.date_input(
        "Select period",
        value=(df["date"].min(), df["date"].max()),
        min_value=df["date"].min(),
        max_value=df["date"].max(),
    )

    st.divider()

    # Additional filters
    st.subheader("Filters")
    show_trend = st.checkbox("Show trend lines", value=True)
    show_annotations = st.checkbox("Show annotations", value=False)

    st.divider()

    # Export options
    st.subheader("Export Data")
    if st.button("📥 Download CSV", width="stretch"):
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download", data=csv, file_name="dashboard_data.csv", mime="text/csv"
        )

    st.divider()

    # Info
    st.caption("Dashboard v1.0")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ============================================================================
# FOOTER
# ============================================================================

st.divider()
st.caption("© 2024 Sales Dashboard. All rights reserved.")
