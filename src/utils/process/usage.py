from utils.data_processing import DataProcessor, calc_pct
from utils.query_builder import QueryBuilder

# Calculate margin
df = calc_pct(df, "profit", "revenue", "margin_pct")

# Save to SQLite
qb = QueryBuilder("dashboard.db")
qb.from_dataframe(df, "sales")

# Query
result = qb.to_dataframe("SELECT * FROM sales WHERE margin_pct > 20")
