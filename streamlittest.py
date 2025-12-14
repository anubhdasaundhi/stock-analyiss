import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------------------------------------
# MYSQL CONNECTION FUNCTION
# ------------------------------------------------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="bankONE",
        password="0701",
        database="stock"
    )

# ------------------------------------------------------------
# LOAD DATA FROM MYSQL
# ------------------------------------------------------------
@st.cache_data
def load_data():
    conn = get_connection()
    query = """
        SELECT Ticker, Company, Sector, trade_date,
               open_price, high_price, low_price, close_price, volume
        FROM stockprices
        ORDER BY Ticker, trade_date;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df

df = load_data()

# ------------------------------------------------------------
# STREAMLIT CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Nifty 50 Stock Dashboard", layout="wide")
st.title("📊 Nifty 50 Stock Analytics Dashboard")
st.write("A full dashboard including **Market Overview**, **Volatility**, "
         "**Returns**, **Cumulative Performance**, **Heatmaps**, and "
         "**Monthly Gainers/Losers** using MySQL + Streamlit.")

# ------------------------------------------------------------
# SIDEBAR NAVIGATION
# ------------------------------------------------------------
menu = st.sidebar.radio(
    "Select Analysis Section",
    [
        "🏦 Market Overview",
        "📉 Volatility Analysis",
        "📈 Returns Analysis",
        "📈 Cumulative Return Over Time",
        "🏭 Sector-wise Performance",
        "📊 Stock Price Correlation Heatmap",
        "📅 Month-wise Top 5 Gainers & Losers",
        "🏭 Sector Performance (Alternate Dashboard)"
    ]
)

# =============================================================
# 1️⃣ MARKET OVERVIEW
# =============================================================
if menu == "🏦 Market Overview":
    st.header("📌 Market Summary")

    df_sorted = df.sort_values(["Ticker", "trade_date"]).copy()
    df_sorted["yearly_return"] = df_sorted.groupby("Ticker")["close_price"].pct_change(periods=250)

    green = df_sorted[df_sorted["yearly_return"] > 0]["Ticker"].nunique()
    red = df_sorted[df_sorted["yearly_return"] <= 0]["Ticker"].nunique()

    avg_price = df["close_price"].mean()
    avg_volume = df["volume"].mean()

    col1, col2, col3 = st.columns(3)
    col1.metric("🟢 Green Stocks", green)
    col2.metric("🔴 Red Stocks", red)
    col3.metric("💰 Average Price", f"{avg_price:.2f}")

    st.metric("📦 Average Daily Volume", f"{avg_volume:,.0f}")

# =============================================================
# 2️⃣ VOLATILITY ANALYSIS
# =============================================================
elif menu == "📉 Volatility Analysis":

    st.header("📉 Volatility Analysis")

    tab1, tab2 = st.tabs(["🔝 Top 10 Volatile Stocks", "📊 Full Volatility Table"])

    df["daily_return"] = df.groupby("Ticker")["close_price"].pct_change()

    volatility_df = (
        df.groupby("Ticker")["daily_return"]
        .std()
        .reset_index()
        .rename(columns={"daily_return": "volatility"})
    )

    top10 = volatility_df.sort_values("volatility", ascending=False).head(10)

    with tab1:
        st.subheader("Top 10 Most Volatile Stocks")
        st.dataframe(top10)
        st.bar_chart(top10.set_index("Ticker")["volatility"])

    with tab2:
        st.subheader("Complete Volatility Dataset")
        st.dataframe(volatility_df)

# =============================================================
# 3️⃣ RETURNS ANALYSIS
# =============================================================
elif menu == "📈 Returns Analysis":

    st.header("📈 Returns Analysis")

    tab1, tab2, tab3 = st.tabs([
        "📈 Top Gainers / Losers",
        "📉 Cumulative Returns",
        "📅 Monthly Returns"
    ])

    # ---------- TOP 10 GAINERS / LOSERS ----------
    with tab1:
        st.subheader("Top 10 Gainers & Losers")

        latest = df.sort_values("trade_date").groupby("Ticker").tail(1)
        earliest = df.sort_values("trade_date").groupby("Ticker").head(1)

        merged = latest.merge(
            earliest[["Ticker", "close_price"]],
            on="Ticker",
            suffixes=("_latest", "_first")
        )

        merged["yearly_return"] = (
            merged["close_price_latest"] - merged["close_price_first"]
        ) / merged["close_price_first"]

        gainers = merged.sort_values("yearly_return", ascending=False).head(10)
        losers = merged.sort_values("yearly_return").head(10)

        col1, col2 = st.columns(2)

        col1.subheader("🟢 Top 10 Gainers")
        col1.dataframe(gainers)
        col1.bar_chart(gainers.set_index("Ticker")["yearly_return"])

        col2.subheader("🔴 Top 10 Losers")
        col2.dataframe(losers)
        col2.bar_chart(losers.set_index("Ticker")["yearly_return"])

    # ---------- CUMULATIVE RETURNS ----------
    with tab2:
        st.subheader("Cumulative Returns (Top 5 Performing Stocks)")

        # Compute daily and cumulative returns
        df = df.sort_values(["Ticker", "trade_date"])
        df["daily_return"] = df.groupby("Ticker")["close_price"].pct_change()
        df["cumulative_return"] = (1 + df["daily_return"]).groupby(df["Ticker"]).cumprod()

        # Find top 5 stocks based on last cumulative return
        last_values = (
            df.groupby("Ticker")["cumulative_return"]
            .last()
            .sort_values(ascending=False)
            .head(5)
        )
        top5_tickers = last_values.index.tolist()

        # Filter only top 5
        df_top5 = df[df["Ticker"].isin(top5_tickers)]

        # Create pivot table for line chart
        pivot = df_top5.pivot_table(index="trade_date", columns="Ticker", values="cumulative_return")

        # Remove all-NaN rows
        pivot = pivot.dropna(how="all")

        st.line_chart(pivot)

    # ---------- MONTHLY RETURNS ----------
    with tab3:
        st.subheader("Month-wise Top 5 Gainers & Losers")

        df["month"] = df["trade_date"].dt.to_period("M")
        monthly = df.groupby(["Ticker", "month"]).agg(
            open_price=("close_price", "first"),
            close_price=("close_price", "last")
        )

        monthly["monthly_return"] = (
            monthly["close_price"] - monthly["open_price"]
        ) / monthly["open_price"]

        months = sorted(monthly.index.get_level_values("month").unique())
        selected_month = st.selectbox("Select Month", months)

        month_data = monthly.loc[pd.IndexSlice[:, selected_month], :].reset_index()

        gainers = month_data.sort_values("monthly_return", ascending=False).head(5)
        losers = month_data.sort_values("monthly_return").head(5)

        col1, col2 = st.columns(2)
        col1.subheader("🟢 Gainers")
        col1.dataframe(gainers)
        col1.bar_chart(gainers.set_index("Ticker")["monthly_return"])

        col2.subheader("🔴 Losers")
        col2.dataframe(losers)
        col2.bar_chart(losers.set_index("Ticker")["monthly_return"])

# =============================================================
# 4️⃣ CUMULATIVE RETURN OVER TIME
# =============================================================
elif menu == "📈 Cumulative Return Over Time":
    st.header("📈 Cumulative Return Over Time (Top 5 Performing Stocks)")

    df = df.sort_values(["Ticker", "trade_date"])
    df["daily_return"] = df.groupby("Ticker")["close_price"].pct_change()

    df["cumulative_return"] = (1 + df["daily_return"]).groupby(df["Ticker"]).cumprod()

    final_cum = (
        df.dropna(subset=["cumulative_return"])
          .groupby("Ticker")["cumulative_return"]
          .last()
          .sort_values(ascending=False)
    )

    top5 = final_cum.head(5)
    st.dataframe(top5.rename("Final Cumulative Return"))

    top5_df = df[df["Ticker"].isin(top5.index)]

    pivot = top5_df.pivot_table(index="trade_date", columns="Ticker", values="cumulative_return")
    st.line_chart(pivot)

# =============================================================
# 5️⃣ SECTOR-WISE PERFORMANCE (FIRST DASHBOARD)
# =============================================================
elif menu == "🏭 Sector-wise Performance":
    st.header("🏭 Sector-wise Average Yearly Return")

    df_sorted = df.sort_values(["Ticker", "trade_date"])
    earliest = df_sorted.groupby("Ticker").head(1)
    latest = df_sorted.groupby("Ticker").tail(1)

    merged = latest.merge(
        earliest[["Ticker", "close_price"]],
        on="Ticker",
        suffixes=("_latest", "_first")
    )

    merged["yearly_return"] = (
        merged["close_price_latest"] - merged["close_price_first"]
    ) / merged["close_price_first"]

    sector_perf = merged.groupby("Sector")["yearly_return"].mean().sort_values(ascending=False)

    st.dataframe(sector_perf.reset_index())
    st.bar_chart(sector_perf)

# =============================================================
# 6️⃣ CORRELATION HEATMAP
# =============================================================
elif menu == "📊 Stock Price Correlation Heatmap":

    st.header("📊 Stock Price Correlation Heatmap")

    # Pivot: rows = dates, columns = tickers, values = close prices
    price_matrix = df.pivot_table(index="trade_date", columns="Ticker", values="close_price", aggfunc="last")

    # Compute daily returns
    returns = price_matrix.pct_change().dropna()

    # ✅ Compute correlation using pandas.DataFrame.corr()
    correlation_matrix = returns.corr(method="pearson")  
    # Display correlation table
    st.dataframe(correlation_matrix)
    

    # Plot heatmap
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.heatmap(
        correlation_matrix,
        cmap="coolwarm",
        center=0,
        linewidths=0.5,
        annot=False
    )
    st.pyplot(fig)
# =============================================================
# 7️⃣ MONTH-WISE GAINERS & LOSERS
# =============================================================
elif menu == "📅 Month-wise Top 5 Gainers & Losers":

    st.header("📅 Month-wise Top 5 Gainers & Losers")

    df["month"] = df["trade_date"].dt.to_period("M")

    monthly = df.groupby(["Ticker", "month"]).agg(
        open_price=("open_price", "first"),
        close_price=("close_price", "last")
    )

    monthly["monthly_return"] = (
        monthly["close_price"] - monthly["open_price"]
    ) / monthly["open_price"]

    months = sorted(monthly.index.get_level_values("month").unique())
    selected_month = st.selectbox("Select Month", months)

    month_data = monthly.loc[pd.IndexSlice[:, selected_month], :].reset_index()

    gainers = month_data.sort_values("monthly_return", ascending=False).head(5)
    losers = month_data.sort_values("monthly_return").head(5)

    col1, col2 = st.columns(2)
    col1.subheader("🟢 Gainers")
    col1.dataframe(gainers)
    col1.bar_chart(gainers.set_index("Ticker")["monthly_return"])

    col2.subheader("🔴 Losers")
    col2.dataframe(losers)
    col2.bar_chart(losers.set_index("Ticker")["monthly_return"])

# =============================================================
# 8️⃣ SECTOR PERFORMANCE ALT
# =============================================================
elif menu == "🏭 Sector Performance (Alternate Dashboard)":

    st.header("🏭 Sector Performance Overview")

    latest = df.sort_values("trade_date").groupby("Ticker").tail(1)
    earliest = df.sort_values("trade_date").groupby("Ticker").head(1)

    merged = latest.merge(
        earliest[["Ticker", "close_price"]],
        on="Ticker",
        suffixes=("_latest", "_first")
    )

    merged["yearly_return"] = (
        merged["close_price_latest"] - merged["close_price_first"]
    ) / merged["close_price_first"]

    sector_perf = merged.groupby("Sector")["yearly_return"].mean().sort_values(ascending=False)

    st.dataframe(sector_perf.reset_index())
    st.bar_chart(sector_perf)
