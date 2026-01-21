from datetime import datetime, timedelta

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots

from webapp.filter import SidebarFilter
from webapp.read import get_annual_new_units
from webapp.utils import pastel_colors, add_pie_slices, apply_default_theme


st.set_page_config(page_title="Resale Trends", layout="wide")


@st.cache_data
def get_median_resale_data(df: pl.DataFrame):
    return (
        df.group_by("quarter_label")
        .agg(
            pl.median("psf").alias("median_psf"),
            pl.max("resale_price").alias("max_price"),
            pl.median("resale_price").alias("median_price"),
            pl.len().alias("txn_count"),
        )
        .sort("quarter_label")
    )


@st.cache_data
def get_lease_years_data(df: pl.DataFrame):
    return (
        df.group_by(["quarter_label", "cat_remaining_lease_years"])
        .agg(
            pl.median("resale_price").alias("median_resale_price"),
            pl.median("psf").alias("median_psf"),
            pl.len().alias("transaction_volume"),
        )
        .sort(["cat_remaining_lease_years", "quarter_label"])
        .sort(by="quarter_label")
    )


@st.cache_data
def get_town_data(df: pl.DataFrame):
    return (
        df.group_by(["quarter_label", "town"])
        .agg(
            pl.median("resale_price").alias("resale_price"),
            pl.median("psf").alias("psf"),
            pl.count("resale_price").alias("transaction_volume"),
        )
        .sort(["town", "quarter_label"])
    )


@st.cache_data
def get_flat_type_data(df: pl.DataFrame):
    all_flat_types = df["flat_type"].unique().sort()
    return (
        df.select("quarter_label")
        .unique()
        .join(pl.DataFrame({"flat_type": all_flat_types}), how="cross")
        .join(
            df.group_by(["quarter_label", "flat_type"]).agg(
                pl.median("resale_price").alias("resale_price"),
                pl.median("psf").alias("psf"),
                pl.len().alias("transaction_volume"),
            ),
            on=["quarter_label", "flat_type"],
            how="left",
        )
        .sort(["flat_type", "quarter_label"])
        .with_columns(
            is_interpolated=pl.col("resale_price").is_null(),
            resale_price=pl.col("resale_price").interpolate().over("flat_type"),
            psf=pl.col("psf").interpolate().over("flat_type"),
            transaction_volume=pl.col("transaction_volume").fill_null(0),
        )
    )


def plot_median_resale(sf: SidebarFilter, metric, annotations):
    chart_df = get_median_resale_data(sf.df)

    is_psf = metric == "Price per Sqft (PSF)"
    y_col = "median_psf" if is_psf else "median_price"
    y_label = "Median PSF ($)" if is_psf else "Median Resale Price ($)"
    hover_template = (
        "Median PSF: $%{y:,.0f}" if is_psf else "Median Resale Price: $%{y:,.0f}"
    )

    if not chart_df.is_empty():
        fig = make_subplots(
            specs=[[{"secondary_y": True}]],
        )
        fig.add_trace(
            go.Scatter(
                x=chart_df["quarter_label"],
                y=chart_df[y_col],
                name="Median Price",
                mode="lines",
                line=dict(width=3, color="#3498db"),
                hovertemplate=hover_template,
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=chart_df["quarter_label"],
                y=chart_df["txn_count"] / 1000,
                name="Transactions",
                line=dict(width=2, color="#2ecc71"),
                fill="tozeroy",
                hovertemplate="Transactions: %{y:,.2f}k",
            ),
            secondary_y=True,
        )

        new_units_df = get_annual_new_units()
        if not new_units_df.is_empty():
            min_q = chart_df["quarter_label"].min()
            max_q = chart_df["quarter_label"].max()
            new_units_df = new_units_df.filter(
                (pl.col("quarter_label") >= min_q) & (pl.col("quarter_label") <= max_q)
            )

            fig.add_trace(
                go.Scatter(
                    x=new_units_df["quarter_label"],
                    y=new_units_df["total_new_units"] / 1000,
                    name="New MOP Units (Est.)",
                    mode="lines+markers",
                    line=dict(width=2, color="#f39c12", dash="dash"),
                    marker=dict(size=6),
                    hovertemplate="New MOP Units: %{y:,.2f}k (Built %{customdata})",
                    customdata=new_units_df["year_completed"],
                ),
                secondary_y=True,
            )

        max_vol_txn = chart_df["txn_count"].max() / 1000
        max_vol_units = (
            new_units_df["total_new_units"].max() / 1000
            if not new_units_df.is_empty()
            else 0
        )
        max_vol = max(max_vol_txn, max_vol_units)

        fig.update_yaxes(title_text=y_label, secondary_y=False)
        fig.update_yaxes(
            title_text="Volume ('000)",
            range=(0, max_vol * 3),
            showgrid=False,
            secondary_y=True,
        )
        fig.update_layout(
            hovermode="x unified",
            xaxis_title="Quarter",
            title=f"HDB {y_label}",
            **annotations,
        )
        apply_default_theme(fig)

        st.plotly_chart(fig, width="stretch")

        latest_data = chart_df.tail(1)
        if not latest_data.is_empty():
            max_price = latest_data["max_price"][0]
            median_psf = latest_data["median_psf"][0]
            median_price = latest_data["median_price"][0]
            transactions = latest_data["txn_count"][0]

            def get_change_label(current, previous, text=""):
                if previous == 0:
                    return "N/A"
                change = ((current - previous) / previous) * 100
                positive = change > 0
                return dict(
                    delta=f"{text}{change:+.1f}%",
                    delta_color="normal" if positive else "inverse",
                    delta_arrow="up" if positive else "down",
                )

            qoq_highest = "-"
            if len(chart_df) >= 2:
                prev_highest = chart_df.tail(2).head(1)["max_price"][0]
                qoq_highest = get_change_label(max_price, prev_highest, text="QoQ: ")

            qoq_info = "-"
            if len(chart_df) >= 2:
                prev_price = chart_df.tail(2).head(1)["median_price"][0]
                qoq_info = get_change_label(median_price, prev_price, text="QoQ: ")

            yoy_info = "-"
            if len(chart_df) >= 5:
                prev_year_price = chart_df.tail(5).head(1)["median_psf"][0]
                yoy_info = get_change_label(median_psf, prev_year_price, text="YoY: ")

            qoq_trans = "-"
            if len(chart_df) >= 2:
                prev_trans = chart_df.tail(2).head(1)["txn_count"][0]
                qoq_trans = get_change_label(transactions, prev_trans, text="QoQ: ")

            col1, col2, col3, col4 = st.columns(4)
            col1.metric(
                "Highest Price",
                f"${max_price:,.0f}",
                **qoq_highest,
            )
            col2.metric("Median Price", f"${median_price:,.0f}", **qoq_info)
            col3.metric("Median PSF", f"${median_psf:,.0f}", **yoy_info)
            col4.metric("Transactions", f"{transactions:,.0f}", **qoq_trans)


def plot_lease_years(sf: SidebarFilter, metric, annotations: dict):
    chart_df = get_lease_years_data(sf.df)

    is_psf = metric == "Price per Sqft (PSF)"
    y_col = "median_psf" if is_psf else "median_resale_price"
    y_label = "Median PSF ($)" if is_psf else "Median Resale Price ($)"
    title = (
        "Median PSF by Lease Years" if is_psf else "Median Resale Price by Lease Years"
    )

    base_line = px.line(
        chart_df,
        x="quarter_label",
        y=y_col,
        color="cat_remaining_lease_years",
        labels={
            y_col: y_label,
            "quarter_label": "Quarter",
            "cat_remaining_lease_years": "Remaining Lease Years",
        },
    )

    base_line.update_traces(hovertemplate="$%{y:,.0f}")
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "xy"}, {"type": "domain"}]],
        column_widths=[0.75, 0.25],
        horizontal_spacing=0.1,
    )
    for tr in base_line.data:
        tr.update(legendgroup=str(tr.name))
        fig.add_trace(tr, row=1, col=1)
    fig.update_layout(
        hovermode="x unified",
        xaxis_tickformat="%Y-%m",
        legend_title_text="Remaining Lease Years",
        xaxis_title="Quarter",
        **annotations,
    )
    fig.update_yaxes(title_text=y_label)

    pie_df = (
        chart_df.group_by("cat_remaining_lease_years")
        .agg(pl.col("transaction_volume").sum().alias("volume"))
        .sort("cat_remaining_lease_years")
    )
    lease_labels = pie_df["cat_remaining_lease_years"]
    lease_values = pie_df["volume"]

    color_map = {str(tr.name): tr.line.color for tr in base_line.data}
    add_pie_slices(
        fig,
        lease_labels,
        lease_values,
        color_map,
        row=1,
        col=2,
        pie_title="Transaction<br>Volume",
    )
    apply_default_theme(fig)

    fig.update_layout(
        title=title,
        xaxis_title="Quarter",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=0.02,
        ),
    )
    st.plotly_chart(fig, width="stretch")

    df_sampled = sf.df.sample(n=min(sf.df.height, 5000), with_replacement=False).sort(
        "cat_remaining_lease_years"
    )
    scatter_fig = px.scatter(
        df_sampled,
        x="remaining_lease_years",
        y="psf" if is_psf else "resale_price",
        color="cat_remaining_lease_years",
        labels={
            "cat_remaining_lease_years": "Remaining Lease Years",
            "resale_price": "Resale Price",
            "psf": "Price per Sqft (PSF)",
        },
        height=600,
        hover_data=[
            "remaining_lease_years",
            "resale_price",
            "psf",
            "address",
            "storey_range",
            "month",
        ],
        title=f"{'PSF' if is_psf else 'Resale Price'} vs Remaining Lease Years",
        render_mode="webgl",
    )
    scatter_fig.update_traces(
        marker=dict(
            size=6,
            symbol="circle-open",
            opacity=1,
            line=dict(width=1.5),
        ),
        selector=dict(mode="markers"),
        hovertemplate="<b>Price:</b> $%{y:,.3s}<br>"
        + "<b>Lease Years:</b> %{x} years<br>"
        + "<b>Address:</b> %{customdata[1]}<br>"
        + "<b>Storey:</b> %{customdata[2]}<br>"
        + "<b>Sold:</b> %{customdata[3]|%Y-%m}<br>",
    )
    apply_default_theme(scatter_fig)
    st.plotly_chart(scatter_fig, width="stretch")

    count_df = pie_df
    bar_fig = px.bar(
        count_df,
        x="volume",
        y="cat_remaining_lease_years",
        color="cat_remaining_lease_years",
        orientation="h",
        labels={"volume": "Count", "cat_remaining_lease_years": "Lease Category"},
        height=250,
        text="volume",
        title="Total Transaction by Remaining Lease Years",
    )
    bar_fig.update_traces(textposition="outside", selector=dict(type="bar"))
    apply_default_theme(bar_fig)
    bar_fig.update_layout(
        showlegend=False,
    )

    st.plotly_chart(bar_fig, width="stretch")

    scatter_fig = px.scatter(
        df_sampled,
        x="storey_lower_bound",
        y="psf" if is_psf else "resale_price",
        color="cat_remaining_lease_years",
        labels={
            "storey_lower_bound": "Storey",
            "resale_price": "Resale Price",
            "psf": "Price per Sqft (PSF)",
            "cat_remaining_lease_years": "Lease Category",
        },
        height=600,
        hover_data=[
            "remaining_lease_years",
            "resale_price",
            "psf",
            "address",
            "storey_range",
            "month",
        ],
        title=f"{'PSF' if is_psf else 'Resale Price'} vs Remaining Lease Years",
        render_mode="webgl",
    )
    scatter_fig.update_traces(
        marker=dict(
            size=6,
            symbol="circle-open",
            opacity=1,
            line=dict(width=1.5),
        ),
        selector=dict(mode="markers"),
        hovertemplate="<b>Price:</b> $%{y:,.3s}<br>"
        + "<b>Lease Years:</b> %{x} years<br>"
        + "<b>Address:</b> %{customdata[1]}<br>"
        + "<b>Storey:</b> %{customdata[2]}<br>"
        + "<b>Sold:</b> %{customdata[3]|%Y-%m}<br>",
    )
    apply_default_theme(scatter_fig)
    st.plotly_chart(scatter_fig, width="stretch")


def plot_town(sf: SidebarFilter, metric, annotations: dict):
    col1, col2 = st.columns(spec=[0.5, 0.5])
    show_transaction_volumes = col1.checkbox("Show transaction volumes", value=False)

    chart_df = get_town_data(sf.df)
    unique_towns = chart_df["town"].unique().sort()
    n_towns = len(unique_towns)

    town_colors = pastel_colors(n_towns)
    town_color_map = {str(t): town_colors[i] for i, t in enumerate(unique_towns)}
    is_psf = metric == "Price per Sqft (PSF)"
    y_col = "psf" if is_psf else "resale_price"
    y_label = "Median PSF ($)" if is_psf else "Median Resale Price ($)"
    title = "Median PSF by Town" if is_psf else "Median Resale Price by Town"

    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"secondary_y": True}, {"type": "domain"}]],
        column_widths=[0.8, 0.2],
        horizontal_spacing=0.1,
    )
    for town in chart_df["town"].unique().sort():
        town_df = chart_df.filter(pl.col("town") == town)

        fig.add_trace(
            go.Scatter(
                x=town_df["quarter_label"],
                y=town_df[y_col],
                mode="lines",
                name=town,
                hovertemplate="$%{y}",
                legendgroup=town,
                line=dict(shape="spline", color=town_color_map[town]),
            ),
            row=1,
            col=1,
            secondary_y=False,
        )

        if show_transaction_volumes:
            fig.add_trace(
                go.Bar(
                    x=town_df["quarter_label"],
                    y=town_df["transaction_volume"],
                    name=town,
                    hovertemplate="%{y} transactions",
                    legendgroup=town,
                    marker_color=town_color_map[town],
                    showlegend=False,
                ),
                secondary_y=True,
                row=1,
                col=1,
            )

    fig.update_xaxes(tickformat="%Y-%m", row=1, col=1)
    fig.update_yaxes(
        title_text=y_label,
        secondary_y=False,
    )

    fig.update_yaxes(
        showgrid=False, zeroline=False, secondary_y=True, showticklabels=False
    )

    custom_layout = {
        "yaxis": dict(
            range=[
                chart_df[y_col].min() * 0.6,
                chart_df[y_col].max() * 1.2,
            ]
        ),
        "yaxis2": (
            dict(range=[0, chart_df["transaction_volume"].max() * 15])
            if show_transaction_volumes
            else {}
        ),
    }

    fig.update_layout(
        title=title,
        yaxis_title=y_label,
        xaxis_title="Quarter",
        hovermode="x unified",
        barmode="stack",
        **custom_layout,
        xaxis_tickformat="%Y-%m",
        legend_title_text="Town",
        **annotations,
    )

    fig.update_layout(
        hovermode="x unified",
        legend=dict(
            orientation="h",
            y=0,
            x=0.5,
            xanchor="center",
            yanchor="bottom",
            yref="container",
        ),
        margin=dict(l=50, r=50, t=100, b=150),
        height=600,
    )

    pie_df = (
        chart_df.group_by("town")
        .agg(pl.col("transaction_volume").sum().alias("volume"))
        .sort("town")
    )
    town_labels = pie_df["town"]
    town_values = pie_df["volume"]

    add_pie_slices(
        fig,
        town_labels,
        town_values,
        town_color_map,
        row=1,
        col=2,
        pie_title="Transaction<br>Volume",
    )
    st.plotly_chart(fig, width="stretch")

    fig_box = px.box(
        chart_df,
        y="town",
        x=y_col,
        hover_data={
            "resale_price": ":.0f",
        },
        title="Distribution by Town",
        height=600,
        labels={
            y_col: y_label,
            "quarter_label": "Quarter",
            "town": "Town",
        },
    )

    fig_box.update_layout(showlegend=False)

    st.plotly_chart(fig_box, width="stretch")
    bar_fig = px.bar(
        pie_df.sort(by="volume"),
        x="volume",
        y="town",
        # color=y_col,
        orientation="h",
        labels={"volume": "Count", "town": "Town"},
        height=600,
        text="volume",
        title="Total Transaction by Remaining Lease Years",
    )
    bar_fig.update_traces(textposition="outside", selector=dict(type="bar"))
    apply_default_theme(bar_fig)
    # apply_default_theme(fig_box)
    st.plotly_chart(bar_fig, width="stretch")


def plot_flat_type(sf: SidebarFilter, metric):
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "xy"}, {"type": "domain"}]],
        column_widths=[0.75, 0.25],
        horizontal_spacing=0.1,
    )
    colors = px.colors.qualitative.Plotly
    chart_df = get_flat_type_data(sf.df)
    all_flat_types = chart_df["flat_type"].unique().sort()

    is_psf = metric == "Price per Sqft (PSF)"
    y_col = "psf" if is_psf else "resale_price"
    y_label = "Median PSF ($)" if is_psf else "Median Resale Price ($)"
    title = "Median PSF by Flat Type" if is_psf else "Median Resale Price by Flat Type"
    color_map = {ft: colors[i % len(colors)] for i, ft in enumerate(all_flat_types)}

    for flat_type in all_flat_types:
        color = color_map[flat_type]

        flat_type_df = chart_df.filter(pl.col("flat_type") == flat_type)

        actual_prices = flat_type_df.with_columns(
            actual=pl.when(pl.col("is_interpolated")).then(None).otherwise(pl.col(y_col))
        )["actual"]

        fig.add_trace(
            go.Scatter(
                x=flat_type_df["quarter_label"],
                y=flat_type_df[y_col],
                mode="lines",
                line=dict(color=color, dash="dot", width=2),
                legendgroup=flat_type,
                showlegend=False,
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=flat_type_df["quarter_label"],
                y=actual_prices,
                mode="lines",
                line=dict(color=color, width=3),
                legendgroup=flat_type,
                showlegend=True,
                name=flat_type,
                hovertemplate="$%{y:,.0f}",
            ),
            row=1,
            col=1,
        )
    fig.update_layout(
        title_text=f"Median {y_label} by Flat Type",
        xaxis_title="Quarter",
        yaxis_title=y_label,
        xaxis_tickformat="%Y-%m",
        legend_title_text="",
        hovermode="x unified",
    )

    pie_df = (
        chart_df.group_by("flat_type")
        .agg(pl.col("transaction_volume").sum().alias("volume"))
        .sort("flat_type")
    )
    flat_labels = pie_df["flat_type"]
    flat_values = pie_df["volume"]

    add_pie_slices(
        fig,
        flat_labels,
        flat_values,
        color_map,
        row=1,
        col=2,
        pie_title="",
    )
    apply_default_theme(fig)

    st.plotly_chart(fig, width="stretch")


st.title("Resale Price Trends")
st.markdown("Explore HDB resale market trends, grouped by different perspectives.")

# badge = get_last_updated_badge()
# col1, col2 = st.columns([0.3, 0.7], vertical_alignment="center")
# with col1:
#    st.image(badge)
# with col2:
metric = st.sidebar.segmented_control(
    "Select Metric",
    options=["Resale Price", "Price per Sqft (PSF)"],
    label_visibility="collapsed",
    help="Choose between total Resale Price or Price per Square Foot (PSF).",
    default="Resale Price",
)

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Lease Years", "Town", "Flat Type"])

source = "Source: <a href='https://data.gov.sg/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/view'>data.gov.sg</a>"
annotations = dict(
    margin=dict(l=50, r=50, t=100, b=100),
    annotations=[
        dict(
            x=0.5,
            y=-0.33,
            xref="paper",
            yref="paper",
            text=source,
            showarrow=False,
        )
    ],
    height=500,
)

sf = SidebarFilter(
    min_date=datetime.strptime("2017-01-01", "%Y-%m-%d").date(),
    select_towns=(True, "multi"),
    select_lease_years=True,
    select_storey=True,
    default_town=None,
)

with tab1:
    from webapp.read import get_last_updated_badge

    # st.subheader("Overview")
    plot_median_resale(sf, metric, annotations)
    st.markdown("### Recent transactions")
    df_to_show = sf.df.select(
        "_ts",
        pl.col("month").dt.strftime("%Y-%m").alias("month_sold"),
        "town",
        "address",
        "flat_type",
        "resale_price",
        "floor_area_sqft",
        "psf",
        "storey_range",
        "remaining_lease",
        "quarter_label",
    ).sort(by=["month_sold", "_ts"], descending=True)

    st.dataframe(
        df_to_show.filter(
            pl.col("month_sold")
            >= (datetime.today().replace(day=1) - timedelta(weeks=5)).strftime("%Y-%m")
        )
    )

    st.markdown("### Download")
    st.write(
        "Download the full dataset for resale flat prices based on registration date from Jan-2017 onwards"
    )
    st.write(
        "Note: the original dataset can be found here: [data.gov.sg](https://data.gov.sg/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/view)."
    )
    st.download_button(
        "Download CSV",
        df_to_show.write_csv(),
        "hdb_resale_data.csv",
        "text/csv",
        key="download-csv",
    )
with tab2:
    group_by = "Lease Years"
    plot_lease_years(sf, metric, annotations)

with tab3:
    group_by = "Town"
    plot_town(sf, metric, annotations)

with tab4:
    group_by = "Flat Type"
    plot_flat_type(
        sf,
        metric,
    )
