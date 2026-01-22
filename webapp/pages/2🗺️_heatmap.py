import streamlit as st
import polars as pl
import pydeck as pdk
import plotly.colors as pc

from webapp.filter import SidebarFilter


def create_heatmap_layer(df, grid_size_meters=70):
    degree_per_meter = 1 / 111000
    grid_size_deg = grid_size_meters * degree_per_meter

    # use simple rounding to create grid centroids
    df_grid = df.with_columns(
        [
            ((pl.col("latitude") / grid_size_deg).round() * grid_size_deg).alias(
                "lat_bin"
            ),
            ((pl.col("longitude") / grid_size_deg).round() * grid_size_deg).alias(
                "lon_bin"
            ),
        ]
    )

    # Calculate average PSF, price stats, and most frequent street
    agg_exprs = [
        pl.col("psf").mean().alias("avg_psf"),
        pl.col("resale_price").mean().alias("avg_price"),
        pl.col("resale_price").max().alias("max_price"),
        pl.col("resale_price").min().alias("min_price"),
        pl.col("remaining_lease_years").mean().alias("avg_remaining_lease"),
        pl.col("street_name").mode().first().alias("mode_street"),
        pl.len().alias("count"),
    ]

    agg_df = df_grid.group_by(["lat_bin", "lon_bin"]).agg(agg_exprs)

    if isinstance(agg_df, pl.LazyFrame):
        agg_df = agg_df.collect()

    return agg_df, grid_size_meters


# Sidebar Filters
sb = SidebarFilter(
    select_towns=(False, "single"),  # Heatmap covers all towns
    select_lease_years=True,
    select_flat_type=True,
)
filtered_df = sb.df

# Ensure valid coordinates
filtered_df = filtered_df.drop_nulls(subset=["latitude", "longitude", "psf"])

if filtered_df.is_empty():
    st.warning("No data found for the selected filters.")
    st.stop()

grid_size_meters = 70
agg_df, grid_meters = create_heatmap_layer(filtered_df, grid_size_meters=grid_size_meters)

st.title("🗺️ HDB Resale Price Heatmap")
st.markdown(
    f"Visualize average resale prices (PSF) in {grid_size_meters}x{grid_size_meters}m grids."
)


# Visualization
# Define color scale
min_psf = agg_df["avg_psf"].min()
max_psf = agg_df["avg_psf"].max()

start_date_str = sb.start_date.strftime("%Y-%m")
end_date_str = sb.end_date.strftime("%Y-%m")

st.caption(
    f"Showing data from {start_date_str} to {end_date_str}. Total grids: {len(agg_df)}"
)
st.caption(rf"Price Range (PSF): \${min_psf:,.0f} - \${max_psf:,.0f}")

# Pydeck Layer
# Use PolygonLayer for exact square boxes
degree_per_meter = 1 / 111000
grid_size_deg = grid_size_meters * degree_per_meter
half_size = grid_size_deg / 2


def get_polygon(row):
    lat = row["lat_bin"]
    lon = row["lon_bin"]
    return [
        [lon - half_size, lat - half_size],
        [lon + half_size, lat - half_size],
        [lon + half_size, lat + half_size],
        [lon - half_size, lat + half_size],
    ]


# Color Mapping Logic
def get_color_mapped(val, vmin, vmax, colorscale_name="Portland"):
    # Normalize
    if vmax == vmin:
        norm_val = 0.5
    else:
        norm_val = (val - vmin) / (vmax - vmin)

    target_val = norm_val

    # Get color from Plotly
    # sample_colorscale returns a list of colors, we take the first one
    color_str = pc.sample_colorscale(colorscale_name, [target_val])[0]

    # Parse color string
    try:
        if color_str.startswith("rgb"):
            # format: rgb(r, g, b) or rgba(r, g, b, a)
            content = color_str.split("(")[1].split(")")[0]
            parts = [float(x.strip()) for x in content.split(",")]
            r, g, b = int(parts[0]), int(parts[1]), int(parts[2])
            return [r, g, b, 180]  # Add alpha 180
        elif color_str.startswith("#"):
            # format: #RRGGBB
            return list(pc.hex_to_rgb(color_str)) + [180]
    except Exception:
        pass

    return [128, 128, 128, 180]  # Fallback


# Apply color mapping and polygon creation
pdf = agg_df.to_pandas()
pdf["color"] = pdf["avg_psf"].apply(lambda x: get_color_mapped(x, min_psf, max_psf))
pdf["polygon"] = pdf.apply(get_polygon, axis=1)

# Format for tooltip
pdf["fmt_psf"] = pdf["avg_psf"].apply(lambda x: f"${x:,.2f}")
pdf["fmt_price"] = pdf["avg_price"].apply(lambda x: f"${x:,.0f}")
pdf["fmt_max_price"] = pdf["max_price"].apply(lambda x: f"${x:,.0f}")
pdf["fmt_min_price"] = pdf["min_price"].apply(lambda x: f"${x:,.0f}")
pdf["fmt_lease"] = pdf["avg_remaining_lease"].apply(lambda x: f"{x:.1f} yrs")

layer = pdk.Layer(
    "PolygonLayer",
    id="heatmap_layer",
    data=pdf,
    get_polygon="polygon",
    get_fill_color="color",
    get_line_color=[0, 0, 0, 0],  # No outline
    pickable=True,
    auto_highlight=True,
    opacity=0.8,
    stroked=False,
    filled=True,
    extruded=False,
)

# Tooltip
tooltip = {
    "html": (
        "<b>Street:</b> {mode_street}<br/>"
        "<b>Average PSF:</b> {fmt_psf}<br/>"
        "<b>Avg Price:</b> {fmt_price}<br/>"
        "<b>Min Price:</b> {fmt_min_price}<br/>"
        "<b>Max Price:</b> {fmt_max_price}<br/>"
        "<b>Avg Lease:</b> {fmt_lease}<br/>"
        "<b>Transactions:</b> {count}"
    ),
    "style": {"backgroundColor": "steelblue", "color": "white"},
}

# View State
view_state = pdk.ViewState(
    latitude=1.3521,
    longitude=103.8198,
    zoom=11,
    pitch=0,
)

deck = pdk.Deck(
    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    initial_view_state=view_state,
    layers=[layer],
    tooltip=tooltip,
)

# Render Chart
st.pydeck_chart(deck)
