from pathlib import Path


def get_project_root() -> Path:
    cloud_path = Path("/mount/src/hdb-kaki")

    if cloud_path.exists():
        return cloud_path
    return Path(__file__).parent.parent


def pastel_colors(n: int):
    import colorsys

    n = max(n, 1)
    return [
        "#%02x%02x%02x"
        % tuple(int(c * 255) for c in colorsys.hls_to_rgb(i / n, 0.75, 0.7))
        for i in range(n)
    ]


def add_pie_slices(fig, labels, values, color_map, row=1, col=2, pie_title="Test"):
    import plotly.graph_objects as go

    colors = [color_map.get(str(lbl), "#cccccc") for lbl in labels]
    fig.add_trace(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.5,
            marker=dict(colors=colors),
            showlegend=False,
        ),
        row=row,
        col=col,
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent",
        selector=dict(type="pie"),
        hovertemplate="Category: %{label}<br>Share: %{percent}<br>Volume: %{value:,.0f}",
    )

    fig.update_layout(
        uniformtext_minsize=12,
        uniformtext_mode="hide",
        annotations=[
            dict(
                text=pie_title,
                x=sum(fig.get_subplot(row, col).x) / 2,
                y=sum(fig.get_subplot(row, col).y) / 2,
                font_size=12,
                showarrow=False,
                xanchor="center",
            ),
        ],
    )
    fig.update_traces(
        hoverlabel=dict(
            bgcolor="white",
            bordercolor="#e5e7eb",
            font=dict(
                size=12,
                family="'Inter', sans-serif",
                color="#374151",
            ),
        ),
        selector=dict(type="pie"),
    )


def apply_default_theme(fig):
    """
    Apply a consistent, gentle, and pretty theme to Plotly figures.
    """

    fig.update_layout(
        template="plotly_white",
        font=dict(family="'Inter', sans-serif", size=12, color="#374151"),
        title=dict(
            font=dict(
                size=18, color="#111827", family="'Inter', sans-serif", weight="bold"
            ),
            x=0,
            xanchor="left",
        ),
        paper_bgcolor="rgba(0,0,0,0)",  # Transparent background
        plot_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(
            bgcolor="white",
            bordercolor="#e5e7eb",
            font=dict(
                family="'Inter', sans-serif",
                size=12,
                color="#374151",
            ),
        ),
        margin=dict(l=40, r=40, t=80, b=40),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor="#e5e7eb",
            linewidth=1,
            tickfont=dict(color="#6b7280"),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#f3f4f6",
            gridwidth=1,
            zeroline=False,
            tickfont=dict(color="#6b7280"),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.0,
            xanchor="right",
            x=1,
            font=dict(size=11, color="#4b5563"),
        ),
    )
    return fig
