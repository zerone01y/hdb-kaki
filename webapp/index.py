import streamlit as st

from webapp.logo import icon, logo
from webapp.read import get_last_updated_badge


def main():
    st.set_page_config(page_title="HDB Kaki", page_icon=icon, layout="wide")

    st.image(logo, width=500)

    badge = get_last_updated_badge(subdir="Resale Flat Prices")
    st.image(badge)

    st.markdown("## Welcome to HDB Kaki")

    st.markdown(
        """
HDB Kaki helps you stay updated on the latest movements in the HDB resale market.

It enables users to examine price trends by:

  - Year and transaction period

  - Flat type

  - Town and location

  - Remaining release years

Navigate through the sidebar to explore!
"""
    )


if __name__ == "__main__":
    pages = {
        "Analysis": [
            st.Page(main, title="HDB Kaki", icon="🔑"),
            st.Page("pages/1📊_price_trend.py", title="Resale Trends", icon="📈"),
            st.Page(
                "pages/2🗺️_heatmap.py",
                title="HDB Resale Price Heatmap",
                icon="🗺️",
            ),
        ],
    }

    pg = st.navigation(pages)

    pg.run()
