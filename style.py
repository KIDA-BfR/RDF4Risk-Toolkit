import streamlit as st

def apply_global_styles(active_step: int = 0):
    """
    active_step: 1=Matching Table Generator, 2=Reconciliation,
                 3=RDF Generator, 4=RDF to Table
                 0 = Home (no step highlighted)
    Home is li:nth-child(1); MTG is li:nth-child(2); so active nth = active_step + 1.
    """
    active_nth = active_step + 1 if active_step >= 1 else 0

    active_circle_css = ""
    if active_nth >= 2:
        active_circle_css = f"""
            [data-testid="stSidebarNavItems"] li:nth-child({active_nth})::before {{
                background-color: #0071e3 !important;
                color: #ffffff !important;
                box-shadow: 0 0 0 3px rgba(0, 113, 227, 0.20),
                            0 2px 8px rgba(0, 113, 227, 0.45) !important;
            }}
        """

    st.markdown(f"""
        <style>
            /* ── Hide Streamlit chrome ─────────────────────────────────────── */
            [data-testid="stDecoration"] {{ display: none !important; }}
            [data-testid="stHeader"]     {{ display: none !important; }}

            /* ── Sidebar: workflow strip ───────────────────────────────────── */

            /* "WORKFLOW" label above the nav */
            [data-testid="stSidebarNav"]::before {{
                content: "RDF4Risk Toolkit";
                display: block;
                font-size: 24px;
                font-weight: 700;
                letter-spacing: 0.12em;
                color: #6e6e73;
                padding: 20px 16px 10px 16px;
            }}

            /* Nav list container */
            [data-testid="stSidebarNavItems"] {{
                counter-reset: step;
                position: relative;
                padding: 0 8px 12px 8px !important;
            }}

            /* Vertical connecting line — drawn as ::after on each non-last
               workflow step so the line starts exactly at MTG's circle center.
               Each segment: from this circle's center (top:50%) downward,
               extending past the li bottom by enough to reach the next circle
               center (3px margin-gap + half of next li height ≈ 25px). */
            [data-testid="stSidebarNavItems"] li:not(:first-child):not(:last-child)::after {{
                content: "";
                position: absolute;
                left: 15px;          /* circle center X: 4px offset + 12px radius - 1px */
                top: 50%;            /* start at this circle's center */
                height: calc(50% + 25px); /* reach the next circle's center */
                width: 2px;
                background-color: #c7c7cc;
                z-index: 0;
            }}

            /* ── Home item (first child): plain link, no circle ─────────── */
            [data-testid="stSidebarNavItems"] li:first-child {{
                list-style: none !important;
                margin: 3px 0 8px 0;
                padding-bottom: 8px;
                border-bottom: 1px solid #e5e5ea;
            }}
            [data-testid="stSidebarNavItems"] li:first-child::before {{
                display: none !important;
            }}
            [data-testid="stSidebarNavItems"] li:first-child
                [data-testid="stSidebarNavLink"] {{
                margin-left: 4px !important;
            }}

            /* ── Workflow steps (all li except first) ───────────────────── */
            [data-testid="stSidebarNavItems"] li:not(:first-child) {{
                counter-increment: step;
                position: relative;
                list-style: none !important;
                margin: 3px 0;
            }}

            /* Numbered circle — inactive (muted gray) */
            [data-testid="stSidebarNavItems"] li:not(:first-child)::before {{
                content: counter(step);
                position: absolute;
                left: 4px;
                top: 50%;
                transform: translateY(-50%);
                z-index: 1;
                width: 24px;
                height: 24px;
                line-height: 24px;
                border-radius: 50%;
                background-color: #d1d1d6;
                color: #6e6e73;
                font-size: 11px;
                font-weight: 700;
                text-align: center;
                box-shadow: none;
            }}

            /* Active step circle — injected per page via active_step param */
            {active_circle_css}

            /* Nav link — push text right of the circle */
            [data-testid="stSidebarNavLink"] {{
                margin-left: 32px !important;
                padding: 7px 10px !important;
                border-radius: 8px !important;
                font-size: 20px !important;
                color: #1d1d1f !important;
                transition: background 0.15s ease !important;
            }}

            [data-testid="stSidebarNavLink"]:hover {{
                background-color: rgba(0, 113, 227, 0.08) !important;
                color: #0071e3 !important;
            }}

            /* Active page link text */
            [data-testid="stSidebarNavLink"][aria-current="page"] {{
                background-color: rgba(0, 113, 227, 0.10) !important;
                color: #0071e3 !important;
                font-weight: 600 !important;
            }}
        </style>
    """, unsafe_allow_html=True)
