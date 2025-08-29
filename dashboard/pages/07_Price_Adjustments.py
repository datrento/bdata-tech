import os
import json
import sys
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import text

_CURR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_CURR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from db import get_db_connection


st.set_page_config(page_title="Price Adjustments", layout="wide")
st.title("Price Adjustments")


@st.cache_data(ttl=60)
def load_proposals(statuses, since_ts, sku_search):
    where = ["created_at >= :since"] if since_ts else []
    params = {"since": since_ts} if since_ts else {}
    if statuses:
        where.append("status = ANY(:statuses)")
        params["statuses"] = statuses
    if sku_search:
        where.append("sku ILIKE :sku")
        params["sku"] = f"%{sku_search.strip()}%"
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    q = text(f"""
        SELECT 
          id, sku, status, created_at, approved_at, applied_at,
          old_price, proposed_price, elasticity_used, expected_profit_delta, expected_demand_delta,
          competitor_gap_after, score, reason_code
        FROM price_adjustments
        {where_sql}
        ORDER BY created_at DESC
        LIMIT 2000
    """)
    with get_db_connection().connect() as conn:
        df = pd.read_sql(q, conn, params=params)
    return df


@st.cache_data(ttl=60)
def load_runs(limit=50):
    q = text("""
        SELECT id, started_at, finished_at, parameters, candidates_count, proposed_count, created_by
        FROM price_adjustment_runs
        ORDER BY started_at DESC
        LIMIT :lim
    """)
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn, params={"lim": limit})


@st.cache_data(ttl=60)
def load_recent_history(hours=24):
    q = text("""
        SELECT sku, old_price, adjusted_price, change_type, changed_by, change_timestamp, proposal_id
        FROM platform_product_price_history
        WHERE change_timestamp >= NOW() - INTERVAL ':h hours'
        ORDER BY change_timestamp DESC
        """.replace(":h", str(int(hours))))
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn)


def approve_reject_form():
    st.subheader("Moderation")
    col1, col2, col3 = st.columns([2, 2, 6])
    with col1:
        pid = st.text_input("Proposal ID", key="mod_pid")
    with col2:
        action = st.selectbox("Action", ["approved", "rejected"], index=0, key="mod_action")
    with col3:
        note = st.text_input("Note (optional)", key="mod_note")
    btn = st.button("Submit", type="primary")
    if btn and pid and pid.isdigit():
        sql = text("""
            UPDATE price_adjustments
            SET status = :status,
                approved_at = CASE WHEN :status = 'approved' THEN NOW() ELSE approved_at END,
                approved_by = COALESCE(approved_by, 'dashboard'),
                notes = COALESCE(notes,'') || :note
            WHERE id = :pid
        """)
        with get_db_connection().begin() as conn:
            conn.execute(sql, {"status": action, "note": (" " + note) if note else "", "pid": int(pid)})
        st.success(f"Proposal {pid} set to {action}.")


# Filters
with st.container():
    colf1, colf2, colf3, colf4 = st.columns([2, 2, 2, 4])
    with colf1:
        timeframe = st.selectbox("Timeframe", ["24h", "7d", "30d", "All"], index=0)
    with colf2:
        statuses = st.multiselect("Status", ["pending", "approved", "applied", "rejected", "expired"], default=["pending", "approved"]) 
    with colf3:
        sku_search = st.text_input("SKU contains", placeholder="e.g., IPHONE")
    with colf4:
        st.caption("Use moderation to approve/reject. Applier runs every 5 minutes.")

since_ts = None
if timeframe != "All":
    if timeframe == "24h":
        since_ts = datetime.utcnow() - timedelta(hours=24)
    elif timeframe == "7d":
        since_ts = datetime.utcnow() - timedelta(days=7)
    elif timeframe == "30d":
        since_ts = datetime.utcnow() - timedelta(days=30)

df = load_proposals(statuses, since_ts, sku_search)
runs = load_runs(50)
hist24 = load_recent_history(24)


# KPIs
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Pending", int((df["status"] == "pending").sum()))
with k2:
    st.metric("Approved", int((df["status"] == "approved").sum()))
with k3:
    st.metric("Applied (24h)", int(len(hist24)))
with k4:
    last_run = runs.iloc[0] if not runs.empty else None
    st.metric("Last Run Proposals", int(last_run["proposed_count"]) if last_run is not None else 0)

# Secondary KPIs from proposals
skp1, skp2, skp3, skp4 = st.columns(4)
with skp1:
    st.metric("Forecast-guided", int((df["reason_code"] == "elasticity+forecast").sum()))
with skp2:
    st.metric("Elasticity-only", int((df["reason_code"] == "elasticity-only").sum()))
with skp3:
    st.metric("Avg Expected Profit Δ", f"${df['expected_profit_delta'].dropna().mean():.2f}" if not df.empty else "$0.00")
with skp4:
    st.metric("Avg Expected Demand Δ", f"{df['expected_demand_delta'].dropna().mean():.2f}")


st.subheader("Action Queue")
st.dataframe(df, use_container_width=True, hide_index=True)

approve_reject_form()

with st.expander("Runs (recent)", expanded=False):
    if not runs.empty:
        runs_disp = runs.copy()
        # show compact parameters parsed
        def _params_to_str(x):
            try:
                js = x if isinstance(x, dict) else json.loads(x)
            except Exception:
                return str(x)
            keys = ["price_grid_pct", "target_undercut_pct", "gap_tolerance_pct", "forecast_min_conf"]
            parts = [f"{k}={js.get(k)}" for k in keys if k in js]
            return ", ".join(parts)
        runs_disp["params"] = runs_disp["parameters"].apply(_params_to_str)
        runs_disp = runs_disp.drop(columns=["parameters"]) 
        st.dataframe(runs_disp, use_container_width=True, hide_index=True)
    else:
        st.caption("No runs found yet.")

with st.expander("Applied (last 24h)", expanded=False):
    st.dataframe(hist24, use_container_width=True, hide_index=True)


# Drilldown: price comparison for a SKU (our price vs competitors)
st.markdown("")
st.divider()
st.subheader("SKU Drill-down: Price Comparison")

@st.cache_data(ttl=60)
def load_sku_platform_history(sku: str, days: int = 7) -> pd.DataFrame:
    q = text("""
        SELECT change_timestamp AS ts, adjusted_price
        FROM platform_product_price_history
        WHERE sku = :sku AND change_timestamp > NOW() - INTERVAL ':d days'
        ORDER BY ts
    """.replace(":d", str(int(days))))
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn, params={"sku": sku})


@st.cache_data(ttl=60)
def load_sku_proposals(sku: str, days: int = 7) -> pd.DataFrame:
    q = text("""
        SELECT id, created_at, effective_from, status, old_price, proposed_price
        FROM price_adjustments
        WHERE sku = :sku AND created_at > NOW() - INTERVAL ':d days'
        ORDER BY created_at
    """.replace(":d", str(int(days))))
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn, params={"sku": sku})


@st.cache_data(ttl=60)
def load_current_platform_price(sku: str) -> float | None:
    q = text("""
        SELECT current_price FROM platform_products WHERE sku = :sku
    """)
    with get_db_connection().connect() as conn:
        df = pd.read_sql(q, conn, params={"sku": sku})
    return float(df.iloc[0]["current_price"]) if not df.empty else None


@st.cache_data(ttl=60)
def load_sku_competitor_windows(sku: str, days: int = 7, window_minutes: int = 5) -> pd.DataFrame:
    q = text(f"""
        WITH b AS (
          SELECT 
            product_sku,
            competitor_id,
            collection_timestamp,
            price,
            date_trunc('hour', collection_timestamp)
              + floor(date_part('minute', collection_timestamp)/{window_minutes}) * interval '{window_minutes} minute' as window_start
          FROM competitor_price_history
          WHERE product_sku = :sku AND collection_timestamp > NOW() - INTERVAL '{days} days'
        )
        SELECT competitor_id, window_start AS ts, AVG(price) AS avg_price
        FROM b
        GROUP BY competitor_id, window_start
        ORDER BY ts
    """)
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn, params={"sku": sku})


@st.cache_data(ttl=300)
def load_competitor_codes() -> pd.DataFrame:
    q = text("""
        SELECT id AS competitor_id, code
        FROM external_competitors
    """)
    with get_db_connection().connect() as conn:
        return pd.read_sql(q, conn)


# SKU picker sourced from proposals/history
sku_options = sorted(set(df["sku"].tolist()) | set(hist24.get("sku", pd.Series(dtype=str)).tolist())) if not df.empty else hist24.get("sku", pd.Series(dtype=str)).unique().tolist()
sku = st.selectbox("Select SKU", sku_options)
days_drill = st.slider("Days", 1, 30, 7)

if sku:
    plat = load_sku_platform_history(sku, days=days_drill)
    comp = load_sku_competitor_windows(sku, days=days_drill)
    props = load_sku_proposals(sku, days=days_drill)
    codes = load_competitor_codes()

    # Show current price just below the selector
    current_price_val = load_current_platform_price(sku)
    if current_price_val is not None:
        st.caption(f"Current price: ${float(current_price_val):.2f}")

    # Prepare chart data
    layers = []
    if True:
        # Resample to 15-minute windows and forward-fill to show continuous price line using LOCAL time
        now_ts = pd.Timestamp.now()
        start_ts = now_ts - pd.Timedelta(days=days_drill)
        # Normalize timestamps to tz-naive LOCAL for platform history
        if not plat.empty:
            plat["ts"] = pd.to_datetime(plat["ts"], errors="coerce")
        base_idx = pd.date_range(start=start_ts, end=now_ts, freq="15min")
        seed_price = load_current_platform_price(sku)
        plat_seed = pd.DataFrame({"ts": [start_ts], "adjusted_price": [seed_price]}) if seed_price is not None else pd.DataFrame(columns=["ts","adjusted_price"])
        plat_all = pd.concat([plat_seed, plat[["ts","adjusted_price"]]], ignore_index=True)
        plat_rs = (
            plat_all.assign(ts=pd.to_datetime(plat_all["ts"], errors="coerce")).set_index("ts").sort_index()
            .reindex(base_idx).ffill().reset_index().rename(columns={"index":"ts"})
        )
        plat_rs["series"] = "Our Price"

    if not comp.empty:
        # limit to top 3 competitors by most recent timestamp presence
        latest = comp.groupby("competitor_id")["ts"].max().sort_values(ascending=False).head(3).index.tolist()
        comp_plot = comp[comp["competitor_id"].isin(latest)].copy()
        comp_plot = comp_plot.merge(codes, on="competitor_id", how="left")
        comp_plot["series"] = comp_plot["code"].fillna(comp_plot["competitor_id"].astype(str))
        comp_plot["ts"] = pd.to_datetime(comp_plot["ts"], errors="coerce")

        # Defer layer creation until we compute a shared x-domain

    # Build proposed price step series from proposals (approved preferred)
    proposal_step_df = pd.DataFrame(columns=["ts", "price", "status"]) 
    if not props.empty:
        props_tmp = props.copy()
        props_tmp["created_at"] = pd.to_datetime(props_tmp["created_at"], errors="coerce")
        if "effective_from" in props_tmp.columns:
            props_tmp["effective_from"] = pd.to_datetime(props_tmp["effective_from"], errors="coerce")
            props_tmp["event_ts"] = props_tmp["effective_from"].where(props_tmp["effective_from"].notna(), props_tmp["created_at"])
        else:
            props_tmp["event_ts"] = props_tmp["created_at"]
        # Prefer approved proposals; fall back to all
        sel = props_tmp[props_tmp["status"] == "approved"].copy()
        if sel.empty:
            sel = props_tmp.copy()
        sel = sel.dropna(subset=["event_ts"]).sort_values("event_ts")
        if not sel.empty:
            proposal_step_df = sel.rename(columns={"event_ts": "ts", "proposed_price": "price"})[["ts", "price", "status"]].copy()

    # Compute overlapping x-domain to avoid right-side compression
    domain_start = None
    domain_end = None
    try:
        plat_min = plat_rs["ts"].min() if 'plat_rs' in locals() and plat_rs is not None and not plat_rs.empty else None
        plat_max = plat_rs["ts"].max() if 'plat_rs' in locals() and plat_rs is not None and not plat_rs.empty else None
        comp_min = None
        comp_max = None
        if 'comp_plot' in locals() and comp_plot is not None and not comp_plot.empty:
            comp_min = pd.to_datetime(comp_plot["ts"], errors="coerce").min()
            comp_max = pd.to_datetime(comp_plot["ts"], errors="coerce").max()
        if plat_min is not None and plat_max is not None and comp_min is not None and comp_max is not None:
            domain_start = max(plat_min, comp_min)
            domain_end = min(plat_max, comp_max)
            if not isinstance(domain_start, pd.Timestamp) or not isinstance(domain_end, pd.Timestamp) or domain_start >= domain_end:
                domain_start, domain_end = None, None
        else:
            # Fallback to platform domain if competitors missing
            if plat_min is not None and plat_max is not None:
                domain_start, domain_end = plat_min, plat_max
            elif comp_min is not None and comp_max is not None:
                domain_start, domain_end = comp_min, comp_max
        # Extend domain to include last proposal time
        if not proposal_step_df.empty:
            last_prop_ts = pd.to_datetime(proposal_step_df["ts"], errors="coerce").max()
            if last_prop_ts is not None:
                if domain_end is None or last_prop_ts > domain_end:
                    domain_end = last_prop_ts
    except Exception:
        domain_start, domain_end = None, None

    x_enc = alt.X("ts:T", title="Time", scale=alt.Scale(domain=[domain_start, domain_end])) if domain_start is not None and domain_end is not None else alt.X("ts:T", title="Time")

    # Create layers with shared domain
    # Current price label (no line)
    if 'seed_price' in locals() and seed_price is not None:
        label_x = domain_end if domain_end is not None else now_ts
        label_df = pd.DataFrame({"ts": [label_x], "price": [float(seed_price)], "label": [f"Current: ${float(seed_price):.2f}"]})
        current_label_layer = alt.Chart(label_df).mark_text(color="#2c7be5", dx=5, dy=-5, fontWeight="bold").encode(
            x=x_enc,
            y=alt.Y("price:Q"),
            text="label"
        )
        layers.append(current_label_layer)

    if 'comp_plot' in locals() and comp_plot is not None and not comp_plot.empty:
        comp_layer = alt.Chart(comp_plot).mark_line(opacity=0.7).encode(
            x=x_enc,
            y=alt.Y("avg_price:Q", title="Price"),
            color=alt.Color("series:N", title="Competitor"),
            tooltip=["ts:T", alt.Tooltip("avg_price:Q", format=".2f"), alt.Tooltip("series:N", title="Competitor")],
        )
        layers.append(comp_layer)

    # Future approved proposals overlay (as upcoming adjustments)
    if not props.empty:
        props_plot = props.copy()
        props_plot["ts"] = pd.to_datetime(props_plot["created_at"], errors="coerce")
        if "effective_from" in props_plot.columns:
            eff = pd.to_datetime(props_plot["effective_from"], errors="coerce")
            props_plot.loc[eff.notna(), "ts"] = eff[eff.notna()]
        cutoff_ns = (pd.Timestamp.now() - pd.Timedelta(minutes=5)).value
        ts_ns = props_plot["ts"].astype("int64", errors="ignore") if hasattr(props_plot["ts"], "astype") else props_plot["ts"]
        if not isinstance(ts_ns, pd.Series) or ts_ns.dtype != "int64":
            ts_ns = pd.to_datetime(props_plot["ts"], errors="coerce").astype("int64")
        future = props_plot[(props_plot["status"].isin(["approved"])) & (ts_ns >= cutoff_ns)]
        if not future.empty:
            future_layer = alt.Chart(future).mark_point(shape="triangle-up", size=80, color="#ff6b6b").encode(
                x=alt.X("ts:T"),
                y=alt.Y("proposed_price:Q", axis=None),
                tooltip=["ts:T", alt.Tooltip("proposed_price:Q", format=".2f"), alt.Tooltip("status:N", title="Status")],
            )
            layers.append(future_layer)
        # Proposed price step line from all proposals within window
        if not proposal_step_df.empty:
            end_point_ts = domain_end if domain_end is not None else pd.Timestamp.now()
            last_price_val = float(proposal_step_df.iloc[-1]["price"]) if not proposal_step_df.empty else None
            if last_price_val is not None:
                tail_df = pd.DataFrame({"ts": [end_point_ts], "price": [last_price_val], "status": [proposal_step_df.iloc[-1]["status"]]})
                prop_line_df = pd.concat([proposal_step_df, tail_df], ignore_index=True)
            else:
                prop_line_df = proposal_step_df
            proposed_line = alt.Chart(prop_line_df).mark_line(color="green", strokeDash=[6,3], interpolate="step-after").encode(
                x=x_enc,
                y=alt.Y("price:Q", title="Price"),
                tooltip=[alt.Tooltip("ts:T", title="When"), alt.Tooltip("price:Q", title="Proposed", format=".2f"), alt.Tooltip("status:N", title="Status")]
            )
            layers.append(proposed_line)

    chart = None
    if layers:
        chart = alt.layer(*layers).resolve_scale(y="shared").properties(height=520, title=f"{sku} – Proposed vs Competitors").interactive()
        st.altair_chart(chart, use_container_width=True)

    st.caption("Recent proposals for this SKU")
    st.dataframe(props, use_container_width=True, hide_index=True)
