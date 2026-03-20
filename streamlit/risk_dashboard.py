import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt

st.set_page_config(page_title="Risk Dashboard", page_icon="\U0001f3e6", layout="wide")

session = get_active_session()

st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 2rem; color: #29B5E8; }
h1, h2, h3 { color: #1565C0; }
</style>
""", unsafe_allow_html=True)

st.title("\U0001f3e6 Risk Management Dashboard")
st.caption("Powered by Snowflake & Streamlit in Snowflake")

@st.cache_data(ttl=300)
def load_summary():
    return session.sql("""
        SELECT event_type, severity, region, month,
               event_count, total_exposure, avg_risk_score, open_events
        FROM risk_hol.analytics.risk_summary
        ORDER BY month DESC
    """).to_pandas()

df = load_summary()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", f"{df['EVENT_COUNT'].sum():,}")
col2.metric("Total Exposure", f"${df['TOTAL_EXPOSURE'].sum():,.0f}")
col3.metric("Open Events", f"{df['OPEN_EVENTS'].sum():,}")
col4.metric("Avg Risk Score", f"{df['AVG_RISK_SCORE'].mean():.1f}")

st.divider()
left, right = st.columns(2)

with left:
    st.subheader("Exposure by Risk Type")
    chart_type = df.groupby('EVENT_TYPE')['TOTAL_EXPOSURE'].sum().reset_index()
    bar = alt.Chart(chart_type).mark_bar(color='#29B5E8').encode(
        x=alt.X('TOTAL_EXPOSURE:Q', title='Total Exposure ($)'),
        y=alt.Y('EVENT_TYPE:N', sort='-x', title=''),
        tooltip=['EVENT_TYPE','TOTAL_EXPOSURE']
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)

with right:
    st.subheader("Events by Severity")
    chart_sev = df.groupby('SEVERITY')['EVENT_COUNT'].sum().reset_index()
    donut = alt.Chart(chart_sev).mark_arc(innerRadius=50).encode(
        theta='EVENT_COUNT:Q',
        color=alt.Color('SEVERITY:N', scale=alt.Scale(
            domain=['LOW','MEDIUM','HIGH','CRITICAL'],
            range=['#51cf66','#ffd43b','#ff922b','#ff6b6b']
        )),
        tooltip=['SEVERITY','EVENT_COUNT']
    ).properties(height=300)
    st.altair_chart(donut, use_container_width=True)

st.divider()
st.subheader("Monthly Risk Trend")
region_filter = st.multiselect("Filter by Region", df['REGION'].unique(), default=df['REGION'].unique())
filtered = df[df['REGION'].isin(region_filter)]
monthly = filtered.groupby(['MONTH','EVENT_TYPE'])['EVENT_COUNT'].sum().reset_index()
line = alt.Chart(monthly).mark_line(point=True, strokeWidth=2).encode(
    x=alt.X('MONTH:T', title='Month'),
    y=alt.Y('EVENT_COUNT:Q', title='Event Count'),
    color='EVENT_TYPE:N',
    tooltip=['MONTH','EVENT_TYPE','EVENT_COUNT']
).properties(height=350)
st.altair_chart(line, use_container_width=True)

st.divider()
st.subheader("Detailed Data")
st.dataframe(filtered, use_container_width=True)
st.caption("Data is synthetic and for demonstration purposes only.")
