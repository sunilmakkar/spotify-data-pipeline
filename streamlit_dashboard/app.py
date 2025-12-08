import streamlit as st
import plotly.express as px
from utils import run_query

# Page configuration
st.set_page_config(
    page_title="Spotify Data Pipeline Dashboard",
    page_icon="🎵",
    layout="wide"
)

# ============================================================================
# PAGE FUNCTIONS
# ============================================================================

def show_overview():
    """Display high-level summary metrics."""
    st.header("📊 Overview")
    
    # Query summary metrics
    query = """
        SELECT 
            SUM(total_plays) as total_plays,
            SUM(unique_tracks) as unique_tracks,
            SUM(unique_artists) as unique_artists,
            SUM(total_listening_time_ms) / 1000 / 60 as total_minutes
        FROM SPOTIFY_DATA.GOLD.daily_user_stats
    """
    df = run_query(query)
    
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Plays", f"{int(df['TOTAL_PLAYS'][0]):,}")
    
    with col2:
        st.metric("Unique Tracks", int(df['UNIQUE_TRACKS'][0]))
    
    with col3:
        st.metric("Unique Artists", int(df['UNIQUE_ARTISTS'][0]))
    
    with col4:
        st.metric("Listening Time", f"{int(df['TOTAL_MINUTES'][0])} min")
    
    st.markdown("---")
    
    # Show date range
    date_query = """
        SELECT 
            MIN(date) as first_date,
            MAX(date) as last_date
        FROM SPOTIFY_DATA.GOLD.daily_user_stats
    """
    date_df = run_query(date_query)
    st.info(f"📅 Data Range: {date_df['FIRST_DATE'][0]} to {date_df['LAST_DATE'][0]}")


def show_trends():
    """Display listening trends over time."""
    st.header("📈 Trends")
    
    # Query daily stats
    query = """
        SELECT 
            date,
            total_plays,
            total_listening_time_ms / 1000 / 60 / 60 as listening_hours
        FROM SPOTIFY_DATA.GOLD.daily_user_stats
        ORDER BY date
    """
    df = run_query(query)
    
    # Plays over time
    st.subheader("Plays Per Day")
    fig_plays = px.line(
        df,
        x='DATE',
        y='TOTAL_PLAYS',
        markers=True,
        title="Daily Play Count"
    )
    fig_plays.update_layout(xaxis_title="Date", yaxis_title="Plays")
    st.plotly_chart(fig_plays, use_container_width=True)
    
    # Listening time over time
    st.subheader("Listening Time Per Day")
    fig_time = px.line(
        df,
        x='DATE',
        y='LISTENING_HOURS',
        markers=True,
        title="Daily Listening Time (Hours)"
    )
    fig_time.update_layout(xaxis_title="Date", yaxis_title="Hours")
    st.plotly_chart(fig_time, use_container_width=True)


def show_top_tracks():
    """Display top tracks by play count."""
    st.header("🎵 Top Tracks")
    
    # Query top tracks
    query = """
        SELECT 
            track_name,
            artist_name,
            total_plays,
            rank
        FROM SPOTIFY_DATA.GOLD.top_tracks
        ORDER BY rank
    """
    df = run_query(query)
    
    # Create horizontal bar chart
    fig = px.bar(
        df,
        x='TOTAL_PLAYS',
        y='TRACK_NAME',
        orientation='h',
        title="Top 5 Most Played Tracks",
        hover_data=['ARTIST_NAME'],
        color='TOTAL_PLAYS',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title="Play Count",
        yaxis_title=""
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Show data table
    st.subheader("Track Details")
    st.dataframe(
        df[['RANK', 'TRACK_NAME', 'ARTIST_NAME', 'TOTAL_PLAYS']],
        use_container_width=True,
        hide_index=True
    )


def show_top_artists():
    """Display top artists by play count."""
    st.header("🎤 Top Artists")
    
    # Query top artists
    query = """
        SELECT 
            artist_name,
            total_plays,
            unique_tracks,
            rank
        FROM SPOTIFY_DATA.GOLD.top_artists
        ORDER BY rank
    """
    df = run_query(query)
    
    # Create horizontal bar chart
    fig = px.bar(
        df,
        x='TOTAL_PLAYS',
        y='ARTIST_NAME',
        orientation='h',
        title="Top 5 Most Played Artists",
        hover_data=['UNIQUE_TRACKS'],
        color='TOTAL_PLAYS',
        color_continuous_scale='Plasma'
    )
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title="Play Count",
        yaxis_title=""
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Show data table
    st.subheader("Artist Details")
    st.dataframe(
        df[['RANK', 'ARTIST_NAME', 'TOTAL_PLAYS', 'UNIQUE_TRACKS']],
        use_container_width=True,
        hide_index=True
    )


def show_device_usage():
    """Display listening breakdown by device type."""
    st.header("📱 Device Usage")
    
    # Query device usage
    query = """
        SELECT 
            device_type,
            total_plays,
            play_percentage
        FROM SPOTIFY_DATA.GOLD.device_usage
        ORDER BY total_plays DESC
    """
    df = run_query(query)
    
    # Create two columns
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart
        st.subheader("Usage Distribution")
        fig_pie = px.pie(
            df,
            values='TOTAL_PLAYS',
            names='DEVICE_TYPE',
            title="Plays by Device Type"
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Bar chart
        st.subheader("Play Count by Device")
        fig_bar = px.bar(
            df,
            x='DEVICE_TYPE',
            y='TOTAL_PLAYS',
            title="Device Play Counts",
            color='DEVICE_TYPE'
        )
        fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="Plays")
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Show data table
    st.subheader("Device Details")
    st.dataframe(
        df[['DEVICE_TYPE', 'TOTAL_PLAYS', 'PLAY_PERCENTAGE']],
        use_container_width=True,
        hide_index=True
    )

# ============================================================================
# MAIN APP (Navigation and Router)
# ============================================================================

# Sidebar navigation
st.sidebar.title("🎵 Navigation")
page = st.sidebar.radio(
    "Select a page:",
    ["Overview", "Trends", "Top Tracks", "Top Artists", "Device Usage"]
)

# Main title
st.title("🎵 Spotify Data Pipeline Dashboard")
st.markdown("---")

# Page router
if page == "Overview":
    show_overview()
elif page == "Trends":
    show_trends()
elif page == "Top Tracks":
    show_top_tracks()
elif page == "Top Artists":
    show_top_artists()
elif page == "Device Usage":
    show_device_usage()