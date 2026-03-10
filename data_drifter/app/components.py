"""
UI rendering components for the Data Drifter Regatta app.

Usage:
    import components
    components.init(config, boat_colors)
    folium_map = components.create_race_map(df)
    components.display_leaderboard(df)
    components.display_boat_stats(df)
"""

import streamlit as st
import pandas as pd
import folium
from folium import plugins
from navigation_utils import calculate_distance

# Module-level state set by init()
_config = None
_boat_colors = None


def init(config, boat_colors):
    """Initialize the components module with config and boat colors."""
    global _config, _boat_colors
    _config = config
    _boat_colors = boat_colors


def get_race_course_config():
    """Get race course configuration (marks, start/finish) from config"""
    return {
        "start_lat": _config["race_course_start_lat"],
        "start_lon": _config["race_course_start_lon"],
        "marks": _config["race_course_marks"]
    }


def calculate_total_remaining_distance(lat, lon, current_mark_index, marks):
    """
    Calculate total remaining distance along the race course in nautical miles.

    Args:
        lat: Current latitude
        lon: Current longitude
        current_mark_index: Index of the next mark to round (0-based)
        marks: List of all marks [[lat1, lon1], [lat2, lon2], ...] where last mark is finish

    Returns:
        Total distance remaining along the course in nautical miles
    """
    if not marks or len(marks) == 0:
        return 0.0

    total_distance = 0.0

    # Marks to round are all except the last one (which is the finish line)
    marks_to_round = marks[:-1] if len(marks) > 1 else []
    finish_lat, finish_lon = marks[-1][0], marks[-1][1]

    # If all marks are rounded, just return distance to finish
    if current_mark_index >= len(marks_to_round):
        return calculate_distance(lat, lon, finish_lat, finish_lon)

    # Add distance to next mark
    next_mark = marks_to_round[current_mark_index]
    total_distance += calculate_distance(lat, lon, next_mark[0], next_mark[1])

    # Add distances between subsequent marks
    for i in range(current_mark_index, len(marks_to_round) - 1):
        mark1 = marks_to_round[i]
        mark2 = marks_to_round[i + 1]
        total_distance += calculate_distance(mark1[0], mark1[1], mark2[0], mark2[1])

    # Add distance from last mark to finish
    if len(marks_to_round) > 0:
        last_mark = marks_to_round[-1]
        total_distance += calculate_distance(last_mark[0], last_mark[1], finish_lat, finish_lon)

    return total_distance


def create_race_map(df):
    """Create interactive race map with boat tracks using Folium"""
    if df is None or len(df) == 0:
        st.warning("No telemetry data available")
        return None

    # Convert timestamp from epoch microseconds to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='us')

    # Get race course configuration
    course_config = get_race_course_config()

    # Get race course marks
    mark_lats = [m[0] for m in course_config['marks']]
    mark_lons = [m[1] for m in course_config['marks']]

    # Center map on current boat positions (centroid of fleet)
    latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()
    racing_boats = latest_positions[latest_positions['race_status'].isin(['racing', 'not_started'])]
    if len(racing_boats) > 0:
        center_lat = racing_boats['latitude'].mean()
        center_lon = racing_boats['longitude'].mean()
    else:
        # All finished/DNF — center on all boats
        center_lat = latest_positions['latitude'].mean()
        center_lon = latest_positions['longitude'].mean()

    # Auto-fit zoom to show all current boat positions
    lat_spread = latest_positions['latitude'].max() - latest_positions['latitude'].min()
    lon_spread = latest_positions['longitude'].max() - latest_positions['longitude'].min()
    spread = max(lat_spread, lon_spread)
    if spread < 0.05:
        zoom = 13
    elif spread < 0.2:
        zoom = 11
    elif spread < 0.5:
        zoom = 10
    elif spread < 1.5:
        zoom = 9
    elif spread < 3.0:
        zoom = 8
    else:
        zoom = 7

    # Create Folium map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles='OpenStreetMap',
        prefer_canvas=True
    )

    # Add race course line connecting marks
    course_coords = [[lat, lon] for lat, lon in zip(mark_lats, mark_lons)]
    folium.PolyLine(
        course_coords,
        color='gray',
        weight=2,
        opacity=0.7,
        dash_array='10, 5',
        popup='Race Course'
    ).add_to(m)

    # Add markers for each race course mark
    for idx, (lat, lon) in enumerate(zip(mark_lats, mark_lons)):
        folium.Marker(
            location=[lat, lon],
            popup=f'Mark {idx + 1}',
            tooltip=f'Mark {idx + 1}',
            icon=folium.Icon(color='orange', icon='flag', prefix='fa')
        ).add_to(m)

    # Add start line marker
    folium.Marker(
        location=[course_config['start_lat'], course_config['start_lon']],
        popup='START',
        tooltip='Start Line',
        icon=folium.Icon(color='green', icon='play', prefix='fa')
    ).add_to(m)

    # Add finish line marker (last mark)
    finish_lat, finish_lon = course_config['marks'][-1]
    folium.Marker(
        location=[finish_lat, finish_lon],
        popup='FINISH',
        tooltip='Finish Line',
        icon=folium.Icon(color='red', icon='stop', prefix='fa')
    ).add_to(m)

    # Group by boat and plot tracks
    boats = df.groupby('boat_id')

    for idx, (boat_id, boat_df) in enumerate(boats):
        boat_df = boat_df.sort_values('timestamp')

        boat_name = boat_df['boat_name'].iloc[0]
        boat_type = boat_df['boat_type'].iloc[0]
        race_status = boat_df['race_status'].iloc[0]

        color = _boat_colors[idx % len(_boat_colors)]

        # Create boat track coordinates
        track_coords = [[row['latitude'], row['longitude']] for _, row in boat_df.iterrows()]

        # Add boat track as polyline
        folium.PolyLine(
            track_coords,
            color=color,
            weight=3,
            opacity=0.8,
            popup=boat_name,
            tooltip=boat_name
        ).add_to(m)

        # Add current position marker (most recent point - first in DESC order)
        latest = boat_df.iloc[-1]  # Last point after sorting by timestamp ASC

        # Create popup content with boat information
        popup_html = f"""
        <div style="font-family: Arial; font-size: 12px;">
            <b>{boat_name}</b><br>
            Status: {race_status}<br>
            Speed: {latest['speed_over_ground_knots']:.1f} knots<br>
            Heading: {latest['heading_degrees']:.0f}<br>
            Distance: {latest['distance_traveled_nm']:.1f} nm<br>
            VMG: {latest['vmg_knots']:.1f} knots<br>
            Marks: {latest['marks_rounded']}/{latest['total_marks']}<br>
            Time: {pd.to_datetime(latest['timestamp'], unit='us').strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        """

        # Get heading for boat marker
        heading = latest['heading_degrees']

        # Use BoatMarker for racing boats, regular markers for finished/DNF
        if race_status == 'finished':
            folium.Marker(
                location=[latest['latitude'], latest['longitude']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0],
                icon=folium.Icon(color='lightgreen', icon='star', prefix='fa')
            ).add_to(m)
        elif race_status == 'dnf':
            folium.Marker(
                location=[latest['latitude'], latest['longitude']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0],
                icon=folium.Icon(color='red', icon='times', prefix='fa')
            ).add_to(m)
        else:
            # Use BoatMarker for racing boats with rounded SVG shape
            plugins.BoatMarker(
                location=[latest['latitude'], latest['longitude']],
                heading=heading,
                wind_heading=latest.get('wind_direction_degrees', heading + 45),
                wind_speed=latest.get('wind_speed_knots', 10),
                color=color,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=boat_name.split("'s")[0]
            ).add_to(m)

    # Add wind indicator in top left corner
    latest_positions_for_wind = df.sort_values('timestamp').groupby('boat_id').last()
    avg_wind_speed = latest_positions_for_wind['wind_speed_knots'].mean()
    avg_wind_direction = latest_positions_for_wind['wind_direction_degrees'].mean()

    # Create wind direction name
    def get_wind_direction_name(degrees):
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        index = int((degrees + 11.25) / 22.5) % 16
        return directions[index]

    wind_dir_name = get_wind_direction_name(avg_wind_direction)

    # Create custom HTML for wind indicator
    wind_html = f"""
    <div style="
        position: fixed;
        top: 80px;
        left: 10px;
        z-index: 1000;
        background-color: rgba(255, 255, 255, 0.95);
        padding: 12px 16px;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        font-family: Arial, sans-serif;
        border: 2px solid #1e40af;
    ">
        <div style="text-align: center;">
            <div style="font-size: 11px; font-weight: bold; color: #1e40af; margin-bottom: 4px;">
                WIND
            </div>
            <div style="
                font-size: 32px;
                transform: rotate({avg_wind_direction}deg);
                color: #1e40af;
                line-height: 1;
                margin: 4px 0;
            ">
                &darr;
            </div>
            <div style="font-size: 14px; font-weight: bold; color: #1e3a8a; margin-top: 4px;">
                {avg_wind_speed:.1f} kts
            </div>
            <div style="font-size: 11px; color: #64748b; margin-top: 2px;">
                {wind_dir_name} ({avg_wind_direction:.0f})
            </div>
        </div>
    </div>
    """

    # Add the wind indicator to the map
    m.get_root().html.add_child(folium.Element(wind_html))

    return m


def display_leaderboard(df):
    """Display leaderboard with all boats"""
    if df is None or len(df) == 0:
        st.info("No race data available")
        return

    # Get latest position for each boat
    latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()

    # Get race course marks
    course_config = get_race_course_config()
    marks = course_config['marks']

    # Calculate total remaining distance along the race course for each boat
    latest_positions['total_remaining_distance'] = latest_positions.apply(
        lambda row: calculate_total_remaining_distance(
            row['latitude'], row['longitude'], row['current_mark_index'], marks
        ),
        axis=1
    )

    # Sort by total remaining distance (less distance = better rank)
    # DNF boats go to the end
    latest_positions['sort_key'] = latest_positions.apply(
        lambda row: (
            float('inf') if row['race_status'] == 'dnf' else row['total_remaining_distance']
        ),
        axis=1
    )
    latest_positions = latest_positions.sort_values('sort_key')

    # Display leaderboard header
    st.markdown("##### Race Leaderboard")

    # Create container for scrollable leaderboard
    leaderboard_container = st.container(height=600)

    with leaderboard_container:
        # Display each boat with rank, name, and distance
        for rank, (idx, row) in enumerate(latest_positions.iterrows(), start=1):
            boat_id = row['boat_id']
            boat_name = row['boat_name']
            distance = row['distance_traveled_nm']

            # Create columns for rank and boat info
            col1, col2 = st.columns([0.5, 4.5])

            with col1:
                # Rank with medal emoji for top 3
                if rank == 1:
                    st.markdown(f"### 🥇")
                elif rank == 2:
                    st.markdown(f"### 🥈")
                elif rank == 3:
                    st.markdown(f"### 🥉")
                else:
                    st.markdown(f"### **{rank}**")

            with col2:
                # Boat name and distance
                st.markdown(f"**{boat_name}**")
                st.caption(f"{distance:.1f} nm")

            # Add divider between boats
            if rank < len(latest_positions):
                st.divider()


def display_boat_stats(df):
    """Display boat statistics table"""
    if df is None or len(df) == 0:
        return

    # Get latest position for each boat
    latest_positions = df.sort_values('timestamp').groupby('boat_id').last().reset_index()

    # Get race course marks
    course_config = get_race_course_config()
    marks = course_config['marks']

    # Calculate total remaining distance along the race course for each boat
    latest_positions['total_remaining_distance'] = latest_positions.apply(
        lambda row: calculate_total_remaining_distance(
            row['latitude'], row['longitude'], row['current_mark_index'], marks
        ),
        axis=1
    )

    # Sort by total remaining distance (ascending) - boat with least distance is in 1st place
    # DNF boats go to the end
    latest_positions['sort_key'] = latest_positions.apply(
        lambda row: (
            float('inf') if row['race_status'] == 'dnf' else row['total_remaining_distance']
        ),
        axis=1
    )
    latest_positions = latest_positions.sort_values('sort_key')

    # Create display dataframe with the new total_remaining_distance column
    display_df = latest_positions[[
        'boat_name', 'boat_type', 'race_status', 'speed_over_ground_knots',
        'distance_traveled_nm', 'total_remaining_distance', 'vmg_knots',
        'marks_rounded', 'total_marks'
    ]].copy()

    display_df.columns = [
        'Boat', 'Type', 'Status', 'Speed (kt)', 'Distance (nm)',
        'To Dest (nm)', 'VMG (kt)', 'Marks', 'Total Marks'
    ]

    # Add position column
    display_df.insert(0, 'Pos', range(1, len(display_df) + 1))

    # Format marks column
    display_df['Marks'] = display_df.apply(lambda row: f"{row['Marks']}/{row['Total Marks']}", axis=1)
    display_df = display_df.drop('Total Marks', axis=1)

    # Round numeric columns
    display_df['Speed (kt)'] = display_df['Speed (kt)'].round(1)
    display_df['Distance (nm)'] = display_df['Distance (nm)'].round(1)
    display_df['To Dest (nm)'] = display_df['To Dest (nm)'].round(1)
    display_df['VMG (kt)'] = display_df['VMG (kt)'].round(1)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn(
                "Status",
                help="Current race status",
            )
        }
    )
