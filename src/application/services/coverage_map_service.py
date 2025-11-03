"""
Coverage Map Visualization Module - Updated with 3 Steps
Place this file at: src/application/services/coverage_map_service.py
"""

import folium
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import Fullscreen
import polars as pl
import math
from typing import List, Tuple, Optional


class CoverageMapVisualization:
    """Clean visualization for cell coverage with 3-step approach"""

    def __init__(self):
        self.map = None
        self.map_center = None
        self.cell_colors = {}  # Untuk warna berbeda per CellName pattern

    def initialize_map(self, df_coverage: pl.DataFrame):
        """Initialize Folium map centered on cells"""
        try:
            valid_cells = df_coverage.filter(
                (pl.col("Latitude").is_not_null()) & (pl.col("Longitude").is_not_null())
            )

            if valid_cells.is_empty():
                self.map_center = (0, 0)
            else:
                lat_mean = valid_cells["Latitude"].mean()
                lon_mean = valid_cells["Longitude"].mean()
                self.map_center = (lat_mean, lon_mean)

        except Exception as e:
            print(f"Error calculating center: {e}")
            self.map_center = (0, 0)

        # Create map with Google Hybrid tiles
        self.map = folium.Map(
            location=self.map_center,
            zoom_start=14,
            tiles=None,
            control_scale=True,
            prefer_canvas=True,
        )

        folium.TileLayer(
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google Hybrid",
            name="Satellite Hybrid",
            control=True,
        ).add_to(self.map)

    def assign_cell_colors(self, df: pl.DataFrame):
        """Assign different colors for each CellName pattern"""
        # Extract unique cell patterns (first part of CellName before underscore)
        cell_patterns = set()
        for cell_name in df["CellName"].to_list():
            if cell_name and "_" in str(cell_name):
                pattern = str(cell_name).split("_")[0]  # AC4G18, AC4G85, etc.
                cell_patterns.add(pattern)
            else:
                cell_patterns.add(str(cell_name))

        unique_patterns = sorted(list(cell_patterns))

        # Enhanced color palette for cell patterns
        cell_colors = [
            "#E74C3C",
            "#3498DB",
            "#2ECC71",
            "#F39C12",
            "#9B59B6",
            "#1ABC9C",
            "#E67E22",
            "#34495E",
            "#E91E63",
            "#FF5722",
            "#00BCD4",
            "#8BC34A",
            "#FFC107",
            "#795548",
            "#607D8B",
            "#FF6B6B",
            "#4ECDC4",
            "#45B7D1",
            "#96CEB4",
            "#FFEAA7",
        ]

        self.cell_colors = {}
        for i, pattern in enumerate(unique_patterns):
            if i < len(cell_colors):
                self.cell_colors[pattern] = cell_colors[i]
            else:
                # Generate color if we have more patterns than predefined colors
                import hashlib

                color_hex = hashlib.md5(pattern.encode()).hexdigest()[:6]
                self.cell_colors[pattern] = f"#{color_hex}"

    def get_cell_color(self, cell_name: str) -> str:
        """Get color for a cell based on its name pattern"""
        if not cell_name:
            return "#95A5A6"

        cell_str = str(cell_name)
        if "_" in cell_str:
            pattern = cell_str.split("_")[0]  # AC4G18, AC4G85, etc.
            return self.cell_colors.get(pattern, "#95A5A6")
        else:
            return self.cell_colors.get(cell_str, "#95A5A6")

    def add_coverage_layers_3step(self, df_coverage: pl.DataFrame):
        """Add coverage layers in 3 steps as requested with updated layer order"""
        if df_coverage.is_empty():
            return

        # Filter cells with coordinates
        df_valid = df_coverage.filter(
            (pl.col("Latitude").is_not_null()) & (pl.col("Longitude").is_not_null())
        )

        # Assign cell colors
        self.assign_cell_colors(df_valid)

        # UPDATED LAYER ORDER:
        # 1. TA90 Coverage (paling awal - di bawah)
        self._add_step2_ta90_coverage(df_valid)

        # 2. Beam Coverage (Ant-Size)
        self._add_step1_beam_coverage(df_valid)

        # 3. Tier1 Connections (paling atas)
        self._add_step3_tier1_connections(df_valid)

    def _add_step1_beam_coverage(self, df: pl.DataFrame):
        """STEP 1: Draw beam coverage using Ant-Size as radius, coloring by CellName, color 100%"""
        layer = folium.FeatureGroup(name="📡 Beam Coverage", show=True)

        for row in df.iter_rows(named=True):
            try:
                lat = row["Latitude"]
                lon = row["Longitude"]
                cell_name = row["CellName"]
                band = str(row["Band"])
                direction = row.get("Dir", 0)
                beam = row.get("Beam", 65)
                ant_size = row.get("Ant-Size", 0.1)
                msc_name = row["MSC"]

                # Use Ant-Size as radius (convert to km if needed)
                coverage_km = ant_size  # Assuming Ant-Size is already in km

                if coverage_km <= 0:
                    continue

                color = self.get_cell_color(cell_name)

                # Create sector coverage polygon
                polygon_coords = self._create_sector_polygon(
                    lat, lon, direction, beam, coverage_km
                )

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>📡 Beam GCELL</b><br>
                    <b>Cell:</b> {cell_name}<br>
                    <b>MSC:</b> {msc_name}<br>
                    <b>Band:</b> L{band}<br>
                    <b>Direction:</b> {direction}°<br>
                    <b>Beam Width:</b> {beam}°<br>
                    <b>Ant-Size Radius:</b> {coverage_km:.3f} km<br>
                    <b>Color:</b> Based on Cell Pattern
                </div>
                """

                # Add coverage area dengan opacity 100% (1.0)
                folium.Polygon(
                    locations=polygon_coords,
                    color=color,
                    weight=2,
                    opacity=0.8,  # Border opacity
                    fill=True,
                    fill_color=color,
                    fill_opacity=1.0,  # 100% fill opacity
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"Beam: {coverage_km:.3f} km ({cell_name})",
                ).add_to(layer)

                # Add cell marker dengan label MSC at center and CellName offset
                self._add_cell_marker_with_label(
                    lat, lon, cell_name, msc_name, color, layer, direction
                )

            except Exception as e:
                print(
                    f"Error creating beam coverage for {row.get('CellName', 'Unknown')}: {e}"
                )

        layer.add_to(self.map)

    def _add_step2_ta90_coverage(self, df: pl.DataFrame):
        """STEP 2: Draw TA90 coverage using TA90 as radius, coloring by CellName, color 20%"""
        layer = folium.FeatureGroup(name="📊 TA90 Coverage", show=True)

        # Filter cells with TA90 values
        ta90_cells = df.filter((pl.col("TA90").is_not_null()) & (pl.col("TA90") > 0))

        if ta90_cells.is_empty():
            print("No TA90 data available for STEP 2")
            return

        for row in ta90_cells.iter_rows(named=True):
            try:
                lat = row["Latitude"]
                lon = row["Longitude"]
                cell_name = row["CellName"]
                band = str(row["Band"])
                direction = row.get("Dir", 0)
                beam = row.get("Beam", 65)
                ta90_value = row.get("TA90", 0)
                msc_name = row["MSC"]

                # Use TA90 as radius (already in km)
                coverage_km = ta90_value

                if coverage_km <= 0:
                    continue

                color = self.get_cell_color(cell_name)

                # Create sector coverage polygon
                polygon_coords = self._create_sector_polygon(
                    lat, lon, direction, beam, coverage_km
                )

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>📊 TA90 Coverage - STEP 2</b><br>
                    <b>Cell:</b> {cell_name}<br>
                    <b>MSC:</b> {msc_name}<br>
                    <b>Band:</b> L{band}<br>
                    <b>Direction:</b> {direction}°<br>
                    <b>Beam Width:</b> {beam}°<br>
                    <b>TA90 Radius:</b> {coverage_km:.3f} km<br>
                    <b>Color:</b> Based on Cell Pattern (20% opacity)
                </div>
                """

                # Add coverage area dengan opacity 20% (0.2)
                folium.Polygon(
                    locations=polygon_coords,
                    color=color,
                    weight=1.5,
                    opacity=0.6,  # Border opacity
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.2,  # 20% fill opacity
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"TA90: {coverage_km:.3f} km ({cell_name})",
                ).add_to(layer)

            except Exception as e:
                print(
                    f"Error creating TA90 coverage for {row.get('CellName', 'Unknown')}: {e}"
                )

        layer.add_to(self.map)

    def _add_step3_tier1_connections(self, df: pl.DataFrame):
        """STEP 3: Draw polyline with 1st Tier connections, fixed red color with dashdot"""
        layer = folium.FeatureGroup(name="🔗 ISD", show=True)

        # Find cells that have 1st tier connections
        tier1_connections = df.filter(
            (pl.col("1st Tier").is_not_null())
            & (pl.col("1st Tier") != "")
            & (pl.col("1st Tier") != "1st Tier")  # Exclude self-references
        )

        if tier1_connections.is_empty():
            print("No tier1 connections found for STEP 3")
            return

        connection_count = 0

        for source_row in tier1_connections.iter_rows(named=True):
            source_cell = source_row["CellName"]
            tier1_site = source_row["1st Tier"]
            source_msc = source_row["MSC"]
            source_band = str(source_row["Band"])

            # Find target cells in the target MSC
            target_cells = df.filter((pl.col("MSC") == tier1_site))

            if not target_cells.is_empty():
                # Use the first cell from target MSC as connection point
                target_row = target_cells.row(0, named=True)

                try:
                    lat1, lon1 = source_row["Latitude"], source_row["Longitude"]
                    lat2, lon2 = target_row["Latitude"], target_row["Longitude"]

                    # Calculate offset (40m = ~0.00036 degrees)
                    offset = 0.00036

                    # Create offset points for curved line
                    mid_lat = (lat1 + lat2) / 2 + offset
                    mid_lon = (lon1 + lon2) / 2 + offset

                    line_coords = [(lat1, lon1), (mid_lat, mid_lon), (lat2, lon2)]

                    distance_km = self._calculate_distance(lat1, lon1, lat2, lon2)

                    popup_html = f"""
                    <div style='font-family: Arial; font-size: 12px;'>
                        <b>🔗 Tier1 Connection - STEP 3</b><br>
                        <b>From:</b> {source_cell}<br>
                        <b>Source MSC:</b> {source_msc}<br>
                        <b>To MSC:</b> {tier1_site}<br>
                        <b>Target Cell:</b> {target_row["CellName"]}<br>
                        <b>Distance:</b> {distance_km:.2f} km<br>
                        <b>Style:</b> Red dashdot line
                    </div>
                    """

                    # Draw polyline dengan fixed red color dan dashdot style
                    folium.PolyLine(
                        locations=line_coords,
                        color="#FF0000",  # Fixed red color
                        weight=3,
                        opacity=0.8,
                        dash_array="10, 5, 2, 5",  # Dashdot pattern
                        popup=folium.Popup(popup_html, max_width=300),
                    ).add_to(layer)

                    # Add distance label di tengah polyline
                    self._add_distance_label(mid_lat, mid_lon, distance_km, layer)

                    connection_count += 1

                except Exception as e:
                    print(f"Error drawing tier1 connection from {source_cell}: {e}")

        print(f"STEP 3: Added {connection_count} tier1 connections")
        layer.add_to(self.map)

    def _add_cell_marker_with_label(
        self,
        lat: float,
        lon: float,
        cell_name: str,
        msc_name: str,
        color: str,
        layer,
        direction: float,
    ):
        """Add cell marker dengan MSC label at center and CellName label offset outside beam based on Azimuth (Dir)"""
        # MSC and Cell marker at center
        popup_html = f"""
        <div style='font-family: Arial; font-size: 12px; min-width: 200px;'>
            <b>📍 Cell</b><br>
            <b>MSC:</b> {msc_name}<br>
            <b>Location:</b> {lat:.6f}, {lon:.6f}
        </div>
        """

        folium.CircleMarker(
            location=(lat, lon),
            radius=8,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{cell_name}",
            color="white",
            weight=2,
            opacity=0.8,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
        ).add_to(layer)

        # MSC label at center - small label below the marker
        msc_label_lat = lat  # Offset slightly below
        msc_label_html = f"""
        <div style='
            font-family: Arial; 
            font-size: 12px; 
            font-weight: bold;
            color: #FF0000; 
            background-color: rgba(255,255,255,0.95);
            border: 1px solid #FF0000;
            border-radius: 3px;
            padding: 1px 3px;
            white-space: nowrap;
            text-align: center;
        '>
            {msc_name}
        </div>
        """

        folium.Marker(
            location=(msc_label_lat, lon),
            icon=folium.DivIcon(
                html=msc_label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=f"{msc_name}",
        ).add_to(layer)

        # CellName label offset outside beam based on Azimuth (Dir)
        # Use a fixed offset distance along the direction (Azimuth = Dir)
        # Beam is used contextually for sector, but offset is along centerline (Dir)
        offset_km = 1.5  # ~200m offset outside center, adjustable
        angle_rad = math.radians(direction)

        # Convert km to degrees for offset
        delta_lat = offset_km * math.cos(angle_rad) / 111.0
        delta_lon = (
            offset_km * math.sin(angle_rad) / (111.0 * math.cos(math.radians(lat)))
        )

        label_lat = lat + delta_lat
        label_lon = lon + delta_lon

        label_html = f"""
        <div style='
            font-family: Arial; 
            font-size: 7px; 
            font-weight: bold;
            color: {color}; 
            background-color: rgba(255,255,255,0.95);
            border: 1px solid {color};
            border-radius: 3px;
            padding: 2px 4px;
            white-space: nowrap;
            text-align: center;
        '>
            {cell_name}
        </div>
        """

        folium.Marker(
            location=(label_lat, label_lon),
            icon=folium.DivIcon(
                html=label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=f"{cell_name}",
        ).add_to(layer)

    def _add_distance_label(self, lat: float, lon: float, distance_km: float, layer):
        """Add distance label pada polyline connections"""
        # Position sedikit lebih tinggi dari garis untuk menghindari overlap
        label_lat = lat + 0.0002
        label_lon = lon + 0.0002

        label_html = f"""
        <div style='
            font-family: Arial; 
            font-size: 10px; 
            font-weight: bold;
            color: #FF0000; 
            background-color: rgba(255,255,255,0.95);
            border: 2px solid #FF0000;
            border-radius: 4px;
            padding: 3px 6px;
            white-space: nowrap;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        '>
            {distance_km:.1f} km
        </div>
        """

        folium.Marker(
            location=(label_lat, label_lon),
            icon=folium.DivIcon(
                html=label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=f"Connection Distance: {distance_km:.1f} km",
        ).add_to(layer)

    def _create_sector_polygon(
        self, lat: float, lon: float, direction: float, beam: float, radius_km: float
    ) -> List[Tuple[float, float]]:
        """Create sector polygon coordinates for coverage area"""
        points = []
        points.append((lat, lon))  # Center point

        start_angle = direction - beam / 2
        end_angle = direction + beam / 2

        # Create arc dengan lebih banyak points untuk kurva yang smooth
        for angle in range(int(start_angle), int(end_angle) + 1, 2):
            angle_rad = math.radians(angle)

            # Convert km to degrees
            delta_lat = radius_km * math.cos(angle_rad) / 111.0
            delta_lon = (
                radius_km * math.sin(angle_rad) / (111.0 * math.cos(math.radians(lat)))
            )

            point_lat = lat + delta_lat
            point_lon = lon + delta_lon
            points.append((point_lat, point_lon))

        points.append((lat, lon))  # Close polygon
        return points

    def _calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two points in km"""
        R = 6371  # Earth radius in km

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def display(self):
        """Display map in Streamlit"""
        folium.LayerControl(position="topright", collapsed=False).add_to(self.map)

        try:
            Fullscreen().add_to(self.map)
        except:
            pass

        st.components.v1.html(self.map._repr_html_(), height=850, scrolling=False)


def render_coverage_map_3step(results: dict):
    """
    Render coverage map with 3-step approach from dashboard results
    """
    # st.subheader("🗺️ 3-Step Coverage Map Visualization")

    df_coverage = results.get("gcell_coverage")

    if df_coverage is None or df_coverage.is_empty():
        st.warning("⚠️ No coverage data available. GCell Coverage merge required.")
        return

    with st.spinner("Generating 3-step coverage map..."):
        viz = CoverageMapVisualization()
        viz.initialize_map(df_coverage)
        viz.add_coverage_layers_3step(df_coverage)
        # viz.add_simple_legend()  # Simplified legend
        viz.display()

    st.markdown("---")


# Update the main render function to use the 3-step approach
def render_coverage_map(results: dict):
    """
    Main function - now uses 3-step approach
    """
    render_coverage_map_3step(results)
