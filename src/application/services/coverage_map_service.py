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
from typing import List, Tuple


class CoverageMapVisualization:
    """Clean visualization for cell coverage with 3-step approach"""

    def __init__(self):
        self.map = None
        self.map_center = None
        self.cell_colors = {}

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

        except Exception:
            self.map_center = (0, 0)

        self.map = folium.Map(
            location=self.map_center,
            zoom_start=12,
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
        """Assign different colors for each unique CellName"""
        cell_names = df["CellName"].unique().to_list()
        unique_cell_names = [str(name) for name in cell_names if name]

        cell_colors = [
            "#E74C3C",
            "#3498DB",
            "#2ECC71",
            "#F39C12",
            "#D4C32A",
            "#1ABC9C",
            "#E67E22",
            "#DB1248",
            "#E91E63",
            "#FF5722",
            "#00BCD4",
            "#8BC34A",
            "#FFC107",
            "#795548",
            "#0FEF2D",
            "#FF6B6B",
            "#4ECDC4",
            "#45B7D1",
            "#96CEB4",
            "#FFEAA7",
        ]

        self.cell_colors = {}
        for i, cell_name in enumerate(unique_cell_names):
            if i < len(cell_colors):
                self.cell_colors[cell_name] = cell_colors[i]
            else:
                import hashlib

                color_hex = hashlib.md5(cell_name.encode()).hexdigest()[:6]
                self.cell_colors[cell_name] = f"#{color_hex}"

    def get_cell_color(self, cell_name: str) -> str:
        """Get color for a cell based on its full name"""
        if not cell_name:
            return "#95A5A6"
        return self.cell_colors.get(str(cell_name), "#95A5A6")

    def add_coverage_layers_3step(self, df_coverage: pl.DataFrame):
        """Add coverage layers in 3 steps with updated layer order"""
        if df_coverage.is_empty():
            return

        df_valid = df_coverage.filter(
            (pl.col("Latitude").is_not_null()) & (pl.col("Longitude").is_not_null())
        )

        self.assign_cell_colors(df_valid)

        # Layer order: TA90 -> Beam -> Tier1 Connections
        self._add_step2_ta90_coverage(df_valid)
        self._add_step1_beam_coverage(df_valid)
        self._add_step3_tier1_connections(df_valid)

    def _add_step1_beam_coverage(self, df: pl.DataFrame):
        """STEP 1: Draw beam coverage using Ant-Size as radius"""
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

                coverage_km = ant_size
                if coverage_km <= 0:
                    continue

                color = self.get_cell_color(cell_name)
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
                    <b>Ant-Size Radius:</b> {coverage_km:.3f} km
                </div>
                """

                folium.Polygon(
                    locations=polygon_coords,
                    color=color,
                    weight=2,
                    opacity=0.8,
                    fill=True,
                    fill_color=color,
                    fill_opacity=1.0,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"Beam: {coverage_km:.3f} km ({cell_name})",
                ).add_to(layer)

                self._add_cell_marker_with_label(
                    lat, lon, cell_name, msc_name, color, layer
                )

            except Exception:
                continue

        layer.add_to(self.map)

    def _add_step2_ta90_coverage(self, df: pl.DataFrame):
        """STEP 2: Draw TA90 coverage using TA90 as radius"""
        layer = folium.FeatureGroup(name="📊 TA90 Coverage", show=True)

        ta90_cells = df.filter((pl.col("TA90").is_not_null()) & (pl.col("TA90") > 0))

        if ta90_cells.is_empty():
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

                coverage_km = ta90_value
                if coverage_km <= 0:
                    continue

                color = self.get_cell_color(cell_name)
                polygon_coords = self._create_sector_polygon(
                    lat, lon, direction, beam, coverage_km
                )

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>📊 TA90 Coverage</b><br>
                    <b>Cell:</b> {cell_name}<br>
                    <b>MSC:</b> {msc_name}<br>
                    <b>Band:</b> L{band}<br>
                    <b>TA90 Radius:</b> {coverage_km:.3f} km
                </div>
                """

                folium.Polygon(
                    locations=polygon_coords,
                    color=color,
                    weight=1.5,
                    opacity=0.6,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.2,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"TA90: {coverage_km:.3f} km ({cell_name})",
                ).add_to(layer)

            except Exception:
                continue

        layer.add_to(self.map)

    def _add_step3_tier1_connections(self, df: pl.DataFrame):
        """STEP 3: Draw polyline with 1st Tier connections"""
        layer = folium.FeatureGroup(name="🔗 ISD", show=True)

        tier1_connections = df.filter(
            (pl.col("1st Tier").is_not_null())
            & (pl.col("1st Tier") != "")
            & (pl.col("1st Tier") != "1st Tier")
        )

        if tier1_connections.is_empty():
            return

        for source_row in tier1_connections.iter_rows(named=True):
            source_cell = source_row["CellName"]
            tier1_site = source_row["1st Tier"]

            target_cells = df.filter(pl.col("MSC") == tier1_site)

            if not target_cells.is_empty():
                target_row = target_cells.row(0, named=True)

                try:
                    lat1, lon1 = source_row["Latitude"], source_row["Longitude"]
                    lat2, lon2 = target_row["Latitude"], target_row["Longitude"]

                    offset = 0.00036
                    mid_lat = (lat1 + lat2) / 2 + offset
                    mid_lon = (lon1 + lon2) / 2 + offset

                    line_coords = [(lat1, lon1), (mid_lat, mid_lon), (lat2, lon2)]
                    distance_km = self._calculate_distance(lat1, lon1, lat2, lon2)

                    popup_html = f"""
                    <div style='font-family: Arial; font-size: 12px;'>
                        <b>🔗 Tier1 Connection</b><br>
                        <b>From:</b> {source_cell}<br>
                        <b>To MSC:</b> {tier1_site}<br>
                        <b>Distance:</b> {distance_km:.2f} km
                    </div>
                    """

                    folium.PolyLine(
                        locations=line_coords,
                        color="#FF0000",
                        weight=3,
                        opacity=0.8,
                        dash_array="10, 5, 2, 5",
                        popup=folium.Popup(popup_html, max_width=300),
                    ).add_to(layer)

                    self._add_distance_label(mid_lat, mid_lon, distance_km, layer)

                except Exception:
                    continue

        layer.add_to(self.map)

    def _add_cell_marker_with_label(
        self, lat: float, lon: float, cell_name: str, msc_name: str, color: str, layer
    ):
        """Add cell marker dengan MSC label"""
        popup_html = f"""
        <div style='font-family: Arial; font-size: 12px; min-width: 200px;'>
            <b>📍 Cell</b><br>
            <b>Name:</b> {cell_name}<br>
            <b>MSC:</b> {msc_name}<br>
            <b>Location:</b> {lat:.6f}, {lon:.6f}
        </div>
        """

        folium.CircleMarker(
            location=(lat, lon),
            radius=8,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=cell_name,
            color="white",
            weight=2,
            fill=True,
            fill_color=color,
            fill_opacity=0.9,
        ).add_to(layer)

        msc_label_html = f"""
        <div style='
            font-family: Arial; 
            font-size: 12px; 
            font-weight: bold;
            color: #666; 
            background-color: rgba(255,255,255,0.95);
            border: 1px solid #ccc;
            border-radius: 3px;
            padding: 1px 3px;
            white-space: nowrap;
        '>
            {msc_name}
        </div>
        """

        folium.Marker(
            location=(lat, lon),
            icon=folium.DivIcon(
                html=msc_label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=msc_name,
        ).add_to(layer)

    def _add_distance_label(self, lat: float, lon: float, distance_km: float, layer):
        """Add distance label pada polyline connections"""
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
        '>
            {distance_km:.1f} km
        </div>
        """

        folium.Marker(
            location=(lat, lon),
            icon=folium.DivIcon(
                html=label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=f"Distance: {distance_km:.1f} km",
        ).add_to(layer)

    def _create_sector_polygon(
        self, lat: float, lon: float, direction: float, beam: float, radius_km: float
    ) -> List[Tuple[float, float]]:
        """Create sector polygon coordinates for coverage area"""
        points = [(lat, lon)]

        start_angle = direction - beam / 2
        end_angle = direction + beam / 2

        for angle in range(int(start_angle), int(end_angle) + 1, 2):
            angle_rad = math.radians(angle)
            delta_lat = radius_km * math.cos(angle_rad) / 111.0
            delta_lon = (
                radius_km * math.sin(angle_rad) / (111.0 * math.cos(math.radians(lat)))
            )

            point_lat = lat + delta_lat
            point_lon = lon + delta_lon
            points.append((point_lat, point_lon))

        points.append((lat, lon))
        return points

    def _calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two points in km"""
        R = 6371
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

    def _add_cell_legend(self):
        """Add custom legend for cell names and colors"""
        if not self.cell_colors:
            return

        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; 
                    background-color: white; z-index:1000; 
                    border: 2px solid grey; padding: 10px; 
                    font-size: 12px; font-family: Arial;">
            <h4 style="margin: 0 0 8px 0;">Cell Legend</h4>
        """

        for cell_name, color in list(self.cell_colors.items())[
            :10
        ]:  # Limit to first 10
            legend_html += f"""
            <div style="display: flex; align-items: center; margin-bottom: 4px;">
                <div style="background-color: {color}; width: 16px; height: 16px; 
                         border: 1px solid grey; margin-right: 8px;"></div>
                <span style="overflow: hidden; text-overflow: ellipsis;">{cell_name}</span>
            </div>
            """

        if len(self.cell_colors) > 10:
            legend_html += (
                f'<div style="color: #666;">+ {len(self.cell_colors) - 10} more</div>'
            )

        legend_html += "</div>"

        legend = MacroElement()
        legend._template = Template(legend_html)
        self.map.get_root().add_child(legend)

    def display(self):
        """Display map in Streamlit"""
        self._add_cell_legend()
        folium.LayerControl(position="topright", collapsed=False).add_to(self.map)

        try:
            Fullscreen().add_to(self.map)
        except Exception:
            pass

        st.components.v1.html(self.map._repr_html_(), height=650, scrolling=False)


def render_coverage_map_3step(results: dict):
    """Render coverage map with 3-step approach from dashboard results"""
    df_coverage = results.get("gcell_coverage")

    if df_coverage is None or df_coverage.is_empty():
        st.warning("⚠️ No coverage data available. GCell Coverage merge required.")
        return

    with st.spinner("Generating 3-step coverage map..."):
        viz = CoverageMapVisualization()
        viz.initialize_map(df_coverage)
        viz.add_coverage_layers_3step(df_coverage)
        viz.display()
        # st.container(border=True)


def render_coverage_map(results: dict):
    """Main function - uses 3-step approach"""
    render_coverage_map_3step(results)
