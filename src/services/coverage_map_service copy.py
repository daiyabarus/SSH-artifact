"""

Coverage Map Service untuk data GCELL + SCOT + TA
"""

import folium
import streamlit as st
from branca.element import MacroElement, Template
from folium.plugins import Fullscreen
import polars as pl
import math
from typing import List, Tuple


class CoverageMapVisualization:
    """Clean visualization untuk cell coverage dengan data GCELL + SCOT + TA"""

    def __init__(self):
        self.map = None
        self.map_center = None
        self.cell_colors = {}

    def initialize_map(self, df_coverage: pl.DataFrame):
        """Initialize Folium map centered on cells"""
        try:
            valid_cells = df_coverage.filter(
                (pl.col("latitude").is_not_null()) & (pl.col("longitude").is_not_null())
            )

            if valid_cells.is_empty():
                self.map_center = (0, 0)
            else:
                lat_mean = valid_cells["latitude"].mean()
                lon_mean = valid_cells["longitude"].mean()
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
        """Assign different colors for each unique CellName (moentity)"""
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

    def add_coverage_layers_3step(
        self, df_coverage: pl.DataFrame, selected_tower_ids: List[str] = None
    ):
        """Add coverage layers in 3 steps dengan data GCELL + SCOT + TA"""
        if df_coverage.is_empty():
            return

        df_valid = df_coverage.filter(
            (pl.col("latitude").is_not_null()) & (pl.col("longitude").is_not_null())
        )

        self.assign_cell_colors(df_valid)

        self._add_step2_ta90_coverage(df_valid)
        self._add_step1_beam_coverage(df_valid)
        self._add_step3_isd_connections(df_valid, selected_tower_ids)

    def _add_step1_beam_coverage(self, df: pl.DataFrame):
        """STEP 1: Draw beam coverage menggunakan ant_size sebagai radius"""
        layer = folium.FeatureGroup(name="📡 Beam Coverage", show=True)

        for row in df.iter_rows(named=True):
            try:
                lat = row["latitude"]
                lon = row["longitude"]
                cell_name = row["CellName"]
                band = str(row["band"])
                direction = row.get("dir", 0)
                beam = row.get("beam", 65)
                ant_size = row.get("ant_size", 0.1)
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
        """STEP 2: Draw TA90 coverage menggunakan newta_ta90 sebagai radius"""
        layer = folium.FeatureGroup(name="📊 TA90 Coverage", show=True)

        ta90_cells = df.filter((pl.col("TA90").is_not_null()) & (pl.col("TA90") > 0))

        if ta90_cells.is_empty():
            return

        for row in ta90_cells.iter_rows(named=True):
            try:
                lat = row["latitude"]
                lon = row["longitude"]
                cell_name = row["CellName"]
                band = str(row["band"])
                direction = row.get("dir", 0)
                beam = row.get("beam", 65)
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
                    <b>TA90 Radius:</b> {coverage_km:.3f} km<br>
                    <b>Sector:</b> {row.get("newta_sector_name", "N/A")}
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

    def _add_step3_isd_connections(
        self, df: pl.DataFrame, selected_tower_ids: List[str] = None
    ):
        """STEP 3: Draw polyline dengan ISD connections - HANYA dari MSC source yang dipilih"""
        layer = folium.FeatureGroup(name="🔗 ISD Connections", show=True)

        if selected_tower_ids is None:
            source_towers = df.select(pl.col("MSC").unique()).to_series().to_list()
        else:
            source_towers = selected_tower_ids

        isd_connections = df.filter(
            (pl.col("MSC").is_in(source_towers))
            & (pl.col("newscot_target_site").is_not_null())
            & (pl.col("newscot_target_site") != "")
            & (pl.col("newscot_isd").is_not_null())
            & (pl.col("newscot_isd") > 0)
        )

        if isd_connections.is_empty():
            return

        drawn_connections = set()

        for row in isd_connections.iter_rows(named=True):
            try:
                source_tower = row["MSC"]
                target_tower = row["newscot_target_site"]

                connection_key = f"{source_tower}->{target_tower}"
                if connection_key in drawn_connections:
                    continue

                lat1 = row["latitude"]
                lon1 = row["longitude"]

                target_cells = df.filter(pl.col("MSC") == target_tower)

                if target_cells.is_empty():
                    continue

                target_row = target_cells.row(0, named=True)
                lat2 = target_row["latitude"]
                lon2 = target_row["longitude"]

                if not all([lat1, lon1, lat2, lon2]):
                    continue

                offset = 0.00036
                mid_lat = (lat1 + lat2) / 2 + offset
                mid_lon = (lon1 + lon2) / 2 + offset

                line_coords = [(lat1, lon1), (mid_lat, mid_lon), (lat2, lon2)]
                isd_distance = row["newscot_isd"]

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>🔗 ISD Connection</b><br>
                    <b>From:</b> {source_tower} <span style='color: blue;'>(Source)</span><br>
                    <b>To:</b> {target_tower}<br>
                    <b>ISD Distance:</b> {isd_distance:.2f} km<br>
                    <b>Calculated:</b> {self._calculate_distance(lat1, lon1, lat2, lon2):.2f} km
                </div>
                """

                folium.PolyLine(
                    locations=line_coords,
                    color="#FF0000",
                    weight=3,
                    opacity=0.8,
                    dash_array="10, 5, 2, 5",
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"ISD: {source_tower} → {target_tower}",
                ).add_to(layer)

                self._add_distance_label(mid_lat, mid_lon, isd_distance, layer)

                drawn_connections.add(connection_key)

            except Exception as e:
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
            tooltip=f"ISD Distance: {distance_km:.1f} km",
        ).add_to(layer)

    def _create_sector_polygon(
        self, lat: float, lon: float, direction: float, beam: float, radius_km: float
    ) -> List[Tuple[float, float]]:
        """Create sector polygon coordinates untuk coverage area"""
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
        """Add custom legend untuk cell names dan colors"""
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

        for cell_name, color in list(self.cell_colors.items())[:10]:
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

        st.components.v1.html(self.map._repr_html_(), height=850, scrolling=False)


def prepare_coverage_data(df_gcell_scot_ta: pl.DataFrame) -> pl.DataFrame:
    """
    Prepare data untuk coverage map dari GCELL + SCOT + TA data

    Args:
        df_gcell_scot_ta: DataFrame dari joined GCELL + SCOT + TA

    Returns:
        DataFrame dengan kolom yang sesuai untuk coverage map
    """
    if df_gcell_scot_ta.is_empty():
        return pl.DataFrame()

    df_coverage = df_gcell_scot_ta.select(
        [
            pl.col("moentity").alias("CellName"),
            pl.col("new_tower_id").alias("MSC"),
            pl.col("band"),
            pl.col("longitude"),
            pl.col("latitude"),
            pl.col("dir"),
            pl.col("ant_type"),
            pl.col("ant_size"),
            pl.col("beam"),
            pl.col("newta_ta90").alias("TA90"),
            pl.col("newscot_isd"),
            pl.col("newscot_target_site"),
            pl.col("newta_sector"),
            pl.col("newta_sector_name"),
        ]
    )

    return df_coverage


def render_coverage_map_3step(
    df_gcell_scot_ta: pl.DataFrame, tower_ids: List[str] = None
):
    """Render coverage map dengan 3-step approach dari GCELL + SCOT + TA data"""
    if df_gcell_scot_ta.is_empty():
        st.warning("⚠️ No GCELL+SCOT+TA data available for coverage map.")
        return

    with st.spinner("Preparing coverage map data..."):
        df_coverage = prepare_coverage_data(df_gcell_scot_ta)

        if df_coverage.is_empty():
            st.warning("⚠️ No valid coverage data available.")
            return

    with st.spinner("Generating 3-step coverage map..."):
        viz = CoverageMapVisualization()
        viz.initialize_map(df_coverage)
        viz.add_coverage_layers_3step(df_coverage, tower_ids)
        viz.display()


def render_coverage_map(df_gcell_scot_ta: pl.DataFrame, tower_ids: List[str] = None):
    """Main function - uses 3-step approach dengan GCELL+SCOT+TA data"""
    render_coverage_map_3step(df_gcell_scot_ta, tower_ids)
