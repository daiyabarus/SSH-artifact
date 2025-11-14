"""

Coverage Map Service untuk data GCELL + SCOT + TA - FULLY FIXED VERSION
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
                (pl.col("latitude").is_not_null())
                & (pl.col("longitude").is_not_null())
                & (pl.col("latitude") != 0)
                & (pl.col("longitude") != 0)
            )

            if valid_cells.is_empty():
                self.map_center = (5.2, 95.9)
            else:
                lat_mean = valid_cells["latitude"].mean()
                lon_mean = valid_cells["longitude"].mean()
                self.map_center = (lat_mean, lon_mean)

        except Exception as e:
            st.error(f"Error calculating map center: {e}")
            self.map_center = (5.2, 95.9)

        self.map = folium.Map(
            location=self.map_center,
            zoom_start=15,
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

    def add_coverage_layers_3step(self, df_coverage: pl.DataFrame):
        """Add coverage layers in 3 steps dengan data GCELL + SCOT + TA"""
        if df_coverage.is_empty():
            st.warning("⚠️ Coverage data is empty")
            return

        df_valid = df_coverage.filter(
            (pl.col("latitude").is_not_null())
            & (pl.col("longitude").is_not_null())
            & (pl.col("latitude") != 0)
            & (pl.col("longitude") != 0)
        )

        if df_valid.is_empty():
            st.warning("⚠️ No valid coordinates found")
            return

        self.assign_cell_colors(df_valid)

        self._add_step2_ta90_coverage(df_valid)
        self._add_step1_beam_coverage(df_valid)

    def _add_step1_beam_coverage(self, df: pl.DataFrame):
        """STEP 1: Draw beam coverage menggunakan ant_size sebagai radius"""
        polygon_layer = folium.FeatureGroup(name="📡 Beam Coverage", show=True)
        marker_layer = folium.FeatureGroup(name="📍 Cell Markers", show=True)

        cells_added = 0

        for row in df.iter_rows(named=True):
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                cell_name = str(row["CellName"])
                band = str(row["band"])
                direction = float(row.get("dir", 0))
                beam = float(row.get("beam", 65))
                ant_size = float(row.get("ant_size", 0.1))
                msc_name = str(row["MSC"])

                if lat == 0 or lon == 0 or abs(lat) > 90 or abs(lon) > 180:
                    continue

                coverage_km = ant_size
                if coverage_km <= 0 or coverage_km > 50:
                    coverage_km = 0.1

                color = self.get_cell_color(cell_name)

                polygon_coords = self._create_sector_polygon_fixed(
                    lat, lon, direction, beam, coverage_km
                )

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>📡 Beam GCELL</b><br>
                    <b>Cell:</b> {cell_name}<br>
                    <b>MSC:</b> {msc_name}<br>
                    <b>Band:</b> L{band}<br>
                    <b>Lat/Lon:</b> {lat:.6f}, {lon:.6f}<br>
                    <b>Direction:</b> {direction}°<br>
                    <b>Beam Width:</b> {beam}°<br>
                    <b>Ant-Size:</b> {coverage_km:.3f} km
                </div>
                """

                folium.Polygon(
                    locations=polygon_coords,
                    color="black",
                    weight=2,
                    opacity=0.8,
                    fill=True,
                    fill_color=color,
                    fill_opacity=1.0,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"{cell_name} ({coverage_km:.3f}km)",
                ).add_to(polygon_layer)

                self._add_cell_marker_with_label(
                    lat, lon, cell_name, msc_name, color, marker_layer
                )

                cells_added += 1

            except Exception as e:
                st.warning(f"⚠️ Error adding cell {row.get('CellName', 'Unknown')}: {e}")
                continue

        polygon_layer.add_to(self.map)
        marker_layer.add_to(self.map)

    def _add_step2_ta90_coverage(self, df: pl.DataFrame):
        """STEP 2: Draw TA90 coverage menggunakan newta_ta90 sebagai radius"""
        layer = folium.FeatureGroup(name="📊 TA90 Coverage", show=True)

        ta90_cells = df.filter(
            (pl.col("TA90").is_not_null())
            & (pl.col("TA90") > 0)
            & (pl.col("TA90") < 50)
        )

        if ta90_cells.is_empty():
            st.info("ℹ️ No TA90 data available")
            return

        ta90_added = 0

        for row in ta90_cells.iter_rows(named=True):
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                cell_name = str(row["CellName"])
                band = str(row["band"])
                direction = float(row.get("dir", 0))
                beam = float(row.get("beam", 65))
                ta90_value = float(row.get("TA90", 0))
                msc_name = str(row["MSC"])

                if lat == 0 or lon == 0 or ta90_value <= 0:
                    continue

                color = self.get_cell_color(cell_name)

                polygon_coords = self._create_sector_polygon_fixed(
                    lat, lon, direction, beam, ta90_value
                )

                popup_html = f"""
                <div style='font-family: Arial; font-size: 12px;'>
                    <b>📊 TA90 Coverage</b><br>
                    <b>Cell:</b> {cell_name}<br>
                    <b>MSC:</b> {msc_name}<br>
                    <b>Band:</b> L{band}<br>
                    <b>TA90 Radius:</b> {ta90_value:.3f} km<br>
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
                    tooltip=f"TA90: {ta90_value:.3f} km",
                ).add_to(layer)

                ta90_added += 1

            except Exception as e:
                continue

        layer.add_to(self.map)

    def _add_cell_marker_with_label(
        self,
        lat: float,
        lon: float,
        cell_name: str,
        msc_name: str,
        color: str,
        layer,
        is_main_tower: bool = True,
    ):
        """Add cell marker dengan MSC label - NO OFFSET"""
        tower_type = "🎯 Main Tower" if is_main_tower else "🔗 1st Tier Tower"

        popup_html = f"""
        <div style='font-family: Arial; font-size: 12px; min-width: 200px;'>
            <b>{tower_type}</b><br>
            <b>Cell:</b> {cell_name}<br>
            <b>Tower ID:</b> {msc_name}<br>
            <b>Coordinates:</b><br>
            Lat: {lat:.6f}<br>
            Lon: {lon:.6f}
        </div>
        """

        marker_radius = 8 if is_main_tower else 6
        folium.CircleMarker(
            location=(lat, lon),
            radius=marker_radius,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{msc_name} - {cell_name}",
            color=color,
            weight=2,
            fill=True,
            fill_color=color,
            fill_opacity=1.0,
        ).add_to(layer)

        label_color = "red" if is_main_tower else "#0066FF"
        label_size = "16pt" if is_main_tower else "18pt"

        label_html = f"""
        <div style='
            font-family: Arial; 
            font-size: {label_size}; 
            font-weight: bold;
            color: {label_color};
            text-shadow: 2px 2px 4px rgba(0,0,0,0.7);
            white-space: nowrap;
        '>
            {msc_name}
        </div>
        """

        folium.Marker(
            location=(lat, lon),
            icon=folium.DivIcon(
                html=label_html,
                icon_size=(None, None),
                icon_anchor=(0, 0),
            ),
            tooltip=msc_name,
        ).add_to(layer)

    def _create_sector_polygon_fixed(
        self, lat: float, lon: float, azimuth: float, beamwidth: float, radius_km: float
    ) -> List[Tuple[float, float]]:
        """
        ✅ FIXED: Proper spherical trigonometry for sector polygon
        Based on working geo_app.py sample
        """

        if radius_km <= 0 or radius_km > 100:
            radius_km = 0.1

        if beamwidth <= 0 or beamwidth > 360:
            beamwidth = 65

        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        azimuth_rad = math.radians(azimuth)
        beamwidth_rad = math.radians(beamwidth)

        R = 6371.0

        num_points = 50
        angle_step = beamwidth_rad / (num_points - 1)
        start_angle = azimuth_rad - beamwidth_rad / 2

        points = []

        for i in range(num_points):
            angle = start_angle + i * angle_step

            lat_new = math.asin(
                math.sin(lat_rad) * math.cos(radius_km / R)
                + math.cos(lat_rad) * math.sin(radius_km / R) * math.cos(angle)
            )

            lon_new = lon_rad + math.atan2(
                math.sin(angle) * math.sin(radius_km / R) * math.cos(lat_rad),
                math.cos(radius_km / R) - math.sin(lat_rad) * math.sin(lat_new),
            )

            points.append([math.degrees(lat_new), math.degrees(lon_new)])

        points.append([lat, lon])

        return points

    def _add_cell_legend(self):
        """Add custom legend untuk cell names dan colors"""
        if not self.cell_colors:
            return

        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 220px; 
                    background-color: white; z-index:1000; 
                    border: 2px solid grey; padding: 10px; 
                    border-radius: 5px;
                    font-size: 12px; font-family: Arial;">
            <h4 style="margin: 0 0 8px 0;">📡 Cell Legend</h4>
        """

        for cell_name, color in list(self.cell_colors.items())[:10]:
            legend_html += f"""
            <div style="display: flex; align-items: center; margin-bottom: 4px;">
                <div style="background-color: {color}; width: 16px; height: 16px; 
                         border: 1px solid grey; margin-right: 8px;"></div>
                <span style="overflow: hidden; text-overflow: ellipsis; font-size: 10px;">{cell_name}</span>
            </div>
            """

        if len(self.cell_colors) > 10:
            legend_html += f'<div style="color: #666; margin-top: 8px;">+ {len(self.cell_colors) - 10} more cells</div>'

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
    ✅ FIXED: Prepare data dengan column mapping yang BENAR
    Sesuai dengan struktur data actual dari database
    """
    if df_gcell_scot_ta.is_empty():
        return pl.DataFrame()

    try:
        required_cols = {
            "moentity": "CellName",
            "new_tower_id": "MSC",
            "band": "band",
            "longitude": "longitude",
            "latitude": "latitude",
            "dir": "dir",
            "ant_type": "ant_type",
            "ant_size": "ant_size",
            "beam": "beam",
        }

        select_exprs = []
        for orig_col, new_col in required_cols.items():
            if orig_col in df_gcell_scot_ta.columns:
                select_exprs.append(pl.col(orig_col).alias(new_col))
            else:
                st.warning(f"⚠️ Missing column: {orig_col}")

        if "newta_ta90" in df_gcell_scot_ta.columns:
            select_exprs.append(pl.col("newta_ta90").alias("TA90"))

        if "newscot_isd" in df_gcell_scot_ta.columns:
            select_exprs.append(pl.col("newscot_isd"))

        if "newscot_target_site" in df_gcell_scot_ta.columns:
            select_exprs.append(pl.col("newscot_target_site"))

        if "newta_sector" in df_gcell_scot_ta.columns:
            select_exprs.append(pl.col("newta_sector"))

        if "newta_sector_name" in df_gcell_scot_ta.columns:
            select_exprs.append(pl.col("newta_sector_name"))

        df_coverage = df_gcell_scot_ta.select(select_exprs)

        return df_coverage

    except Exception as e:
        st.error(f"❌ Error preparing coverage data: {e}")
        return pl.DataFrame()


def render_coverage_map_3step(df_gcell_scot_ta: pl.DataFrame):
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
        viz.add_coverage_layers_3step(df_coverage)
        viz.display()


def render_coverage_map(df_gcell_scot_ta: pl.DataFrame):
    """Main function - uses 3-step approach dengan GCELL+SCOT+TA data"""
    render_coverage_map_3step(df_gcell_scot_ta)
