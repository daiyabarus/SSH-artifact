import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
import openpyxl
from openpyxl.styles import Font
from copy import copy
from openpyxl.drawing.image import Image as XLImage
from io import BytesIO
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import subprocess
import time
import warnings

# Suppress openpyxl conditional formatting warning
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class SSHReportGenerator:
    def __init__(self, input_folder, template_path, output_folder, db_path):
        # Resolve all paths to absolute to avoid relative path issues in PowerShell/COM
        self.input_folder = Path(input_folder).resolve()
        self.template_path = Path(template_path).resolve()
        self.output_folder = Path(output_folder).resolve()
        self.db_path = Path(db_path).resolve()
        self.output_folder.mkdir(exist_ok=True, parents=True)
        
        # Color palettes
        self.color_palette = [
            "#080cec", "#ef17e8", "#52eb0c", "#f59e0b", "#10b981",
            "#06b6d4", "#ef4444", "#a855f7", "#14b8a6", "#f97316"
        ]
        
        self.band_colors = {
            "850": "#52eb0c",
            "1800": "#080cec",
            "2100": "#ef17e8",
            "2300": "#F39C12",
            "2600": "#9B59B6",
        }
    
    def connect_db(self):
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        return sqlite3.connect(self.db_path)
    
    def parse_date_flexible(self, date_str):
        if pd.isna(date_str):
            return None
        
        date_formats = [
            "%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", 
            "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        print(f"  ⚠ Could not parse date: {date_str}")
        return None

    # === Database Queries (unchanged) ===
    def query_ta_data(self, conn, tower_id):
        query = """
        SELECT * FROM tbl_newta 
        WHERE newta_managed_element = ?
        ORDER BY newta_date DESC
        """
        df = pd.read_sql_query(query, conn, params=(tower_id,))
        return df
    
    def query_wd_data(self, conn, tower_id):
        query = """
        SELECT newwd_date, newwd_moentity, newwd_cell_fdd_band, 
               newwd_spectral_efficiency_dl_num, newwd_spectral_efficiency_dl_den,
               newwd_enodeb_fdd_msc
        FROM tbl_newwd
        WHERE newwd_enodeb_fdd_msc = ?
        ORDER BY newwd_date
        """
        df = pd.read_sql_query(query, conn, params=(tower_id,))
        df['newwd_date'] = df['newwd_date'].apply(self.parse_date_flexible)
        return df
    
    def query_bh_data(self, conn, tower_id):
        query = """
        SELECT newbh_date, newbh_moentity, 
               newbh_cell_average_cqi_num, newbh_cell_average_cqi_den,
               newbh_cell_qpsk_rate_num, newbh_cell_qpsk_rate_den,
               newbh_cell_fdd_band, newbh_enodeb_fdd_msc
        FROM tbl_newbh
        WHERE newbh_enodeb_fdd_msc = ?
        ORDER BY newbh_date
        """
        df = pd.read_sql_query(query, conn, params=(tower_id,))
        df['newbh_date'] = df['newbh_date'].apply(self.parse_date_flexible)
        return df
    
    def merge_data(self, ta_df, wd_df, bh_df):
        if ta_df.empty:
            return None, None, None
        
        wd_merged = None
        if not wd_df.empty:
            wd_merged = pd.merge(
                wd_df, 
                ta_df[['newta_eutrancell', 'newta_sector', 'newta_sector_name']],
                left_on='newwd_moentity',
                right_on='newta_eutrancell',
                how='left'
            )
        
        bh_merged = None
        if not bh_df.empty:
            bh_merged = pd.merge(
                bh_df,
                ta_df[['newta_eutrancell', 'newta_sector', 'newta_sector_name']],
                left_on='newbh_moentity',
                right_on='newta_eutrancell',
                how='left'
            )
        
        return ta_df, wd_merged, bh_merged

    # === Chart Functions ===
    def create_timing_advance_chart(self, ta_df):
        """Create unstacked bar chart for TA with line labels"""
        if ta_df.empty:
            return None
        
        # Group by sector_name
        sectors = ta_df['newta_sector_name'].unique()
        if len(sectors) == 0:
            return None
        
        # Create figure with 3 columns (one for each sector)
        fig, axes = plt.subplots(1, min(3, len(sectors)), figsize=(15, 6))
        if len(sectors) == 1:
            axes = [axes]
        elif len(sectors) == 2:
            axes = [axes[0], axes[1]]
        
        distance_labels = [
            "0-78", "78-234", "234-390", "390-546", "546-702", "702-858",
            "858-1014", "1014-1560", "1560-2106", "2106-2652", "2652-3120",
            "3120-3900", "3900-6318", "6318-10062", "10062-13962", "13962-20000"
        ]
        
        for idx, sector in enumerate(sectors):
            if idx >= 3:
                break
                
            ax = axes[idx] if len(sectors) > 1 else axes
            sector_data = ta_df[ta_df['newta_sector_name'] == sector]
            
            bands = sector_data['newta_band'].unique()
            bands = [b for b in bands if pd.notna(b)]
            bands.sort()
            
            x_pos = np.arange(len(distance_labels))
            bar_width = 0.8 / len(bands) if len(bands) > 0 else 0.8
            
            for band_idx, band in enumerate(bands):
                band_data = sector_data[sector_data['newta_band'] == band]
                if band_data.empty:
                    continue
                    
                latest_data = band_data.iloc[0]
                band_str = str(int(band)) if pd.notna(band) else "N/A"
                
                distance_cols = [
                    'newta_0_78_m', 'newta_78_234_m', 'newta_234_390_m', 'newta_390_546_m',
                    'newta_546_702_m', 'newta_702_858_m', 'newta_858_1014_m', 'newta_1014_1560_m',
                    'newta_1560_2106_m', 'newta_2106_2652_m', 'newta_2652_3120_m', 'newta_3120_3900_m',
                    'newta_3900_6318_m', 'newta_6318_10062_m', 'newta_10062_13962_m', 'newta_13962_20000_m'
                ]
                distance_values = [latest_data[col] if pd.notna(latest_data[col]) else 0 for col in distance_cols]
                
                bar_positions = x_pos + band_idx * bar_width - (len(bands) - 1) * bar_width / 2
                
                color = self.band_colors.get(band_str, "#95A5A6")
                ax.bar(bar_positions, distance_values, width=bar_width, 
                       alpha=0.7, color=color, label=f'L{band_str}')
            
            ax2 = ax.twinx()
            
            for band_idx, band in enumerate(bands):
                band_data = sector_data[sector_data['newta_band'] == band]
                if band_data.empty:
                    continue
                    
                latest_data = band_data.iloc[0]
                band_str = str(int(band)) if pd.notna(band) else "N/A"
                
                cdf_cols = [
                    'newta_78', 'newta_234', 'newta_390', 'newta_546', 'newta_702', 'newta_858',
                    'newta_1014', 'newta_1560', 'newta_2106', 'newta_2652', 'newta_3120', 'newta_3900',
                    'newta_6318', 'newta_10062', 'newta_13962', 'newta_20000'
                ]
                cdf_values = [latest_data[col] if pd.notna(latest_data[col]) else 0 for col in cdf_cols]
                
                color = self.band_colors.get(band_str, "#95A5A6")
                ax2.plot(x_pos, cdf_values, marker='o', color=color, 
                        linewidth=2, label=f'L{band_str} CDF')
                
                for i, (x, y) in enumerate(zip(x_pos, cdf_values)):
                    if i % 2 == 0:
                        ax2.annotate(f'{y:.1f}%', xy=(x, y), xytext=(0, 5),
                                    textcoords='offset points', ha='center',
                                    fontsize=7, color=color)
            
            ax2.axhline(y=90, color='red', linestyle='--', linewidth=1.5, 
                       label='TA90% Reference', alpha=0.7)
            
            ax2.annotate('TA90%', xy=(len(distance_labels)-1, 90), 
                        xytext=(5, 0), textcoords='offset points',
                        color='red', fontsize=8, fontweight='bold')
            
            ax2.set_ylabel('CDF (%)', fontsize=9)
            ax2.set_ylim(0, 105)
            ax2.grid(True, alpha=0.2, linestyle='--')
            
            ax.set_xlabel('Distance (m)', fontsize=9)
            ax.set_ylabel('Number of Samples', fontsize=9)
            ax.set_title(f'Sector {sector}', fontsize=11, fontweight='bold')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(distance_labels, rotation=45, ha='right', fontsize=7)
            ax.legend(loc='upper left', fontsize=7, ncol=2)
            ax.grid(True, alpha=0.2, axis='y')
            
            ax2.legend(loc='upper right', fontsize=7)
        
        plt.tight_layout()
        
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    
    def create_spectral_efficiency_chart(self, wd_merged):
        if wd_merged is None or wd_merged.empty:
            return None
        
        wd_merged['spectral_eff'] = np.where(
            wd_merged['newwd_spectral_efficiency_dl_den'] > 0,
            wd_merged['newwd_spectral_efficiency_dl_num'] / wd_merged['newwd_spectral_efficiency_dl_den'],
            np.nan
        )
        
        sectors = wd_merged['newta_sector_name'].dropna().unique()
        if len(sectors) == 0:
            return None
        
        num_sectors = min(3, len(sectors))
        fig, axes = plt.subplots(1, num_sectors, figsize=(15, 5))
        if num_sectors == 1:
            axes = [axes]
        
        for idx, sector in enumerate(sectors[:3]):
            ax = axes[idx]
            sector_data = wd_merged[wd_merged['newta_sector_name'] == sector]
            bands = sector_data['newwd_cell_fdd_band'].unique()
            
            for band_idx, band in enumerate(bands):
                band_data = sector_data[sector_data['newwd_cell_fdd_band'] == band].sort_values('newwd_date')
                if band_data.empty:
                    continue
                    
                band_str = str(int(band)) if pd.notna(band) else "Unknown"
                color = self.color_palette[band_idx % len(self.color_palette)]
                
                ax.plot(band_data['newwd_date'], band_data['spectral_eff'], 
                        marker='o', label=f'L{band_str}', color=color, linewidth=2, markersize=4)
            
            ax.set_title(f'Sector {sector}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Date', fontsize=9)
            ax.set_ylabel('Spectral Efficiency', fontsize=9)
            ax.legend(fontsize=7, loc='best')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax.set_ylim(bottom=0)
        
        plt.tight_layout()
        
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    
    def create_cqi_chart(self, bh_merged):
        if bh_merged is None or bh_merged.empty:
            return None
        
        bh_merged['cqi'] = np.where(
            bh_merged['newbh_cell_average_cqi_den'] > 0,
            bh_merged['newbh_cell_average_cqi_num'] / bh_merged['newbh_cell_average_cqi_den'],
            np.nan
        )
        
        sectors = bh_merged['newta_sector_name'].dropna().unique()
        if len(sectors) == 0:
            return None
        
        num_sectors = min(3, len(sectors))
        fig, axes = plt.subplots(1, num_sectors, figsize=(15, 5))
        if num_sectors == 1:
            axes = [axes]
        
        for idx, sector in enumerate(sectors[:3]):
            ax = axes[idx]
            sector_data = bh_merged[bh_merged['newta_sector_name'] == sector]
            bands = sector_data['newbh_cell_fdd_band'].unique()
            
            for band_idx, band in enumerate(bands):
                band_data = sector_data[sector_data['newbh_cell_fdd_band'] == band].sort_values('newbh_date')
                if band_data.empty:
                    continue
                    
                band_str = str(int(band)) if pd.notna(band) else "Unknown"
                color = self.color_palette[band_idx % len(self.color_palette)]
                
                ax.plot(band_data['newbh_date'], band_data['cqi'], 
                        marker='o', label=f'L{band_str}', color=color, linewidth=2, markersize=4)
            
            ax.set_title(f'Sector {sector}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Date', fontsize=9)
            ax.set_ylabel('Average CQI', fontsize=9)
            ax.legend(fontsize=7, loc='best')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax.set_ylim(0, 15)
        
        plt.tight_layout()
        
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    
    def create_qpsk_chart(self, bh_merged):
        if bh_merged is None or bh_merged.empty:
            return None
        
        bh_merged['qpsk'] = np.where(
            bh_merged['newbh_cell_qpsk_rate_den'] > 0,
            (bh_merged['newbh_cell_qpsk_rate_num'] / bh_merged['newbh_cell_qpsk_rate_den']) * 100,
            np.nan
        )
        
        sectors = bh_merged['newta_sector_name'].dropna().unique()
        if len(sectors) == 0:
            return None
        
        num_sectors = min(3, len(sectors))
        fig, axes = plt.subplots(1, num_sectors, figsize=(15, 5))
        if num_sectors == 1:
            axes = [axes]
        
        for idx, sector in enumerate(sectors[:3]):
            ax = axes[idx]
            sector_data = bh_merged[bh_merged['newta_sector_name'] == sector]
            bands = sector_data['newbh_cell_fdd_band'].unique()
            
            for band_idx, band in enumerate(bands):
                band_data = sector_data[sector_data['newbh_cell_fdd_band'] == band].sort_values('newbh_date')
                if band_data.empty:
                    continue
                    
                band_str = str(int(band)) if pd.notna(band) else "Unknown"
                color = self.color_palette[band_idx % len(self.color_palette)]
                
                ax.plot(band_data['newbh_date'], band_data['qpsk'], 
                        marker='o', label=f'L{band_str}', color=color, linewidth=2, markersize=4)
            
            ax.set_title(f'Sector {sector}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Date', fontsize=9)
            ax.set_ylabel('QPSK Rate (%)', fontsize=9)
            ax.legend(fontsize=7, loc='best')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax.set_ylim(0, 100)
        
        plt.tight_layout()
        
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    
    # ====================== EXCEL COPY VIA POWERSHELL ======================
    def copy_range_with_excel(self, source_file, template_file, output_file):
        """Copy range B6:V34 as picture using Excel COM via PowerShell"""
        ps_file = None
        try:
            # Convert paths to use forward slashes for PowerShell compatibility
            source_path = str(Path(source_file).resolve()).replace('\\', '/')
            template_path = str(Path(template_file).resolve()).replace('\\', '/')
            output_path = str(Path(output_file).resolve()).replace('\\', '/')
            
            ps_script = f'''
$ErrorActionPreference = "Stop"

try {{
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false

    Write-Host "Opening source file: {source_path}"
    $sourceWb = $excel.Workbooks.Open("{source_path}")
    $sourceWs = $sourceWb.Worksheets.Item("SSH Achievement")

    Write-Host "Copying range B6:V34 as picture..."
    $range = $sourceWs.Range("B6:V34")
    $range.CopyPicture(1, 2)

    Write-Host "Opening template: {template_path}"
    $templateWb = $excel.Workbooks.Open("{template_path}")
    $templateWs = $templateWb.Worksheets.Item("SSH Achievement")

    Write-Host "Pasting at C8..."
    $templateWs.Range("C8").Select()
    $templateWs.Paste()

    Write-Host "Saving as: {output_path}"
    $templateWb.SaveAs("{output_path}", 51)

    Write-Host "Closing..."
    $templateWb.Close($false)
    $sourceWb.Close($false)
    $excel.Quit()

    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($templateWs) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($templateWb) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($sourceWs) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($sourceWb) | Out-Null
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()

    Write-Host "SUCCESS"
    exit 0
}} catch {{
    Write-Host "ERROR: $_"
    exit 1
}}
'''

            ps_file = self.output_folder / "temp_copy.ps1"
            with open(ps_file, 'w', encoding='utf-8') as f:
                f.write(ps_script)

            print("  📝 Running PowerShell script...")
            result = subprocess.run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(ps_file)],
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=90
            )

            print(f"  PowerShell stdout: {result.stdout.strip()}")
            if result.stderr:
                print(f"  PowerShell stderr: {result.stderr.strip()}")
            print(f"  Return code: {result.returncode}")

            output_path_obj = Path(output_file)
            if result.returncode == 0 and output_path_obj.exists():
                print(f"  ✓ Output file created successfully: {output_path_obj.name}")
                return True
            else:
                print(f"  ✗ PowerShell failed (code {result.returncode}) or file not created")
                return False

        except subprocess.TimeoutExpired:
            print("  ✗ PowerShell script timed out")
            return False
        except Exception as e:
            print(f"  ✗ PowerShell execution error: {e}")
            return False
        finally:
            # Safe cleanup: try to delete ps file, but don't fail the whole process if locked
            if ps_file and ps_file.exists():
                try:
                    ps_file.unlink()
                except PermissionError:
                    print(f"  ⚠ Could not delete temp script (locked by OS): {ps_file.name}")
                except Exception as e:
                    print(f"  ⚠ Failed to delete temp script: {e}")

    # ====================== PROCESSING ======================
    def get_conditional_values(self, ws):
        k_values = ["99.00", "98.00", "1.00", "97.00", "8.00", "50.00", "1.10",
                    "1.00", "120.00", "5.00", "1.00", "20.00", "-105.00", "30.00"]
        n_values = ["99.00", "98.00", "1.00", "97.00", "9.00", "40.00", "1.50",
                    "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00",
                    "98.00", "98.00", "5.00"]
        q_values = ["99.00", "98.00", "1.00", "97.00", "9.00", "40.00", "1.70",
                    "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00"]
        t_values = ["99.00", "98.00", "1.00", "97.00", "10.00", "40.00", "1.90",
                    "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00"]
        
        # M28 -> K11:K24
        try:
            if ws['M28'].value and float(ws['M28'].value) > 0:
                for idx, i in enumerate(range(11, 25)):
                    ws[f'K{i}'].value = k_values[idx]
                    ws[f'K{i}'].number_format = '@'
            else:
                for i in range(11, 25):
                    ws[f'K{i}'].value = "-"
                    ws[f'K{i}'].number_format = '@'
        except Exception as e:
            print(f"  ⚠ Warning M28: {e}")
        
        # P28 -> N11:N27
        try:
            if ws['P28'].value and float(ws['P28'].value) > 0:
                for idx, i in enumerate(range(11, 28)):
                    ws[f'N{i}'].value = n_values[idx]
                    ws[f'N{i}'].number_format = '@'
            else:
                for i in range(11, 28):
                    ws[f'N{i}'].value = "-"
                    ws[f'N{i}'].number_format = '@'
        except Exception as e:
            print(f"  ⚠ Warning P28: {e}")
        
        # S28 -> Q11:Q24
        try:
            if ws['S28'].value and float(ws['S28'].value) > 0:
                for idx, i in enumerate(range(11, 25)):
                    ws[f'Q{i}'].value = q_values[idx]
                    ws[f'Q{i}'].number_format = '@'
            else:
                for i in range(11, 25):
                    ws[f'Q{i}'].value = "-"
                    ws[f'Q{i}'].number_format = '@'
        except Exception as e:
            print(f"  ⚠ Warning S28: {e}")
        
        # V28 -> T11:T24
        try:
            if ws['V28'].value and float(ws['V28'].value) > 0:
                for idx, i in enumerate(range(11, 25)):
                    ws[f'T{i}'].value = t_values[idx]
                    ws[f'T{i}'].number_format = '@'
            else:
                for i in range(11, 25):
                    ws[f'T{i}'].value = "-"
                    ws[f'T{i}'].number_format = '@'
        except Exception as e:
            print(f"  ⚠ Warning V28: {e}")

    def list_xlsx_files(self):
        xlsx_files = sorted(self.input_folder.glob("*.xlsx"))
        print(f"Found {len(xlsx_files)} Excel files:")
        for i, file in enumerate(xlsx_files, 1):
            print(f"  {i}. {file.name}")
        return xlsx_files
    
    def extract_cluster_tower(self, text):
        cluster_match = re.search(r'Cluster\s*:\s*([^T]+?)Tower', text)
        tower_match = re.search(r'Tower\s*:\s*([^\s(]+)', text)
        
        cluster = cluster_match.group(1).strip() if cluster_match else "Unknown"
        tower = tower_match.group(1).strip() if tower_match else "Unknown"
        
        return cluster, tower

    def process_file(self, file_path):
        print(f"\nProcessing: {file_path.name}")
        
        try:
            wb = openpyxl.load_workbook(file_path)
            
            if "Cluster & Tower" in wb.sheetnames:
                ws = wb["Cluster & Tower"]
                ws.title = "SSH Achievement"
                print(f"  ✓ Renamed 'Cluster & Tower' to 'SSH Achievement'")
            else:
                print(f"  ✗ Sheet 'Cluster & Tower' not found")
                return None
            
            sheets_to_remove = [s for s in wb.sheetnames if s != "SSH Achievement"]
            for sheet in sheets_to_remove:
                wb.remove(wb[sheet])
            print(f"  ✓ Removed {len(sheets_to_remove)} other sheets")
            
            b6_value = ws['B6'].value or ""
            cluster, tower = self.extract_cluster_tower(str(b6_value))
            print(f"  ✓ Cluster: {cluster} | Tower: {tower}")
            
            self.get_conditional_values(ws)
            
            # Save modified source as temp file
            timestamp = datetime.now().strftime("%H%M%S")
            modified_source = self.output_folder / f"temp_{file_path.stem}_{timestamp}.xlsx"
            wb.save(modified_source)
            wb.close()
            
            # Ensure file is written to disk
            time.sleep(1)
            if not modified_source.exists():
                raise FileNotFoundError(f"Temp file failed to save: {modified_source}")
            print(f"  ✓ Saved temp source: {modified_source.name}")

            # Query DB
            conn = self.connect_db()
            ta_df = self.query_ta_data(conn, tower)
            wd_df = self.query_wd_data(conn, tower)
            bh_df = self.query_bh_data(conn, tower)
            conn.close()
            
            ta_df, wd_merged, bh_merged = self.merge_data(ta_df, wd_df, bh_df)
            
            if not self.template_path.exists():
                print(f"  ✗ Template not found: {self.template_path}")
                return None
            
            timestamp_full = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Clean up special characters in filenames
            cluster_clean = re.sub(r'[<>:"/\\|?*]', '_', cluster)
            tower_clean = re.sub(r'[<>:"/\\|?*]', '_', tower)
            
            output_filename = f"SSH_Achievement_Report_NS_{cluster_clean}_{tower_clean}_{timestamp_full}.xlsx"
            output_path = self.output_folder / output_filename
            
            # Step 1: Copy-paste via Excel COM
            print("  ⚡ Step 1: Creating base report with Excel automation...")
            success = self.copy_range_with_excel(
                str(modified_source),
                str(self.template_path),
                str(output_path)
            )
            
            if not success or not output_path.exists():
                print("  ✗ Excel automation failed → falling back to Python-only method")
                result = self.create_report_alternative(file_path, cluster, tower, 
                                                       ta_df, wd_merged, bh_merged)
            else:
                # Step 2: Add charts
                print("  ⚡ Step 2: Adding charts to report...")
                self.add_charts_to_report(output_path, ta_df, wd_merged, bh_merged, cluster, tower)
                result = output_path

            # Cleanup temp
            modified_source.unlink(missing_ok=True)
            
            if result and result.exists():
                print(f"  ✓ Successfully created: {result.name}")
                return result
            else:
                print(f"  ✗ Final report not created")
                return None
                
        except Exception as e:
            print(f"  ✗ Error processing {file_path.name}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def add_charts_to_report(self, report_path, ta_df, wd_merged, bh_merged, cluster, tower):
        try:
            wb = openpyxl.load_workbook(report_path)
            
            if "Justification" in wb.sheetnames:
                just_sheet = wb["Justification"]
                row_offset = 35

                if not ta_df.empty:
                    chart_bytes = self.create_timing_advance_chart(ta_df)
                    if chart_bytes:
                        img = XLImage(BytesIO(chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25
                        print("  ✓ Added Timing Advance chart")

                if bh_merged is not None and not bh_merged.empty:
                    # Add CQI chart
                    cqi_chart_bytes = self.create_cqi_chart(bh_merged)
                    if cqi_chart_bytes:
                        img = XLImage(BytesIO(cqi_chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25
                        print("  ✓ Added CQI chart")
                    
                    # Add QPSK chart
                    qpsk_chart_bytes = self.create_qpsk_chart(bh_merged)
                    if qpsk_chart_bytes:
                        img = XLImage(BytesIO(qpsk_chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25
                        print("  ✓ Added QPSK chart")

                if wd_merged is not None and not wd_merged.empty:
                    chart_bytes = self.create_spectral_efficiency_chart(wd_merged)
                    if chart_bytes:
                        img = XLImage(BytesIO(chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        print("  ✓ Added Spectral Efficiency chart")

            if "SSH Achievement" in wb.sheetnames:
                ssh_sheet = wb["SSH Achievement"]
                ssh_sheet['E34'] = cluster
                ssh_sheet['E34'].font = Font(bold=True, size=12)
                ssh_sheet['E35'] = tower
                ssh_sheet['E35'].font = Font(bold=True, size=12)

            wb.save(report_path)
            wb.close()

        except Exception as e:
            print(f"  ⚠ Could not add charts: {e}")

    def create_report_alternative(self, original_file, cluster, tower, ta_df, wd_merged, bh_merged):
        """Fallback: pure Python method that also copies the data range"""
        print("  ⚡ Using alternative Python-only method...")
        try:
            template_wb = openpyxl.load_workbook(self.template_path)
            template_ws = template_wb["SSH Achievement"]

            # Load original data and copy B6:V34 → C8 in template
            source_wb = openpyxl.load_workbook(original_file)
            source_ws = source_wb["SSH Achievement"]

            for row in source_ws.iter_rows(min_row=6, max_row=34, min_col=2, max_col=22):
                for cell in row:
                    target_row = cell.row - 5 + 7   # B6 → C8 (row +2, col +1)
                    target_col = cell.column + 1
                    target_cell = template_ws.cell(row=target_row, column=target_col)
                    target_cell.value = cell.value
                    if cell.has_style:
                        target_cell.font = copy(cell.font)
                        target_cell.border = copy(cell.border)
                        target_cell.fill = copy(cell.fill)
                        target_cell.number_format = cell.number_format
                        target_cell.protection = copy(cell.protection)
                        target_cell.alignment = copy(cell.alignment)

            source_wb.close()

            # Update cluster/tower
            template_ws['E34'] = cluster
            template_ws['E34'].font = Font(bold=True, size=12)
            template_ws['E35'] = tower
            template_ws['E35'].font = Font(bold=True, size=12)

            # Add charts (same as main path)
            if "Justification" in template_wb.sheetnames:
                just_sheet = template_wb["Justification"]
                row_offset = 35

                if not ta_df.empty:
                    chart_bytes = self.create_timing_advance_chart(ta_df)
                    if chart_bytes:
                        img = XLImage(BytesIO(chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25

                if bh_merged is not None and not bh_merged.empty:
                    # Add CQI chart
                    cqi_chart_bytes = self.create_cqi_chart(bh_merged)
                    if cqi_chart_bytes:
                        img = XLImage(BytesIO(cqi_chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25
                    
                    # Add QPSK chart
                    qpsk_chart_bytes = self.create_qpsk_chart(bh_merged)
                    if qpsk_chart_bytes:
                        img = XLImage(BytesIO(qpsk_chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)
                        row_offset += 25

                if wd_merged is not None and not wd_merged.empty:
                    chart_bytes = self.create_spectral_efficiency_chart(wd_merged)
                    if chart_bytes:
                        img = XLImage(BytesIO(chart_bytes))
                        img.anchor = f'C{row_offset}'
                        just_sheet.add_image(img)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Clean up special characters in filenames
            cluster_clean = re.sub(r'[<>:"/\\|?*]', '_', cluster)
            tower_clean = re.sub(r'[<>:"/\\|?*]', '_', tower)
            
            alt_name = f"SSH_Achievement_Report_NS_{cluster_clean}_{tower_clean}_{timestamp}_ALT.xlsx"
            alt_path = self.output_folder / alt_name
            template_wb.save(alt_path)
            template_wb.close()

            print(f"  ✓ Created alternative report: {alt_name}")
            return alt_path

        except Exception as e:
            print(f"  ✗ Alternative method failed: {e}")
            return None

    def process_all_files(self):
        files = self.list_xlsx_files()
        if not files:
            print("No Excel files found in the input folder.")
            return
        
        print(f"\n{'='*60}")
        print("Starting batch processing...")
        print(f"{'='*60}")
        
        successful = 0
        failed = 0
        
        for file in files:
            result = self.process_file(file)
            if result:
                successful += 1
            else:
                failed += 1
        
        print(f"\n{'='*60}")
        print("Processing complete!")
        print(f"  ✓ Successful: {successful}")
        print(f"  ✗ Failed: {failed}")
        print(f"  Output folder: {self.output_folder}")
        print(f"{'='*60}")


def main():
    # Kill any existing Excel processes before starting
    try:
        subprocess.run(["taskkill", "/F", "/IM", "excel.exe"], 
                      capture_output=True, shell=True)
        time.sleep(2)  # Wait for processes to terminate
    except:
        pass
    
    INPUT_FOLDER = "D:\\NEW SITE\\WORK\\REQ\\"
    TEMPLATE_PATH = "./template.xlsx"
    OUTPUT_FOLDER = "./output_reports"
    DB_PATH = "./newdatabase.db"
    
    # Ensure template exists
    template_path_obj = Path(TEMPLATE_PATH)
    if not template_path_obj.exists():
        print(f"✗ Template file not found at: {TEMPLATE_PATH}")
        print(f"  Current working directory: {Path.cwd()}")
        return
    
    generator = SSHReportGenerator(INPUT_FOLDER, TEMPLATE_PATH, OUTPUT_FOLDER, DB_PATH)
    generator.process_all_files()


if __name__ == "__main__":
    main()