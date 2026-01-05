import os
import re
import sqlite3
import traceback
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import openpyxl
import pandas as pd
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Font
from io import BytesIO

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext


class SSHReportGenerator:
    def __init__(self, input_folder: str, template_path: str, output_folder: str, db_path: str):
        self.input_folder = Path(input_folder)
        self.template_path = Path(template_path)
        self.output_folder = Path(output_folder)
        self.db_path = Path(db_path)
        self.output_folder.mkdir(exist_ok=True)

        self.color_palette = [
            "#080cec", "#ef17e8", "#52eb0c", "#f59e0b", "#10b981",
            "#06b6d4", "#ef4444", "#a855f7", "#14b8a6", "#f97316"
        ]
        self.band_colors = {
            "850": "#52eb0c", "1800": "#080cec", "2100": "#ef17e8",
            "2300": "#F39C12", "2600": "#9B59B6",
        }

    def connect_db(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        return sqlite3.connect(self.db_path)

    @staticmethod
    def parse_date_flexible(date_str) -> Optional[datetime]:
        if pd.isna(date_str):
            return None
        formats = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        return None

    def query_all_data(self, conn: sqlite3.Connection, tower_id: str):
        queries = {
            'ta': """
                SELECT * FROM tbl_newta 
                WHERE newta_managed_element = ?
                ORDER BY newta_date DESC LIMIT 1
            """,
            'wd': """
                SELECT newwd_date, newwd_moentity, newwd_cell_fdd_band, 
                       newwd_spectral_efficiency_dl_num, newwd_spectral_efficiency_dl_den,
                       newwd_enodeb_fdd_msc
                FROM tbl_newwd WHERE newwd_enodeb_fdd_msc = ?
                ORDER BY newwd_date
            """,
            'bh': """
                SELECT newbh_date, newbh_moentity, 
                       newbh_cell_average_cqi_num, newbh_cell_average_cqi_den,
                       newbh_cell_qpsk_rate_num, newbh_cell_qpsk_rate_den,
                       newbh_cell_fdd_band, newbh_enodeb_fdd_msc
                FROM tbl_newbh WHERE newbh_enodeb_fdd_msc = ?
                ORDER BY newbh_date
            """
        }

        ta_df = pd.read_sql_query(queries['ta'], conn, params=(tower_id,))
        wd_df = pd.read_sql_query(queries['wd'], conn, params=(tower_id,))
        bh_df = pd.read_sql_query(queries['bh'], conn, params=(tower_id,))

        wd_df['newwd_date'] = wd_df['newwd_date'].apply(self.parse_date_flexible)
        bh_df['newbh_date'] = bh_df['newbh_date'].apply(self.parse_date_flexible)

        return ta_df, wd_df, bh_df

    def merge_data(self, ta_df, wd_df, bh_df):
        if ta_df.empty:
            return None, None, None

        sector_map = ta_df[['newta_eutrancell', 'newta_sector', 'newta_sector_name']]

        wd_merged = pd.merge(wd_df, sector_map, left_on='newwd_moentity', right_on='newta_eutrancell', how='left') if not wd_df.empty else None
        bh_merged = pd.merge(bh_df, sector_map, left_on='newbh_moentity', right_on='newta_eutrancell', how='left') if not bh_df.empty else None

        return ta_df, wd_merged, bh_merged

    def create_timing_advance_chart(self, ta_df) -> Optional[BytesIO]:
        if ta_df.empty:
            return None

        sectors = ta_df['newta_sector_name'].dropna().unique()
        if len(sectors) == 0:
            return None

        fig, axes = plt.subplots(len(sectors), 1, figsize=(10, 4 * len(sectors)))
        axes = [axes] if len(sectors) == 1 else axes

        distance_labels = [
            "0-78", "78-234", "234-390", "390-546", "546-702", "702-858",
            "858-1014", "1014-1560", "1560-2106", "2106-2652", "2652-3120",
            "3120-3900", "3900-6318", "6318-10062", "10062-13962", "13962-20000"
        ]
        distance_cols = [f'newta_{l.replace("-", "_")}_m' for l in distance_labels]
        cdf_cols = [f'newta_{v}' for v in ["78", "234", "390", "546", "702", "858", "1014", "1560", "2106", "2652", "3120", "3900", "6318", "10062", "13962", "20000"]]

        x_pos = np.arange(len(distance_labels))

        for ax, sector in zip(axes, sectors):
            sector_data = ta_df[ta_df['newta_sector_name'] == sector]
            for _, row in sector_data.iterrows():
                band = str(int(row['newta_band'])) if pd.notna(row['newta_band']) else "Unknown"
                color = self.band_colors.get(band, "#95A5A6")

                dist_vals = [row.get(c, 0) or 0 for c in distance_cols]
                cdf_vals = [row.get(c, 0) or 0 for c in cdf_cols]

                ax.bar(x_pos, dist_vals, alpha=0.6, color=color, label=f'L{band} Samples')
                ax2 = ax.twinx()
                ax2.plot(x_pos, cdf_vals, 'o-', color=color, linewidth=2, label=f'L{band} CDF')
                ax2.axhline(90, color='red', linestyle='--', linewidth=2)
                ax2.set_ylim(0, 105)
                ax2.set_ylabel('CDF (%)')

            ax.set_title(f'Sector {sector}', fontweight='bold')
            ax.set_xlabel('Distance (m)')
            ax.set_ylabel('Samples')
            ax.set_xticks(x_pos)
            ax.set_xticklabels(distance_labels, rotation=45, ha='right', fontsize=8)
            ax.grid(True, alpha=0.3)
            ax.legend(loc='upper left', fontsize=8)
            ax2.legend(loc='upper right', fontsize=8)

        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    def create_line_chart(self, df: pd.DataFrame, date_col: str, value_col: str, band_col: str,
                          y_label: str, title_suffix: str) -> Optional[BytesIO]:
        if df is None or df.empty:
            return None

        sectors = df['newta_sector_name'].dropna().unique()
        if len(sectors) == 0:
            return None

        cols = 3
        rows = (len(sectors) + cols - 1) // cols
        fig, axes = plt.subplots(rows, cols, figsize=(15, 4 * rows))

        # Ensure axes is always a flat iterable
        if rows == 1 and cols == 1:
            axes_flat = [axes]
        elif rows == 1 or cols == 1:
            axes_flat = axes.flatten() if hasattr(axes, 'flatten') else [axes]
        else:
            axes_flat = axes.flatten()

        for i, sector in enumerate(sectors):
            ax = axes_flat[i]
            sector_data = df[df['newta_sector_name'] == sector]
            unique_bands = sector_data[band_col].dropna().unique()

            for j, band in enumerate(unique_bands):
                band_data = sector_data[sector_data[band_col] == band].sort_values(date_col)
                if band_data.empty:
                    continue
                color = self.color_palette[j % len(self.color_palette)]
                ax.plot(band_data[date_col], band_data[value_col],
                        'o-', label=f'L{int(band)}', color=color, linewidth=2)

            ax.set_title(f'Sector {sector} - {title_suffix}', fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel(y_label)
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))

        # Hide unused subplots
        for i in range(len(sectors), len(axes_flat)):
            axes_flat[i].axis('off')

        plt.tight_layout()
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    @staticmethod
    def extract_cluster_tower(text: str) -> Tuple[str, str]:
        text = str(text or "")
        cluster = re.search(r'Cluster\s*:\s*([^T]+?)Tower', text, re.I)
        tower = re.search(r'Tower\s*:\s*([^\s(]+)', text, re.I)
        return (cluster.group(1).strip() if cluster else "Unknown",
                tower.group(1).strip() if tower else "Unknown")

    def apply_conditional_values(self, ws):
        """Apply values to K, N, Q, T columns based on M28, P28, S28, V28"""
        configs = [
            ('M28', 'K', 11, 25, ["99.00", "98.00", "1.00", "97.00", "8.00", "50.00", "1.10",
                                 "1.00", "120.00", "5.00", "1.00", "20.00", "-105.00", "30.00"]),
            ('P28', 'N', 11, 28, ["99.00", "98.00", "1.00", "97.00", "9.00", "40.00", "1.50",
                                 "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00",
                                 "98.00", "98.00", "5.00"]),
            ('S28', 'Q', 11, 25, ["99.00", "98.00", "1.00", "97.00", "9.00", "40.00", "1.70",
                                 "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00"]),
            ('V28', 'T', 11, 25, ["99.00", "98.00", "1.00", "97.00", "10.00", "40.00", "1.90",
                                 "1.00", "120.00", "10.00", "1.50", "35.00", "-105.00", "30.00"])
        ]

        for trigger_cell, col_letter, start_row, end_row, values in configs:
            try:
                val = ws[trigger_cell].value
                if val and float(val) > 0:
                    for i, row_num in enumerate(range(start_row, end_row)):
                        cell = ws[f'{col_letter}{row_num}']
                        cell.value = values[i]
                        cell.number_format = '@'
                else:
                    for row_num in range(start_row, end_row):
                        cell = ws[f'{col_letter}{row_num}']
                        cell.value = "-"
                        cell.number_format = '@'
            except Exception as e:
                print(f"  Warning on {trigger_cell}: {e}")

    def process_file(self, file_path: Path, log_callback=None) -> Optional[Path]:
        def log(msg: str):
            print(msg)
            if log_callback:
                log_callback(msg + "\n")

        log(f"Processing: {file_path.name}")

        try:
            wb = openpyxl.load_workbook(file_path)
            if "Cluster & Tower" not in wb.sheetnames:
                log("  Sheet 'Cluster & Tower' not found")
                return None

            ws = wb["Cluster & Tower"]
            ws.title = "SSH Achievement"

            # Remove other sheets
            for sheet_name in list(wb.sheetnames):
                if sheet_name != "SSH Achievement":
                    wb.remove(wb[sheet_name])

            cluster, tower = self.extract_cluster_tower(ws['B6'].value)
            log(f"  Cluster: {cluster} | Tower: {tower}")

            # Apply conditional values
            self.apply_conditional_values(ws)
            log("  Applied conditional values")

            # Query database
            conn = self.connect_db()
            ta_df, wd_df, bh_df = self.query_all_data(conn, tower)
            ta_df, wd_merged, bh_merged = self.merge_data(ta_df, wd_df, bh_df)
            conn.close()

            # Generate charts
            ta_chart = self.create_timing_advance_chart(ta_df)

            se_chart = None
            if wd_merged is not None and not wd_merged.empty:
                wd_merged['value'] = np.where(wd_merged['newwd_spectral_efficiency_dl_den'] > 0,
                                              wd_merged['newwd_spectral_efficiency_dl_num'] / wd_merged['newwd_spectral_efficiency_dl_den'],
                                              np.nan)
                se_chart = self.create_line_chart(wd_merged, 'newwd_date', 'value', 'newwd_cell_fdd_band',
                                                  'Spectral Efficiency (bps/Hz)', 'Spectral Efficiency')

            cqi_chart = qpsk_chart = None
            if bh_merged is not None and not bh_merged.empty:
                bh_merged['cqi'] = np.where(bh_merged['newbh_cell_average_cqi_den'] > 0,
                                            bh_merged['newbh_cell_average_cqi_num'] / bh_merged['newbh_cell_average_cqi_den'],
                                            np.nan)
                cqi_chart = self.create_line_chart(bh_merged, 'newbh_date', 'cqi', 'newbh_cell_fdd_band',
                                                   'Average CQI', 'CQI')

                bh_merged['qpsk'] = np.where(bh_merged['newbh_cell_qpsk_rate_den'] > 0,
                                             (bh_merged['newbh_cell_qpsk_rate_num'] / bh_merged['newbh_cell_qpsk_rate_den']) * 100,
                                             np.nan)
                qpsk_chart = self.create_line_chart(bh_merged, 'newbh_date', 'qpsk', 'newbh_cell_fdd_band',
                                                    'QPSK Rate (%)', 'QPSK Rate')

            if not self.template_path.exists():
                log(f"  Template missing: {self.template_path}")
                return None

            template_wb = openpyxl.load_workbook(self.template_path)

            # Copy full sheet content to template (preserves formatting)
            if "SSH Achievement" in template_wb.sheetnames:
                target = template_wb["SSH Achievement"]
                for row in ws.iter_rows():
                    for cell in row:
                        t_cell = target.cell(row=cell.row, column=cell.column)
                        t_cell.value = cell.value
                        t_cell.font = cell.font.copy()
                        t_cell.border = cell.border.copy()
                        t_cell.fill = cell.fill.copy()
                        t_cell.number_format = cell.number_format
                        t_cell.alignment = cell.alignment.copy()

                target['E34'].value = cluster
                target['E34'].font = Font(bold=True, size=12)
                target['E35'].value = tower
                target['E35'].font = Font(bold=True, size=12)

            # Insert charts into Justification sheet
            if "Justification" in template_wb.sheetnames:
                just_sheet = template_wb["Justification"]
                row = 35
                for chart_buf, name in [(ta_chart, "Timing Advance"), (cqi_chart, "CQI"),
                                        (qpsk_chart, "QPSK"), (se_chart, "Spectral Efficiency")]:
                    if chart_buf:
                        img = XLImage(chart_buf)
                        img.anchor = f'C{row}'
                        just_sheet.add_image(img)
                        row += 28

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_folder / f"SSH Achievement Report_NS_{cluster}_{tower}_{timestamp}.xlsx"
            template_wb.save(output_file)
            log(f"  Saved: {output_file.name}")
            return output_file

        except Exception as e:
            log(f"  Error: {str(e)}")
            traceback.print_exc()
            return None


# ========================= GUI =========================
class SSHReportGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("SSH Achievement Report Generator")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        self.running = False
        self.thread = None

        self.vars = {
            'input': tk.StringVar(value="./input_files"),
            'template': tk.StringVar(value="./template.xlsx"),
            'output': tk.StringVar(value="./output_reports"),
            'db': tk.StringVar(value="./mydatabase.db")
        }

        self.create_widgets()

    def create_widgets(self):
        style = ttk.Style()
        style.theme_use('clam')

        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill='both', expand=True)

        ttk.Label(main_frame, text="SSH Achievement Report Generator", font=("Helvetica", 18, "bold")).grid(
            row=0, column=0, columnspan=3, pady=20)

        paths = [
            ("Input Folder (*.xlsx):", 'input', True),
            ("Template File:", 'template', False),
            ("Output Folder:", 'output', True),
            ("Database File:", 'db', False),
        ]

        for i, (label, key, is_folder) in enumerate(paths, start=1):
            ttk.Label(main_frame, text=label).grid(row=i, column=0, sticky='e', pady=8, padx=5)
            ttk.Entry(main_frame, textvariable=self.vars[key], width=70).grid(row=i, column=1, pady=8, padx=5)
            ttk.Button(
                main_frame,
                text="Browse",
                command=lambda k=key, f=is_folder: self.browse(k, f)
            ).grid(row=i, column=2, padx=5)

        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=5, column=0, columnspan=3, pady=20)
        self.start_btn = ttk.Button(btn_frame, text="Start Processing", command=self.toggle_processing, style="Accent.TButton")
        self.start_btn.pack(side='left', padx=10)

        ttk.Label(main_frame, text="Processing Log:").grid(row=6, column=0, sticky='w', pady=(20,5))
        self.log_text = scrolledtext.ScrolledText(main_frame, height=20, state='disabled', wrap='word')
        self.log_text.grid(row=7, column=0, columnspan=3, sticky='nsew', padx=5)

        main_frame.grid_rowconfigure(7, weight=1)
        main_frame.grid_columnconfigure(1, weight=1)

    def browse(self, key: str, is_folder: bool):
        if is_folder:
            path = filedialog.askdirectory(title="Select Folder")
        else:
            filetypes = [("Excel Files", "*.xlsx"), ("SQLite DB", "*.db"), ("All Files", "*.*")]
            path = filedialog.askopenfilename(title="Select File", filetypes=filetypes)
        if path:
            self.vars[key].set(path)

    def log(self, message: str):
        self.log_text.config(state='normal')
        self.log_text.insert('end', message)
        self.log_text.see('end')
        self.log_text.config(state='disabled')

    def toggle_processing(self):
        if self.running:
            self.running = False
            self.start_btn.config(text="Start Processing")
            self.log("Processing stopped by user.\n")
            return

        paths = {k: v.get().strip() for k, v in self.vars.items()}
        missing = [k for k, p in paths.items() if not p]
        if missing:
            messagebox.showerror("Error", f"Please select: {', '.join(missing)}")
            return

        input_path = Path(paths['input'])
        if not list(input_path.glob("*.xlsx")):
            messagebox.showwarning("Warning", "No .xlsx files found in input folder")
            return

        self.running = True
        self.start_btn.config(text="Stop Processing")
        self.log("Starting processing...\n" + "="*60 + "\n")

        self.thread = threading.Thread(target=self.run_processing, args=(paths,), daemon=True)
        self.thread.start()

    def run_processing(self, paths: dict):
        generator = SSHReportGenerator(
            input_folder=paths['input'],
            template_path=paths['template'],
            output_folder=paths['output'],
            db_path=paths['db']
        )
        files = sorted(Path(paths['input']).glob("*.xlsx"))
        success = fail = 0

        for file in files:
            if not self.running:
                break
            result = generator.process_file(file, log_callback=self.log)
            if result:
                success += 1
            else:
                fail += 1

        summary = f"\n{'='*60}\nProcessing finished!\nSuccess: {success} | Failed: {fail}\nOutput: {paths['output']}\n{'='*60}\n"
        self.log(summary)

        self.root.after(0, lambda: messagebox.showinfo("Done", f"Success: {success}\nFailed: {fail}"))
        self.root.after(0, lambda: self.start_btn.config(text="Start Processing"))
        self.running = False

if __name__ == "__main__":
    root = tk.Tk()
    app = SSHReportGUI(root)
    root.mainloop()