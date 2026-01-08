import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

# Konfigurasi
CSV_FILE = "tower_data.csv"
FLATNESS_THRESHOLD = 0.15
MIN_DATA_POINTS = 5


def load_and_prepare_data(file_path):
    """Memuat dan mempersiapkan data dari CSV"""
    df = pd.read_csv(file_path)
    df["Begin Time"] = pd.to_datetime(df["Begin Time"])
    df = df.sort_values(["towerid", "Begin Time"])

    return df


def calculate_flatness_metrics(values):
    """
    Menghitung metrik untuk mendeteksi apakah data flat
    Returns: (is_flat, coefficient_of_variation, std_dev, mean)
    """
    if len(values) < MIN_DATA_POINTS:
        return False, 0, 0, 0

    mean_val = np.mean(values)
    std_val = np.std(values)
    cv = std_val / mean_val if mean_val > 0 else 0
    is_flat = cv < FLATNESS_THRESHOLD

    return is_flat, cv, std_val, mean_val


def detect_sustained_flat_periods(values, times, threshold=0.03, min_duration=3):
    """
    Mendeteksi periode sustained flat (nilai yang konsisten flat dalam periode tertentu)
    """
    flat_periods = []

    if len(values) < min_duration:
        return flat_periods

    for i in range(len(values) - min_duration + 1):
        window = values[i : i + min_duration]
        window_mean = np.mean(window)
        window_cv = np.std(window) / window_mean if window_mean > 0 else 0

        if window_cv < threshold:
            flat_periods.append(
                {
                    "start_time": times[i],
                    "end_time": times[i + min_duration - 1],
                    "duration": min_duration,
                    "mean_value": window_mean,
                    "cv": window_cv,
                }
            )

    return flat_periods


def analyze_tower_data(df):
    """Analisa semua tower dan klasifikasikan yang flat"""
    results = []

    tower_ids = df["towerid"].unique()

    for tower_id in tower_ids:
        tower_data = df[df["towerid"] == tower_id].copy()

        if len(tower_data) < MIN_DATA_POINTS:
            continue

        values = tower_data["Maximum Receive Speed(Kbps)"].values
        times = tower_data["Begin Time"].values
        is_flat, cv, std_dev, mean_val = calculate_flatness_metrics(values)
        flat_periods = detect_sustained_flat_periods(values, times)

        results.append(
            {
                "towerid": tower_id,
                "is_flat": is_flat,
                "coefficient_of_variation": cv,
                "std_deviation": std_dev,
                "mean_speed": mean_val,
                "data_points": len(values),
                "max_speed": np.max(values),
                "min_speed": np.min(values),
                "flat_periods_count": len(flat_periods),
            }
        )

    return pd.DataFrame(results)


def plot_individual_tower_charts(
    df, tower_ids_to_plot=None, output_folder="tower_charts"
):
    """
    Membuat line chart terpisah untuk setiap tower ID
    Setiap tower disimpan sebagai file gambar tersendiri
    """
    import os

    if tower_ids_to_plot is None:
        tower_ids_to_plot = df["towerid"].unique()

    # Buat folder output jika belum ada
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"✓ Folder '{output_folder}' dibuat")

    total_towers = len(tower_ids_to_plot)
    print(f"   Membuat {total_towers} grafik individual...")

    for idx, tower_id in enumerate(tower_ids_to_plot, 1):
        tower_data = df[df["towerid"] == tower_id].sort_values("Begin Time")

        # Hitung metrik
        values = tower_data["Maximum Receive Speed(Kbps)"].values
        is_flat, cv, std_dev, mean_val = calculate_flatness_metrics(values)

        # Buat figure baru untuk setiap tower
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot line chart
        ax.plot(
            tower_data["Begin Time"],
            tower_data["Maximum Receive Speed(Kbps)"],
            marker="o",
            linewidth=2.5,
            markersize=6,
            color="red" if is_flat else "blue",
            label="Max Receive Speed",
        )

        # Tambahkan reference line untuk mean
        ax.axhline(
            y=mean_val,
            color="orange",
            linestyle="--",
            alpha=0.7,
            linewidth=2,
            label=f"Mean: {mean_val:.0f} Kbps",
        )

        # Styling
        status = "FLAT" if is_flat else "NORMAL"
        title_color = "red" if is_flat else "green"

        ax.set_title(
            f"Tower ID: {tower_id} - Status: [{status}]\n"
            f"CV: {cv:.4f} | Std Dev: {std_dev:.2f} | Mean: {mean_val:.2f} Kbps",
            fontsize=14,
            fontweight="bold",
            color=title_color,
            pad=20,
        )

        ax.set_xlabel("Begin Time", fontsize=12, fontweight="bold")
        ax.set_ylabel("Maximum Receive Speed (Kbps)", fontsize=12, fontweight="bold")
        ax.grid(True, alpha=0.3, linestyle="--")
        ax.legend(loc="best", fontsize=10)

        # Rotate x-axis labels
        plt.xticks(rotation=45, ha="right")

        # Tambahkan info box
        info_text = f"Data Points: {len(values)}\n"
        info_text += f"Min: {np.min(values):.0f} Kbps\n"
        info_text += f"Max: {np.max(values):.0f} Kbps\n"
        info_text += f"Range: {np.max(values) - np.min(values):.0f} Kbps"

        ax.text(
            0.02,
            0.98,
            info_text,
            transform=ax.transAxes,
            fontsize=9,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

        plt.tight_layout()

        # Simpan dengan nama file yang clean
        safe_filename = (
            str(tower_id).replace("/", "_").replace("\\", "_").replace(" ", "_")
        )
        status_prefix = "FLAT_" if is_flat else "NORMAL_"
        filename = f"{output_folder}/{status_prefix}{safe_filename}.png"

        plt.savefig(filename, dpi=150, bbox_inches="tight")
        plt.close(fig)  # Tutup figure untuk menghemat memory

        # Progress indicator
        if idx % 10 == 0 or idx == total_towers:
            print(f"   Progress: {idx}/{total_towers} grafik selesai")

    print(f"\n✓ Semua grafik disimpan di folder '{output_folder}/'")
    print(f"   Total file: {total_towers} gambar")


def generate_report(analysis_df):
    """Generate laporan analisa"""
    print("\n" + "=" * 80)
    print("LAPORAN ANALISA TOWER ID - DETEKSI FLAT LINE")
    print("=" * 80)

    total_towers = len(analysis_df)
    flat_towers = analysis_df[analysis_df["is_flat"] == True]
    normal_towers = analysis_df[analysis_df["is_flat"] == False]

    print(f"\nTotal Tower Dianalisa: {total_towers}")
    print(
        f"Tower dengan Kondisi FLAT: {len(flat_towers)} ({len(flat_towers) / total_towers * 100:.1f}%)"
    )
    print(
        f"Tower dengan Kondisi NORMAL: {len(normal_towers)} ({len(normal_towers) / total_towers * 100:.1f}%)"
    )

    if len(flat_towers) > 0:
        print("\n" + "-" * 80)
        print("DAFTAR TOWER ID DENGAN KONDISI FLAT:")
        print("-" * 80)
        print(
            f"{'Tower ID':<25} {'Mean Speed':<15} {'CV':<10} {'Std Dev':<12} {'Points':<8}"
        )
        print("-" * 80)

        for _, row in flat_towers.sort_values("coefficient_of_variation").iterrows():
            print(
                f"{row['towerid']:<25} {row['mean_speed']:>10.2f} Kbps  {row['coefficient_of_variation']:>8.4f}  "
                f"{row['std_deviation']:>10.2f}  {row['data_points']:>6}"
            )

    print("\n" + "=" * 80)
    print("STATISTIK SUMMARY:")
    print("=" * 80)
    print(
        f"Average CV (Flat towers): {flat_towers['coefficient_of_variation'].mean():.4f}"
        if len(flat_towers) > 0
        else "No flat towers"
    )
    print(
        f"Average CV (Normal towers): {normal_towers['coefficient_of_variation'].mean():.4f}"
        if len(normal_towers) > 0
        else "No normal towers"
    )
    print(f"Threshold yang digunakan: {FLATNESS_THRESHOLD}")

    # Simpan hasil ke CSV
    analysis_df.to_csv("tower_analysis_results.csv", index=False)
    flat_towers.to_csv("flat_towers_only.csv", index=False)
    print(
        "\n✓ Hasil disimpan ke 'tower_analysis_results.csv' dan 'flat_towers_only.csv'"
    )


def main():
    """Fungsi utama"""
    print("Memulai analisa tower data...")

    # Load data
    print(f"\n1. Loading data dari '{CSV_FILE}'...")
    df = load_and_prepare_data(CSV_FILE)
    print(f"   ✓ Data loaded: {len(df)} rows, {df['towerid'].nunique()} unique towers")

    # Analisa
    print("\n2. Menganalisa setiap tower...")
    analysis_df = analyze_tower_data(df)
    print(f"   ✓ Analisa selesai")

    # Generate report
    print("\n3. Generate laporan...")
    generate_report(analysis_df)

    # Plot charts - SEMUA tower, satu file per tower
    print("\n4. Membuat visualisasi charts individual untuk SEMUA tower...")
    all_tower_ids = df["towerid"].unique()
    print(f"   Total tower untuk divisualisasi: {len(all_tower_ids)}")
    plot_individual_tower_charts(df, all_tower_ids, output_folder="tower_charts")

    print("\n" + "=" * 80)
    print("ANALISA SELESAI!")
    print("=" * 80)

    return analysis_df


if __name__ == "__main__":
    analysis_results = main()
