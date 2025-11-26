import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

CSV_FILE = 'tower_data.csv'
FLATNESS_THRESHOLD = 0.1
MIN_DATA_POINTS = 5

def load_and_prepare_data(file_path):
    """Memuat dan mempersiapkan data dari CSV"""
    df = pd.read_csv(file_path)
    df['Begin Time'] = pd.to_datetime(df['Begin Time'])
    df = df.sort_values(['towerid', 'Begin Time'])

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
        window = values[i:i+min_duration]
        window_mean = np.mean(window)
        window_cv = np.std(window) / window_mean if window_mean > 0 else 0
        
        if window_cv < threshold:
            flat_periods.append({
                'start_time': times[i],
                'end_time': times[i+min_duration-1],
                'duration': min_duration,
                'mean_value': window_mean,
                'cv': window_cv
            })
    
    return flat_periods

def analyze_tower_data(df):
    """Analisa semua tower dan klasifikasikan yang flat"""
    results = []
    
    tower_ids = df['towerid'].unique()
    
    for tower_id in tower_ids:
        tower_data = df[df['towerid'] == tower_id].copy()
        
        if len(tower_data) < MIN_DATA_POINTS:
            continue
        
        values = tower_data['Maximum Receive Speed(Kbps)'].values
        times = tower_data['Begin Time'].values
        is_flat, cv, std_dev, mean_val = calculate_flatness_metrics(values)
        flat_periods = detect_sustained_flat_periods(values, times)
        
        results.append({
            'towerid': tower_id,
            'is_flat': is_flat,
            'coefficient_of_variation': cv,
            'std_deviation': std_dev,
            'mean_speed': mean_val,
            'data_points': len(values),
            'max_speed': np.max(values),
            'min_speed': np.min(values),
            'flat_periods_count': len(flat_periods)
        })
    
    return pd.DataFrame(results)

def plot_tower_charts(df, tower_ids_to_plot=None, save_plots=True):
    """
    Membuat line charts untuk tower IDs
    Jika tower_ids_to_plot None, plot semua tower
    """
    if tower_ids_to_plot is None:
        tower_ids_to_plot = df['towerid'].unique()
    if len(tower_ids_to_plot) > 50:
        print(f"Warning: Terlalu banyak tower ({len(tower_ids_to_plot)}). Hanya plot 50 pertama.")
        tower_ids_to_plot = tower_ids_to_plot[:50]
    n_towers = len(tower_ids_to_plot)
    n_cols = 3
    n_rows = (n_towers + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5*n_rows))
    
    if n_rows == 1 and n_cols == 1:
        axes = [axes]
    else:
        axes = axes.flatten() if n_rows > 1 else [axes] if n_cols == 1 else axes
    
    for idx, tower_id in enumerate(tower_ids_to_plot):
        ax = axes[idx]
        tower_data = df[df['towerid'] == tower_id].sort_values('Begin Time')
        
        ax.plot(tower_data['Begin Time'], 
                tower_data['Maximum Receive Speed(Kbps)'], 
                marker='o', linewidth=2, markersize=4)
        
        values = tower_data['Maximum Receive Speed(Kbps)'].values
        is_flat, cv, _, mean_val = calculate_flatness_metrics(values)
        
        ax.set_title(f'{tower_id}\n{"[FLAT]" if is_flat else "[NORMAL]"} CV: {cv:.4f}', 
                     fontsize=10, fontweight='bold',
                     color='red' if is_flat else 'green')
        ax.set_xlabel('Time', fontsize=8)
        ax.set_ylabel('Speed (Kbps)', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45, labelsize=7)
        ax.tick_params(axis='y', labelsize=7)
        
        ax.axhline(y=mean_val, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    
    for idx in range(len(tower_ids_to_plot), len(axes)):
        axes[idx].set_visible(False)
    
    plt.tight_layout()
    
    if save_plots:
        plt.savefig('tower_analysis_charts.png', dpi=150, bbox_inches='tight')
        print("✓ Charts disimpan ke 'tower_analysis_charts.png'")
    
    plt.show()

def generate_report(analysis_df):
    """Generate laporan analisa"""
    print("\n" + "="*80)
    print("LAPORAN ANALISA TOWER ID - DETEKSI FLAT LINE")
    print("="*80)
    
    total_towers = len(analysis_df)
    flat_towers = analysis_df[analysis_df['is_flat'] == True]
    normal_towers = analysis_df[analysis_df['is_flat'] == False]
    
    print(f"\nTotal Tower Dianalisa: {total_towers}")
    print(f"Tower dengan Kondisi FLAT: {len(flat_towers)} ({len(flat_towers)/total_towers*100:.1f}%)")
    print(f"Tower dengan Kondisi NORMAL: {len(normal_towers)} ({len(normal_towers)/total_towers*100:.1f}%)")
    
    if len(flat_towers) > 0:
        print("\n" + "-"*80)
        print("DAFTAR TOWER ID DENGAN KONDISI FLAT:")
        print("-"*80)
        print(f"{'Tower ID':<25} {'Mean Speed':<15} {'CV':<10} {'Std Dev':<12} {'Points':<8}")
        print("-"*80)
        
        for _, row in flat_towers.sort_values('coefficient_of_variation').iterrows():
            print(f"{row['towerid']:<25} {row['mean_speed']:>10.2f} Kbps  {row['coefficient_of_variation']:>8.4f}  "
                  f"{row['std_deviation']:>10.2f}  {row['data_points']:>6}")
    
    print("\n" + "="*80)
    print("STATISTIK SUMMARY:")
    print("="*80)
    print(f"Average CV (Flat towers): {flat_towers['coefficient_of_variation'].mean():.4f}" if len(flat_towers) > 0 else "No flat towers")
    print(f"Average CV (Normal towers): {normal_towers['coefficient_of_variation'].mean():.4f}" if len(normal_towers) > 0 else "No normal towers")
    print(f"Threshold yang digunakan: {FLATNESS_THRESHOLD}")
    
    analysis_df.to_csv('tower_analysis_results.csv', index=False)
    flat_towers.to_csv('flat_towers_only.csv', index=False)
    print("\n✓ Hasil disimpan ke 'tower_analysis_results.csv' dan 'flat_towers_only.csv'")

def main():
    """Fungsi utama"""
    print("Memulai analisa tower data...")
    
    print(f"\n1. Loading data dari '{CSV_FILE}'...")
    df = load_and_prepare_data(CSV_FILE)
    print(f"   ✓ Data loaded: {len(df)} rows, {df['towerid'].nunique()} unique towers")
    print("\n2. Menganalisa setiap tower...")
    analysis_df = analyze_tower_data(df)
    print(f"   ✓ Analisa selesai")
    print("\n3. Generate laporan...")
    generate_report(analysis_df)
    
    print("\n4. Membuat visualisasi charts...")
    flat_tower_ids = analysis_df[analysis_df['is_flat'] == True]['towerid'].values
    
    if len(flat_tower_ids) > 0:
        print(f"   Plotting {len(flat_tower_ids)} tower dengan kondisi FLAT...")
        plot_tower_charts(df, flat_tower_ids, save_plots=True)
    else:
        print("   Tidak ada tower dengan kondisi flat untuk divisualisasi")
    
    print("\n" + "="*80)
    print("ANALISA SELESAI!")
    print("="*80)
    
    return analysis_df

if __name__ == "__main__":
    analysis_results = main()