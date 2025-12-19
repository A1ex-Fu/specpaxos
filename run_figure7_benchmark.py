# generates figure 7 graph based on teh outputs csv's
import pandas as pd
import matplotlib.pyplot as plt
import glob
import re
import os
from matplotlib.ticker import FuncFormatter

def get_max_throughput_and_droprate(filepath):
    """
    Parses the filename to get the drop rate and reads the CSV 
    to find the maximum throughput.
    """
    filename = os.path.basename(filepath)
    
    # Extract drop rate from filename "results_vr_xxx_drop.csv"
    match = re.search(r'results_vr_(.*)_drop.csv', filename)
    if match:
        drop_rate_str = match.group(1)
        try:
            drop_rate_val = float(drop_rate_str)
        except ValueError:
            return None
    else:
        return None

    try:
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()
        
        if 'Throughput_ops' in df.columns:
            max_throughput = df['Throughput_ops'].max()
            return (drop_rate_val, max_throughput)
        else:
            print(f"Column 'Throughput_ops' not found in {filename}")
            return None
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None

def to_percent(x, pos):
    """Formatter to convert raw probability to percentage string."""
    val = x * 100 
    if val >= 1:
        return f'{int(val)}%'
    else:
        return f'{val:.3g}%'

def y_fmt(x, pos):
    """Formatter for Y-axis (e.g., 200000 -> 200K)"""
    if x == 0:
        return "0K"
    return f'{int(x/1000)}K'

def main():
    # 1. Find files in the 'outputs' directory
    search_path = os.path.join("outputs", "results_vr_*_drop.csv")
    files = glob.glob(search_path)
    
    data = []
    if not files:
        print(f"No files found matching pattern '{search_path}'")
        return
    else:
        print(f"Found {len(files)} files. Processing...")
        for f in files:
            res = get_max_throughput_and_droprate(f)
            if res:
                data.append(res)
    
    # Sort by drop rate
    data.sort(key=lambda x: x[0])
    
    if not data:
        print("No valid numeric data found to plot.")
        return

    drop_rates, throughputs = zip(*data)

    # 3. Plotting
    fig, ax = plt.subplots(figsize=(8, 5))

    # Plot the line
    ax.plot(drop_rates, throughputs, marker='o', linestyle='-', color='#1f77b4', label='VR', linewidth=1.5)

    # 4. Styling
    
    # X-Axis: Log scale, fixed limits 0.001% -> 1%
    ax.set_xscale('log')
    ax.set_xlabel("Simulated drop rate", fontsize=12, fontfamily='serif')
    ax.set_xlim(0.00001, 0.01)
    
    ticks = [0.00001, 0.0001, 0.0005, 0.001, 0.01]
    ax.set_xticks(ticks)
    ax.get_xaxis().set_major_formatter(FuncFormatter(to_percent))
    
    # Y-Axis: Fixed limits 0 -> 350K
    ax.set_ylabel("Throughput (ops/sec)", fontsize=12, fontfamily='serif')
    ax.set_ylim(0, 350000)
    ax.yaxis.set_major_formatter(FuncFormatter(y_fmt))
    
    # Ticks facing inward
    ax.tick_params(direction='in', which='both', top=True, right=True)

    # Caption
    plt.figtext(0.5, 0.01, "Figure 7: Maximum throughput with simulated packet dropping.", 
                wrap=True, horizontalalignment='center', fontsize=12, style='italic', fontfamily='serif')

    plt.tight_layout(rect=[0, 0.05, 1, 1]) 

    # 5. Save
    output_file = os.path.join("outputs", "max_throughput_with_drops.png")
    plt.savefig(output_file, dpi=300)
    print(f"Plot saved to {output_file}")

if __name__ == "__main__":
    main()