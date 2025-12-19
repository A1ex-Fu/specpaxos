import subprocess
import os
import re
import time
import statistics
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

NISTORE_DIR = "nistore"
CLIENT_BIN = "sudo ./benchClient"
CONFIG_FILE = "test"
KEYS_FILE = "keys.txt"
PROTOCOL = "vr-l"
DURATION = 30
WRITE_PERCENT = 50
OUTPUT_DIR = "outputs"
CLIENT_COUNTS = [2, 5, 10, 20, 50, 100, 150]
WARMUP_TIME = 5

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

COMMIT_COUNT_RE = re.compile(r"# Commit:\s+(\d+),")
TRACE_RE = re.compile(r"^\d+\s+[0-9.]+\s+[0-9.]+\s+(\d+)\s+1")

def run_clients(n_clients):
    print(f"\n=== Running {n_clients} clients for protocol '{PROTOCOL}' ===")
    procs = []
    
    for i in range(n_clients):
        cmd = f"{CLIENT_BIN} -c {CONFIG_FILE} -f {KEYS_FILE} -m {PROTOCOL} -d {DURATION} -w {WRITE_PERCENT}"
        p = subprocess.Popen(
            cmd.split(),
            cwd=NISTORE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        procs.append((i, p))
        time.sleep(0.1)

    client_data = []

    for i, p in procs:
        try:
            stdout, stderr = p.communicate()
            
            tput = 0.0
            commit_match = COMMIT_COUNT_RE.search(stdout)
            if commit_match:
                total_commits = int(commit_match.group(1))
                tput = total_commits / DURATION
            
            latencies = []
            for line in stderr.splitlines():
                match = TRACE_RE.search(line.strip())
                if match:
                    lat_us = int(match.group(1))
                    latencies.append(lat_us)
            
            median_lat = 0.0
            if latencies:
                median_lat = statistics.median(latencies)

            if tput > 0 and median_lat > 0:
                client_data.append((tput, median_lat))
            else:
                print(f"  Client {i}: Failed to parse data.")

        except Exception as e:
            print(f"  Client {i}: Exception {e}")

    if client_data:
        total_throughput = sum(c[0] for c in client_data)
        agg_median_latency = statistics.median(c[1] for c in client_data)
        
        print(f"→ Total Throughput: {total_throughput:.2f} ops/sec")
        print(f"→ Median Latency:   {agg_median_latency:.2f} µs")
        return total_throughput, agg_median_latency
    else:
        print("→ No valid data collected.")
        return None, None

def plot_results(results, protocol):
    valid_results = [r for r in results if r[1] is not None]
    if not valid_results:
        print("No data to plot.")
        return

    client_counts = [r[0] for r in valid_results]
    throughputs = [r[1] for r in valid_results]
    latencies = [r[2] for r in valid_results]

    plt.figure(figsize=(8, 6))
    plt.plot(throughputs, latencies, marker='o', label=protocol, linestyle='-')

    for i, count in enumerate(client_counts):
        plt.annotate(str(count),
                     (throughputs[i], latencies[i]),
                     textcoords="offset points",
                     xytext=(0, 10),
                     ha='center', fontsize=8, color='blue')

    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Median Latency (µs)")
    plt.title(f"Median Latency vs Throughput — {protocol}")
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.legend()
    
    plot_path = os.path.join(OUTPUT_DIR, f"latency_vs_throughput_under_10ms_slo_{protocol}.png")
    plt.savefig(plot_path)
    print(f"\nPlot saved to {plot_path}")

def append_result(csv_path, clients, tput, median):
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["Clients", "Throughput_ops", "Median_Latency_us"])
        writer.writerow([clients, tput, median])

def load_existing_results(csv_path):
    existing = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    n = int(row["Clients"])
                    tput = float(row["Throughput_ops"])
                    lat = float(row["Median_Latency_us"])
                    existing[n] = (tput, lat)
                except:
                    pass
    return existing

def main():
    csv_path = os.path.join(OUTPUT_DIR, f"results_{PROTOCOL}.csv")
    existing_results = load_existing_results(csv_path)
    final_results_list = []

    for n_clients in CLIENT_COUNTS:
        if n_clients in existing_results:
            tput, median = existing_results[n_clients]
            print(f"\n→ Skipping {n_clients} clients (found in CSV): {tput:.2f} ops/sec, {median:.2f} µs")
            final_results_list.append((n_clients, tput, median))
            continue

        time.sleep(WARMUP_TIME)
        
        tput, median = run_clients(n_clients)
        
        if tput is not None:
            append_result(csv_path, n_clients, tput, median)
            final_results_list.append((n_clients, tput, median))

    final_results_list.sort(key=lambda x: x[0])
    plot_results(final_results_list, PROTOCOL)

if __name__ == "__main__":
    main()