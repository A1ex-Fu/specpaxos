import subprocess
import os
import re
import time
import statistics
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Settings
CLIENT_BIN = "./bench/client"
REPLICA_BIN = "./bench/replica"
CONFIG_FILE = "testConfig.txt"
OUTPUT_DIR = "outputs"
RESULT_DIR = "outputs"
CSV_EXPORT = True
TOTAL_REQUESTS = 20000
MIN_REQUESTS_PER_CLIENT = 400
CLIENT_COUNTS = [2, 5, 10, 20, 50, 100, 150, 200, 250, 300]
REPLICA_COUNT = 3
WARMUP_TIME = 5

PROTOCOLS = ["spec", "vr"]
# PROTOCOLS = ["spec", "vr", "paxos-batch64", "fastpaxos"]

for directory in [OUTPUT_DIR, RESULT_DIR]:
    os.makedirs(directory, exist_ok=True)

THROUGHPUT_RE = re.compile(r"Completed\s+(\d+)\s+requests\s+in\s+([0-9.]+)\s+seconds")
MEDIAN_LAT_RE = re.compile(r"Median latency is\s+([0-9]+)\s+ns")

def start_replicas(protocol):
    replicas = []
    for i in range(REPLICA_COUNT):
        log_path = os.path.join(OUTPUT_DIR, f"replica_{i}_{protocol}.log")
        log_file = open(log_path, "w")
        args = [REPLICA_BIN, "-c", CONFIG_FILE, "-i", str(i)]

        mode = "vr" if protocol == "paxos-batch64" else protocol
        args += ["-m", mode]

        if protocol == "paxos-batch64":
            args += ["-b", "32"]

        proc = subprocess.Popen(args, stdout=log_file, stderr=log_file, text=True)
        replicas.append((proc, log_file))
    time.sleep(5)
    return replicas

def kill_replicas(replicas):
    for proc, _ in replicas:
        proc.terminate()
    for proc, log_file in replicas:
        proc.wait()
        log_file.close()

def run_clients(n_clients, protocol):
    print(f"\n=== Running {n_clients} clients for protocol '{protocol}' ===")
    procs = []
    requests_per_client = max(MIN_REQUESTS_PER_CLIENT, TOTAL_REQUESTS // n_clients)
    outputs = []

    for i in range(n_clients):
        args = [
            CLIENT_BIN,
            "-c", CONFIG_FILE,
            "-m", "vr" if protocol == "paxos-batch64" else protocol,
            "-n", str(requests_per_client),
            "-t", "1"
        ]

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append((i, p))

    for i, p in procs:
        stdout, _ = p.communicate()
        print(f"\n[Client {i} Output]\n{stdout}")
        outputs.append((i, stdout))

    throughputs, medians = [], []
    for i, stdout in outputs:
        client_tput = None
        client_median = None
        for line in stdout.splitlines():
            t_match = THROUGHPUT_RE.search(line)
            if t_match:
                try:
                    completed = int(t_match.group(1))
                    seconds = float(t_match.group(2))
                    client_tput = completed / seconds
                except Exception as e:
                    print(f"[ParseError] Throughput: {e}")
            lat_match = MEDIAN_LAT_RE.search(line)
            if lat_match:
                try:
                    latency_ns = int(lat_match.group(1))
                    client_median = latency_ns / 1000  # µs
                except Exception as e:
                    print(f"[ParseError] Latency: {e}")
        if client_tput is not None and client_median is not None:
            throughputs.append(client_tput)
            medians.append(client_median)

    if throughputs and medians:
        total_tput = sum(throughputs)
        median_latency = statistics.median(medians)
        print(f"\n→ Total Throughput: {total_tput:.2f} ops/sec, Median of Medians Latency: {median_latency:.1f} µs")
        return total_tput, median_latency
    else:
        print("\n→ Failed to collect data from any client.")
        return None, None

def plot_all_results(all_protocol_results):
    plt.figure(figsize=(10, 7))

    for protocol, results in all_protocol_results.items():
        results = [r for r in results if r[1] is not None]
        throughputs = [r[1] for r in results]
        latencies = [r[2] for r in results]

        plt.plot(throughputs, latencies, marker='o', label=protocol)
        for i in range(len(throughputs)):
            label = f"{results[i][0]}"
            plt.annotate(label,
                         (throughputs[i], latencies[i]),
                         textcoords="offset points",
                         xytext=(5, 5),
                         ha='left', fontsize=8, color='gray')

    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Median Latency (µs)")
    plt.title("Median Latency vs Throughput — All Protocols")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plot_path = os.path.join(RESULT_DIR, "latency_vs_throughput_all.png")
    plt.savefig(plot_path)
    print(f"Plot saved as {plot_path}")

def export_csv(results, protocol):
    csv_path = os.path.join(RESULT_DIR, f"results_{protocol}.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Clients", "Throughput_ops", "Median_Latency_us"])
        for row in results:
            writer.writerow(row)
    print(f"Exported CSV: {csv_path}")

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
                except Exception as e:
                    print(f"[CSV ParseError] {e}")
    return existing

def append_result(csv_path, clients, tput, median):
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["Clients", "Throughput_ops", "Median_Latency_us"])
        writer.writerow([clients, tput, median])

def main():
    all_protocol_results = {}

    for protocol in PROTOCOLS:
        print(f"\n=== Benchmarking Protocol: {protocol} ===")
        all_results = []
        csv_path = os.path.join(RESULT_DIR, f"results_{protocol}.csv")
        existing_results = load_existing_results(csv_path)

        for n_clients in CLIENT_COUNTS:
            if n_clients in existing_results:
                tput, median = existing_results[n_clients]
                print(f"→ Skipping {n_clients} clients (cached): Throughput = {tput:.2f}, Latency = {median:.1f}")
                all_results.append((n_clients, tput, median))
                continue

            replicas = start_replicas(protocol)
            time.sleep(WARMUP_TIME)

            tput, median = run_clients(n_clients, protocol)
            kill_replicas(replicas)

            if tput is not None and median is not None:
                append_result(csv_path, n_clients, tput, median)
                all_results.append((n_clients, tput, median))

        all_protocol_results[protocol] = all_results
        export_csv(all_results, protocol)

    plot_all_results(all_protocol_results)

if __name__ == "__main__":
    main()
