import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Analyze LoRaMESH route stability")
    parser.add_argument("trace_file", help="Path to the trace CSV file (e.g., loramesh-trace-Baseline.csv)")
    return parser.parse_args()

def analyze_routes(df):
    # Filter for ROUTE events
    routes = df[df['Type'] == 'ROUTE'].copy()
    
    if routes.empty:
        print("No route events found.")
        return

    # Convert Time to numeric
    routes['Time'] = pd.to_numeric(routes['Time'])
    
    # 1. Convergence: Updates per second over time
    routes['TimeBin'] = routes['Time'].astype(int)
    updates_per_sec = routes.groupby('TimeBin').size()
    
    plt.figure(figsize=(10, 6))
    updates_per_sec.plot()
    plt.title("Route Updates per Second")
    plt.xlabel("Time (s)")
    plt.ylabel("Updates")
    plt.grid(True)
    plt.savefig("route_convergence.png")
    print("Saved route_convergence.png")

    # 2. Route Stability: Duration of routes?
    # Difficult to track without unique route IDs, but we can see churn.
    
    # 3. Hops Distribution
    # Filter for 'NEW' or 'UPDATE' actions where Hops is valid
    valid_routes = routes[routes['Details'].str.contains('hops=', na=False) | (routes['Hops'] != '-')]
    # Note: In our CSV format, Hops is in the 'Hops' column for RX/TX, but for ROUTE it might be mixed?
    # Let's check the Tracer implementation.
    # TraceRouteUpdate: ... (int)hops ...
    # It puts hops in the Hops column.
    
    # Clean up Hops column (remove '-')
    routes_hops = routes[routes['Hops'] != '-'].copy()
    routes_hops['Hops'] = pd.to_numeric(routes_hops['Hops'])
    
    plt.figure(figsize=(10, 6))
    routes_hops['Hops'].hist(bins=range(1, 15))
    plt.title("Route Hops Distribution")
    plt.xlabel("Hops")
    plt.ylabel("Count")
    plt.savefig("route_hops.png")
    print("Saved route_hops.png")

def analyze_pdr(df):
    # Filter for TX and RX events
    tx = df[(df['Type'] == 'TX') & (df['Details'] == 'OK')]
    rx = df[(df['Type'] == 'RX') & (df['Details'] == 'DELIVERED')]
    
    # Count unique packets (Src, Seq)
    # Note: Src is in 'Src' column.
    tx_pkts = tx.groupby(['Src', 'Seq']).size().shape[0]
    rx_pkts = rx.groupby(['Src', 'Seq']).size().shape[0]
    
    print(f"Total TX Packets: {tx_pkts}")
    print(f"Total RX Packets (Delivered): {rx_pkts}")
    if tx_pkts > 0:
        print(f"PDR: {rx_pkts / tx_pkts * 100:.2f}%")

def main():
    args = parse_args()
    
    if not os.path.exists(args.trace_file):
        print(f"File {args.trace_file} not found.")
        return

    # Read CSV with custom separator if needed, assuming comma from Tracer
    try:
        df = pd.read_csv(args.trace_file, sep=',')
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    print("Analyzing Routes...")
    analyze_routes(df)
    
    print("\nAnalyzing PDR...")
    analyze_pdr(df)

if __name__ == "__main__":
    main()
