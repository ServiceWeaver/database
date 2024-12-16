
#!/usr/bin/env python3 
import json
import matplotlib.pyplot as plt
import numpy as np

class Benchmark:
    def __init__(self, data):
        self.workload = data["Workload"]
        self.plain_mean = convert_to_ms(data["Plain"]["Mean"])
        self.plain_variance = convert_to_ms(data["Plain"]["Std"])  # Calculate variance from Std
        self.branch_mean =convert_to_ms(data["Branch"]["Mean"])
        self.branch_variance = convert_to_ms(data["Branch"]["Std"]) # Calculate variance from Std


def readFromFile(fn):
    with open(fn, "r") as f:
        data = json.load(f)
    return [Benchmark(item) for item in data]

def convert_to_ms(time_str):
    if time_str.endswith('ms'):
      milliseconds = float(time_str[:-2])  
    elif time_str.endswith('s'):
      seconds = float(time_str[:-1]) 
      milliseconds = seconds * 1000
    return milliseconds


def plot(benchmarks):
    # Numbers of pairs of bars you want
    N = len(benchmarks)

    # Data on X-axis

    blue_bar = []
    # Specify the values of orange bars (height)
    orange_bar =  []

    # Error bar values
    blue_err = []
    orange_err = []
    titles = []
    for benchmark in benchmarks:
        blue_bar.append(benchmark.plain_mean)
        blue_err.append(benchmark.plain_variance)
        orange_bar.append(benchmark.branch_mean)
        orange_err.append(benchmark.branch_variance)
        titles.append(benchmark.workload)

    # Position of bars on x-axis
    ind = np.arange(N)

    # Figure size
    plt.figure(figsize=(10,8))

    plt.rcParams['lines.linewidth'] = 8
    plt.tick_params(axis='both', which='major', labelsize=15)

    # Width of a bar 
    width = 0.3       

    # Plotting
    plt.bar(ind, blue_bar, width, yerr=blue_err, label='Postgres', capsize=5)
    plt.bar(ind + width, orange_bar, width, yerr=orange_err, label='R$^+$R$^-$', capsize=5)

    plt.xlabel('Core Workload',fontsize=15)
    plt.ylabel('Latency(ms)',fontsize=15)
    plt.title('YCSB Benchmark 1M rows')

    # xticks()
    # First argument - A list of positions at which ticks should be placed
    # Second argument -  A list of labels to place at the given locations
    plt.xticks(ind + width / 2, titles, fontsize=15)  # Corrected xtick labels
    plt.yticks(fontsize=15)

    # Finding the best position for legends and putting it
    plt.legend(loc='best')

    plt.savefig('ycsb.pdf', bbox_inches="tight")

def main():
    benchmark = readFromFile("/usr/local/google/home/zhukexin/database/ycsb_benchmark/ycsb_metrcis_1M.json")
    print(benchmark)
    plot(benchmark)


if __name__ == "__main__":
    main()
