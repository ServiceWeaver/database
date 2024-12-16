
#!/usr/bin/env python3 
import json
import matplotlib.pyplot as plt
import numpy as np

class Benchmark:
    def __init__(self, data):
        self.workload = data["Workload"][-1].upper()
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

    branch_bar = []
    plain_bar =  []

    branch_err = []
    plain_err = []
    titles = []
    for benchmark in benchmarks:
        plain_bar.append(benchmark.plain_mean)
        plain_err.append(benchmark.plain_variance)
        branch_bar.append(benchmark.branch_mean)
        branch_err.append(benchmark.branch_variance)
        titles.append(benchmark.workload)

    # Position of bars on x-axis
    ind = np.arange(N)

    # Figure size
    plt.figure(figsize=(10,8))

    plt.rcParams['lines.linewidth'] = 8
    plt.tick_params(axis='both', which='major', labelsize=22)

    # Width of a bar 
    width = 0.3       

    # Plotting
    plt.bar(ind, plain_bar, width, yerr=plain_err, label='Postgres', capsize=22, color='green')
    plt.bar(ind + width, branch_bar, width, yerr=branch_err, label='R$^+$R$^-$', capsize=22, color='blue')

    plt.xlabel('Core Workload',fontsize=22)
    plt.ylabel('Latency(ms)',fontsize=22)
    plt.title('YCSB Benchmark 1M Rows',fontsize=22)

    plt.xticks(ind + width / 2, titles, fontsize=22)
    plt.yticks(fontsize=22)

    # Finding the best position for legends and putting it
    plt.legend(loc='best',prop={'size': 22})

    plt.savefig('ycsb.pdf', bbox_inches="tight")

def main():
    benchmark = readFromFile("/usr/local/google/home/zhukexin/database/ycsb_benchmark/ycsb_metrcis_1M.json")
    plot(benchmark)


if __name__ == "__main__":
    main()
