#!/usr/bin/env python3 
import json
import matplotlib.pyplot as plt
import numpy as np


database = ['benchmark_1mb','benchmark_20mb','benchmark_100mb']
types = ['RPlusRMinus','Postgres', 'Dolt']
colors = ['blue', 'green', 'red']
table = 'users'
table_pk = 'users_pk'


def convertNstoMs(t):
    return t/1000000.0

def extractTablesize(text):
    number = float(text.split()[0])
    if number is not None:
        return f"{number:.2f}"
    else:
        return 0

def readFromFile(fn):
    with open(fn, "r") as f:
        data = json.load(f)
    return data


def plotBranchStats(data,table_name):
    table_sizes =  [[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Branch']['Time'][t]))
            yerr_values[i].append(0) # we do not run multiple times for branch
            table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))

    if "pk" in table_name:
        title='Branch latency(Primary Key)'
    else:
        title='Branch latency'
    plot(table_sizes,stats,yerr_values,colors,types,title )

def plotDiffStats(data,table_name):
    table_sizes =  [[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Diffs'][2]['Time'][t]))
            yerr_values[i].append(0) # we do not run multiple times for diffs
            table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))

    if "pk" in table_name:
        title='Diff latency(Primary Key)'
    else:
        title='Diff latency'
    plot(table_sizes,stats,yerr_values,colors,types,title)

def plotWriteStats(data,table_name):
    table_sizes =[[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Writes'][2]['Time'][t]['Mean']))
            yerr_values[i].append(convertNstoMs(data[db][table_name]['Writes'][2]['Time'][t]['Std']))
            table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))

    if "pk" in table_name:
        title='Write latency(Primary Key)'
    else:
        title='Write latency'
    plot(table_sizes,stats,yerr_values,colors,types,title )

def plotReadStats(data,table_name):
    all_table_sizes = [[[] for _ in range(len(types))] for _ in range(4)]
    all_stats = [[[] for _ in range(len(types))] for _ in range(4)]
    all_yerr_values = [[[] for _ in range(len(types))] for _ in range(4)]
    queries = []
    for db in database:
        info =data[db][table_name]['Reads'][0]
        if len(queries)==0:
            queries=list(info.keys())
        queries = sorted(queries)
        for j,r in enumerate(queries):
            for i,t in enumerate(types):
                all_stats[j][i].append(convertNstoMs(info[r]['Time'][t]['Mean']))
                all_yerr_values[j][i].append(convertNstoMs(info[r]['Time'][t]['Std']))
                all_table_sizes[j][i].append(extractTablesize(data[db][table_name]['TableSize']))
    
    for i in range(4):
        if "pk" in table_name:
            title='Read latency(Primary Key): '+queries[i]
        else:
            title='Read latency: '+queries[i]
        plot(all_table_sizes[i],all_stats[i],all_yerr_values[i],colors,types,title)


def plotStats(data):
    # branch whole database instead of one table
    plotBranchStats(data,table)

    # diffing
    plotDiffStats(data,table)
    plotDiffStats(data,table_pk)


    # write
    plotWriteStats(data,table)
    plotWriteStats(data,table_pk)

    # read
    plotReadStats(data,table)
    plotReadStats(data,table_pk)

def plot(x_values, y_values, yerr_values, colors,line_names,title_name):
    plt.figure(figsize=(10, 6))
    for i in range(len(x_values)):
        x = x_values[i]
        y = y_values[i]
        yerr = yerr_values[i]
        color = colors[i]
        plt.errorbar(x, y, yerr=yerr, fmt='o', capsize=5, linestyle='-', color=color, label=line_names[i])

    plt.xlabel('Table Size(MB)')
    plt.ylabel('Latency(ms)')
    plt.title(title_name)
    plt.legend() 

    plt.grid(axis='y', alpha=0.5)

    plt.savefig('dump/'+title_name+'.png')


def main():
    data = readFromFile("dump/metrics.json")
    plotStats(data)

if __name__ == "__main__":
    main()
