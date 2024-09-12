#!/usr/bin/env python3 
import json
import matplotlib.pyplot as plt
import numpy as np
import re
import argparse

database = []

types = ['RPlusRMinus']
colors = ['blue', 'green', 'red','orange']
fmts = ['o','s','v','^']


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


def extract_number(text):
  match = re.search(r'\d+', text)
  if match:
    return int(match.group())
  else:
    return 0

def plotBranchStats(data,table_name,dir,is_analysis):
    database_sizes=[[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Branch']['Time'][t]))
            yerr_values[i].append(0) # we do not run multiple times for branch
            # if is_analysis:
            #     database_sizes[i].append(extractTablesize(data[db]["users"]['DbSizeIncrease']["RPlusRMinus"]))
            # else:
            #     database_sizes[i].append(extractTablesize(data[db]["users"]['DbSizeIncrease']["Postgres"]))
            database_sizes[i].append(str(data[db][table_name]['Rows']*2))

    if "pk" in table_name:
        title='Branch latency(Primary Key)'
    else:
        title='Branch latency'

    if is_analysis:
        plotForBranch(database_sizes,stats,yerr_values,colors,types,title,dir,True,False)
    else:
        plotForBranch(database_sizes,stats,yerr_values,colors,types,title,dir,True,True)

def plotDiffStats(data,table_name,dir):
    table_sizes =  [[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Diffs'][2]['Time'][t]))
            yerr_values[i].append(0) # we do not run multiple times for diffs
            table_sizes[i].append(str(data[db][table_name]['Rows']))
            # table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))
    if "pk" in table_name:
        title='Diff latency(Primary Key)'
    else:
        title='Diff latency'
    plot(table_sizes,stats,yerr_values,colors,types,title, dir, True, True)

def plotWriteStats(data,table_name,dir,is_analysis):
    table_sizes =[[] for _ in range(len(types))] 
    stats = [[] for _ in range(len(types))] 
    yerr_values = [[] for _ in range(len(types))] 
    for db in database:
        for i,t in enumerate(types):
            stats[i].append(convertNstoMs(data[db][table_name]['Writes'][2]['Time'][t]['Mean']))
            yerr_values[i].append(convertNstoMs(data[db][table_name]['Writes'][2]['Time'][t]['Std']))
            table_sizes[i].append(str(data[db][table_name]['Rows']))
            # table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))

    if "pk" in table_name:
        title='Write latency(Primary Key)'
    else:
        title='Write latency'
    if is_analysis:
        plot(table_sizes,stats,yerr_values,colors,types,title,dir,True,False)
    else:
        plot(table_sizes,stats,yerr_values,colors,types,title,dir,True,False)

def plotReadStats(data,table_name,dir,is_analysis):
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
                all_table_sizes[j][i].append(str(data[db][table_name]['Rows']))
                # all_table_sizes[j][i].append(extractTablesize(data[db][table_name]['TableSize']))
    
    for i in range(3):
        if "pk" in table_name:
            title='Read latency(Primary Key): '+queries[i]
        else:
            title='Read latency: '+queries[i]
        if is_analysis:
            plot(all_table_sizes[i],all_stats[i],all_yerr_values[i],colors,types,title,dir,True,False)
        else:
            plot(all_table_sizes[i],all_stats[i],all_yerr_values[i],colors,types,title,dir,True,False)


def plotStats(data,dir,is_analysis):
    global types
    if is_analysis:
        types = ['RPlusRMinus']
    else:
        types = ['RPlusRMinus','Postgres', 'Dolt']

    # branch whole database instead of one table
    plotBranchStats(data,table,dir,is_analysis)

    # # diffing
    if is_analysis:    
        getMultipleDiff(data,table,dir)
        getMultipleDiff(data,table_pk,dir)
    else:
        plotDiffStats(data,table,dir)
        plotDiffStats(data,table_pk,dir)


    # write
    plotWriteStats(data,table,dir,is_analysis)
    plotWriteStats(data,table_pk,dir,is_analysis)

    # read
    plotReadStats(data,table,dir,is_analysis)
    plotReadStats(data,table_pk,dir,is_analysis)


def plot(x_values, y_values, yerr_values, colors,line_names,title_name,dir,x_log_scale=False, y_log_scale=False):
    line_names = ['R$^+$R$^-$' if n == 'RPlusRMinus' else n for n in line_names]

    plt.figure(figsize=(10, 8))

    plt.rcParams['lines.linewidth'] = 8
    plt.tick_params(axis='both', which='major', labelsize=40)

    for i in range(len(x_values)):
        x = x_values[i]
        y = y_values[i]
        yerr = yerr_values[i]
        color = colors[i]
        plt.errorbar([float(a) for a in x], y, yerr=yerr, fmt=fmts[i], capsize=5, linestyle='-', color=color, label=line_names[i],ms=35)
    if y_log_scale:
        plt.yscale('log')

    if x_log_scale:
        plt.xscale('log')
    else:
        plt.xscale('linear')

    plt.xlabel('Table Rows',fontsize=40)
    plt.ylabel('Latency(ms)',fontsize=40)
    plt.title('')
    if len(line_names) > 1:
        # if line_names == ["1","100","1000","10000"]:
        #     plt.legend(prop={'size': 40}, loc='best')
        # else:
        plt.legend(prop={'size': 35},bbox_to_anchor=(0.395, 1.16), loc='upper center', ncol=len(line_names))
        # plt.legend(prop={'size': 40}, loc='upper left', mode='expand')

        # leg = plt.legend(prop={'size': 33}, loc='upper left',)

        # # Anchor the legend to the upper left corner
        # leg.set_bbox_to_anchor((0, 1))

        # fig = plt.gcf()  # Get the current figure
        # ax = plt.gca()   # Get the current axes

        # handles, labels = ax.get_legend_handles_labels()

        # renderer = fig.canvas.get_renderer()
        # bbox = leg.get_window_extent(renderer)
        # axes_bbox = ax.get_window_extent(renderer)

        # if bbox.overlaps(axes_bbox):
        #     ymin, ymax = ax.get_ylim()
        #     overlap = bbox.y1 - axes_bbox.y0
        #     new_ymax = ymax + overlap
        #     ax.set_ylim(ymin, new_ymax)


    plt.yticks(fontsize=40)
    plt.xticks(fontsize=40)

    plt.grid(axis='y', alpha=0.5)
    plt.savefig(dir+'/'+title_name+'.pdf', bbox_inches="tight")


def plotForBranch(x_values, y_values, yerr_values, colors,line_names,title_name,dir,x_log_scale=False, y_log_scale=False):
    line_names = ['R$^+$R$^-$' if n == 'RPlusRMinus' else n for n in line_names]

    plt.figure(figsize=(10, 8))

    plt.rcParams['lines.linewidth'] = 8
    plt.tick_params(axis='both', which='major', labelsize=40)

    for i in range(len(x_values)):
        x = x_values[i]
        y = y_values[i]
        yerr = yerr_values[i]
        color = colors[i]
        plt.errorbar([float(a) for a in x], y, yerr=yerr, fmt=fmts[i], capsize=5, linestyle='-', color=color, label=line_names[i],ms=35)

    if y_log_scale:
        plt.yscale('log')
    
    if x_log_scale:
        plt.xscale('log')
    else: 
        plt.xscale('linear')
    if len(line_names) > 1:
        plt.legend(prop={'size': 40},bbox_to_anchor=(1, 1), loc='upper left')
    plt.xlabel('Database Rows',fontsize=40)
    plt.ylabel('Latency(ms)',fontsize=40)

    plt.yticks(fontsize=40)
    plt.xticks(fontsize=40)

    plt.title('')
    plt.grid(axis='y', alpha=0.5)

    plt.savefig(dir+'/'+title_name+'.pdf', bbox_inches="tight")

def getMultipleDiff(data,table_name,dir):
    diffTypes = ["1","100","1000","10000"] 
    table_sizes =  [[] for _ in range(len(diffTypes))] 
    stats = [[] for _ in range(len(diffTypes))] 
    yerr_values = [[] for _ in range(len(diffTypes))] 
    for db in database:
        for i,t in enumerate(diffTypes):
            stats[i].append(convertNstoMs(data[db][table_name]['Diffs'][i]['Time']['RPlusRMinus']))
            yerr_values[i].append(0) # we do not run multiple times for diffs
            table_sizes[i].append(str(data[db][table_name]['Rows']))
            # table_sizes[i].append(extractTablesize(data[db][table_name]['TableSize']))
    if "pk" in table_name:
        title='Diff latency(Primary Key)'
    else:
        title='Diff latency'
    plot(table_sizes, stats, yerr_values,colors,diffTypes,title,dir,True,False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--is_analysis", help="Whether to plot analysis plot")
    args = parser.parse_args()
    global database

    if args.is_analysis:
        data = readFromFile("dump/metrics_2.json")
        databaseSize = data.keys()
        database = sorted(databaseSize,key=extract_number)
        plotStats(data,'analysis_plot',True)
    else:
        data = readFromFile("dump/metrics_5stat.json")
        databaseSize = data.keys()
        database = sorted(databaseSize,key=extract_number)
        plotStats(data,'plot', False)


if __name__ == "__main__":
    main()
