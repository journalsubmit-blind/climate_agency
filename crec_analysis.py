#!/usr/bin/env python3

import os
import re
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


CON = sqlite3.connect(os.path.expanduser(
    "~/projects/archives/congressional_record/crec.db"))

CUR = CON.cursor()

MAIN_KEYWORD = r'climate\schange|global\swarming' # Used to reduce full db

QUERY1 = r'\b(climate\schange|global\swarming)\W+(?:\w+\W+){0,150}?(fight(ing)?|(battle|battling)|must act|combat(ing)?|(struggle|struggling)|(oppose|opposing)|fight(ing)?\sback|defend(ing?)|press(ing)?|push(ing)?|campaign(ing)?)|(fight(ing)?|(battle|battling)|must act|combat(ing)?|(struggle|struggling)|(oppose|opposing)|fight(ing)?\sback|defend(ing?)|press(ing)?|push(ing)?|campaign(ing)?)\W+(?:\w+\W+){0,150}?(climate\schange|global\swarming)\b'
QUERY1_DESC = 'Active-agentic framing'

QUERY2 = r'\b(climate\schange|global\swarming)\W+(?:\w+\W+){0,150}?((examine|examining)|study(ing)?|assess(ing)?|model(ing)?|(measure|measuring)|(evaluate|evaluating)|(appraise|appraising))|((examine|examining)|study(ing)?|assess(ing)?|model(ing)?|(measure|measuring)|(evaluate|evaluating)|(appraise|appraising))\W+(?:\w+\W+){0,150}?(climate\schange|global\swarming)\b'
QUERY2_DESC = 'Passive-agentic framing'

QUERY3 = r'\b(climate\schange|global\swarming)\W+(?:\w+\W+){0,150}?(man-made|anthropogenic|human-caused|cause(d|s)?)|(man-made|anthropogenic|human-caused|cause(d)?)\W+(?:\w+\W+){0,150}?(climate\schange|global\swarming)\b'
QUERY3_DESC = 'Agentic ratio/human agency foregrounded/culpability foregrounded'

QUERY4 = r'\b(climate\schange|global\swarming)\W+(?:\w+\W+){0,150}?(nature|natural|cycle|cyclical|slow)|(nature|natural|cycle|cyclical|slow)\W+(?:\w+\W+){0,150}?(climate\schange|global\swarming)\b'
QUERY4_DESC = 'Scenic ratio/nature foregrounded/culpability backgrounded'


def df_main():
    """Pull full database into dataframe"""
    crec_df = pd.read_sql("Select * from crec", CON, index_col='UTC')
    crec_df['html_data'] = crec_df['html_data'].str.replace('\n', ' ')
    crec_df = crec_df.set_index(pd.DatetimeIndex(crec_df.index)).ix[:'2016-12-31']
    #control date range of df
    return crec_df

def df_reduce():
    """Pull database reduced by MAIN_KEYWORD into dataframe"""
    df_reduced = df_main()[df_main()['html_data'].str.contains(MAIN_KEYWORD)]
    return df_reduced

def df_map():
    """Move database to pandas df, reduce by keyword, and map by regex counts."""
    query1_compiled = re.compile(QUERY1, re.IGNORECASE)
    query2_compiled = re.compile(QUERY2, re.IGNORECASE)
    query3_compiled = re.compile(QUERY3, re.IGNORECASE)
    query4_compiled = re.compile(QUERY4, re.IGNORECASE)

    query1_list = []
    query2_list = []
    query3_list = []
    query4_list = []

    for chunk in pd.read_sql("Select * from crec", CON, index_col='UTC',
                             chunksize=1000):
        chunk['html_data'] = chunk['html_data'].str.replace('\n', ' ')
        chunk = chunk.set_index(pd.DatetimeIndex(chunk.index)).ix[:'2017-12-31']
        chunk = chunk[chunk.html_data.str.contains(MAIN_KEYWORD)]

        query1_list.append(pd.DataFrame(index=chunk.index,
                                        data=chunk.html_data.str.count(query1_compiled)))
        query2_list.append(pd.DataFrame(index=chunk.index,
                                        data=chunk.html_data.str.count(query2_compiled)))
        query3_list.append(pd.DataFrame(index=chunk.index,
                                        data=chunk.html_data.str.count(query3_compiled)))
        query4_list.append(pd.DataFrame(index=chunk.index,
                                        data=chunk.html_data.str.count(query4_compiled)))

    query1_df = pd.concat(query1_list)
    query2_df = pd.concat(query2_list)
    query3_df = pd.concat(query3_list)
    query4_df = pd.concat(query4_list)

    return query1_df, query2_df, query3_df, query4_df

def total_ratio():
    """Get ratio of all active-agentic:passive-agentic frames"""
    plot1_data, plot2_data, plot3_data, plot4_data = df_map()

    print(plot1_data.groupby(lambda x:x.year).sum())
    print(plot2_data.groupby(lambda x:x.year).sum())


def plot_frame():
    """Plot dataframes from regex_queries."""
    plot1_data, plot2_data, plot3_data, plot4_data = df_map()

    active_frame = []
    passive_frame = []
    ratio_list_frame = ([])
    for i in plot1_data.groupby(lambda x:x.year).sum().iterrows():
        active_frame.append(i[1].values)
    for i in plot2_data.groupby(lambda x:x.year).sum().iterrows():
        passive_frame.append(i[1].values)
    for ac, pa in zip(active_frame, passive_frame):
        ratio_list_frame.append(ac/pa)
    rounded_ratiolist_frame = [round(float(elem),3) for elem in
            ratio_list_frame]

    active_ratio = []
    passive_ratio = []
    ratio_list_ratio = ([])
    for i in plot3_data.groupby(lambda x:x.year).sum().iterrows():
        active_ratio.append(i[1].values)
    for i in plot4_data.groupby(lambda x:x.year).sum().iterrows():
        passive_ratio.append(i[1].values)
    for ac, pa in zip(active_ratio, passive_ratio):
        ratio_list_ratio.append(ac/pa)
    rounded_ratiolist_ratio = [round(float(elem),3) for elem in
            ratio_list_ratio]

    fig1 = plt.figure(figsize=(13,15))

    ax1=plt.subplot(211)
    ax1.set_xlabel('Year', fontweight='bold')
    ax1.set_ylabel('Frequency', fontweight='bold')
    ax1.set_title('Agentic Frames', fontweight='bold', pad=15)
    plot1 = plt.bar(np.unique(plot1_data.index.year),
            plot1_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#FF5500", width=.4, align='edge')
    plot2 = plt.bar(np.unique(plot2_data.index.year),
            plot2_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#6495ED")
    plt.legend((plot1, plot2), (QUERY1_DESC, QUERY2_DESC), loc=2, fontsize=17)

    def autolabel_frames(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_frame):
            height = rect.get_height()
            ax1.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_frames(plot2)

    ax2=plt.subplot(212)
    ax2.set_xlabel('Year', fontweight='bold')
    ax2.set_ylabel('Frequency', fontweight='bold')
    ax2.set_title('Pentadic Frames/Ratios', fontweight='bold', pad=15)
    plot3 = plt.bar(np.unique(plot3_data.index.year),
            plot3_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#FF5500", width=.4, align='edge')
    plot4 = plt.bar(np.unique(plot4_data.index.year),
            plot4_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#6495ED")
    plt.legend((plot3, plot4), (QUERY3_DESC, QUERY4_DESC), loc=2, fontsize=17)

    def autolabel_ratios(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_ratio):
            height = rect.get_height()
            ax2.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_ratios(plot4)

    fig1.savefig('fig.png')



    fig2 = plt.figure(figsize=(13,15))

    ax1=plt.subplot(211)
    ax1.set_xlabel('Year', fontweight='bold')
    ax1.set_ylabel('Frequency', fontweight='bold')
    ax1.set_title('Agentic Frames', fontweight='bold', pad=15)
    plot1 = plt.bar(np.unique(plot1_data.index.year),
            plot1_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#FF5500", width=.4, align='edge')
    plot2 = plt.bar(np.unique(plot2_data.index.year),
            plot2_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#6495ED", width=.4)
    plt.legend((plot1, plot2), (QUERY1_DESC, QUERY2_DESC), loc=2, fontsize=17)

    def autolabel_frames(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_frame):
            height = rect.get_height()
            ax1.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_frames(plot2)

    ax2=plt.subplot(212)
    ax2.set_xlabel('Year', fontweight='bold')
    ax2.set_ylabel('Frequency', fontweight='bold')
    ax2.set_title('Pentadic Frames/Ratios', fontweight='bold', pad=15)
    plot3 = plt.bar(np.unique(plot3_data.index.year),
            plot3_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#FF5500", width=.4, align='edge')
    plot4 = plt.bar(np.unique(plot4_data.index.year),
            plot4_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#6495ED", width=.4)
    plt.legend((plot3, plot4), (QUERY3_DESC, QUERY4_DESC), loc=2, fontsize=17)

    def autolabel_ratios(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_ratio):
            height = rect.get_height()
            ax2.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_ratios(plot4)

    fig2.savefig('fig2.png')


    fig3 = plt.figure(figsize=(13,15))

    ax1=plt.subplot(211)
    ax1.set_xlabel('Year', fontweight='bold')
    ax1.set_ylabel('Frequency', fontweight='bold')
    ax1.set_title('Agentic Frames', fontweight='bold', pad=15)
    plot1 = plt.bar(np.unique(plot1_data.index.year),
            plot1_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#000000", width=.4, align='edge')
    plot2 = plt.bar(np.unique(plot2_data.index.year),
            plot2_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#a8a8a8", width=.4)
    plt.legend((plot1, plot2), (QUERY1_DESC, QUERY2_DESC), loc=2, fontsize=17)

    def autolabel_frames(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_frame):
            height = rect.get_height()
            ax1.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_frames(plot2)

    ax2=plt.subplot(212)
    ax2.set_xlabel('Year', fontweight='bold')
    ax2.set_ylabel('Frequency', fontweight='bold')
    ax2.set_title('Pentadic Frames/Ratios', fontweight='bold', pad=15)
    plot3 = plt.bar(np.unique(plot3_data.index.year),
            plot3_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.8, color="#000000", width=.4, align='edge')
    plot4 = plt.bar(np.unique(plot4_data.index.year),
            plot4_data['html_data'].groupby(lambda x:x.year).sum(),
            alpha=0.5, color="#a8a8a8")
    plt.legend((plot3, plot4), (QUERY3_DESC, QUERY4_DESC), loc=2, fontsize=17)

    def autolabel_ratios(rects):
        """
        Attach a text label above each bar displaying its height
        """
        for rect,item in zip(rects, rounded_ratiolist_ratio):
            height = rect.get_height()
            ax2.text(rect.get_x() + rect.get_width()/2., 1.50+height,
                    item,
                    ha='center', va='bottom')
    autolabel_ratios(plot4)

    fig3.savefig('fig3.png')
