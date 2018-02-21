#!/usr/bin/env python3

import os
import re
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


CON = sqlite3.connect(os.path.expanduser(
    "~/projects/archives/congressional_record/crec.db"))

CUR = CON.cursor()

MAIN_KEYWORD = r'climate\schange|global\swarming' # Used to reduce full db

QUERY1 = r'\bclimate\schange\W+(?:\w+\W+){0,150}?(fight(ing)?|(battle|battling)|must act|combat(ing)?|(struggle|struggling)|(oppose|opposing)|fight(ing)?\sback|defend(ing?)|press(ing)?|push(ing)?|campaign(ing)?)|(fight(ing)?|(battle|battling)|must act|combat(ing)?|(struggle|struggling)|(oppose|opposing)|fight(ing)?\sback|defend(ing?)|press(ing)?|push(ing)?|campaign(ing)?)\W+(?:\w+\W+){0,150}?climate\schange\b'
QUERY1_DESC = 'Climate Change collocated with combat terms'

QUERY2 = r'\bclimate\schange\W+(?:\w+\W+){0,150}?((examine|examining)|study(ing)?|assess(ing)?|model(ing)?|(measure|measuring)|(evaluate|evaluating)|(appraise|appraising))|((examine|examining)|study(ing)?|assess(ing)?|model(ing)?|(measure|measuring)|(evaluate|evaluating)|(appraise|appraising))\W+(?:\w+\W+){0,150}?climate\schange\b'
QUERY2_DESC = 'Climate Change collocated with assessment terms'

QUERY3 = r'\bclimate\schange\W+(?:\w+\W+){0,150}?(man-made|anthropogenic|human-caused|cause(d|s)?)|(man-made|anthropogenic|human-caused|cause(d)?)\W+(?:\w+\W+){0,150}?climate\schange\b'
QUERY3_DESC = 'Agentic Ratio/Human Agency Foregrounded/Culpability Foregrounded'

QUERY4 = r'\bclimate\schange\W+(?:\w+\W+){0,150}?(nature|natural|cycle|cyclical|slow)|(nature|natural|cycle|cyclical|slow)\W+(?:\w+\W+){0,150}?climate\schange\b'
QUERY4_DESC = 'Scenic Ratio/Nature Foregrounded/Culpability Backgrounded'


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
        chunk = chunk.set_index(pd.DatetimeIndex(chunk.index)).ix[:'2016-12-31']
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

def summary():
    """Print table with total main keyword numbers."""
    print(df_map().html_data.str.contains(MAIN_KEYWORD, re.IGNORECASE).groupby(lambda
        x: x.year).sum())


def plot_it():
    """Plot dataframes from regex_queries."""
    plot1_data, plot2_data, plot3_data, plot4_data = df_map()

    fig1 = plt.figure(figsize=(15, 13))

    ax = fig1.add_subplot(211)
    ax.set_xlabel('Year', fontweight='bold')
    ax.set_ylabel('Frequency', fontweight='bold')
    ax.set_title('Agentic Frames', fontweight='bold')
    plt.plot(plot1_data.groupby(lambda x: x.year).sum())
    plt.plot(plot2_data.groupby(lambda x: x.year).sum())
    plt.legend((QUERY1_DESC, QUERY2_DESC), loc=2, fontsize=15)

    ax = fig1.add_subplot(212)
    ax.set_xlabel('Year', fontweight='bold')
    ax.set_ylabel('Frequency', fontweight='bold')
    ax.set_title('Pentadic Frames/Ratios', fontweight='bold')
    plt.plot(plot3_data.groupby(lambda x: x.year).sum())
    plt.plot(plot4_data.groupby(lambda x: x.year).sum())
    plt.legend((QUERY3_DESC, QUERY4_DESC), loc=2, fontsize=15)

    fig1.savefig('fig.png')
