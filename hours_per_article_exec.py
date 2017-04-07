#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calculates average time spent per article for a given obsession, with a min 
page view threshold (num) to include
Formula - for a given obsession sum of total time spent for all articles, 
divided by number of articles that comprise of that obsession
"""

import pandas as pd

def scrub_blank(val):    
    """replaces the "" for no obsession with the value contained below
    """
    if val == "":
        return "## NO OBSESSION ##"
    else:
        return val
def main(pv_data, time_data, num=500):
    """primary function: pv_data and time_data should be from same time 
    interval; 
    num refers to threshold of pageviews; so default num=500 means that only
    articles with a minimum of 500 page views will be considered
    returns- sorted DataFrame
    """
    
    df = pd.DataFrame(pv_data)
    df1 = df[df['result'] >= num]
    
    df2 = pd.DataFrame(time_data)
    df3 = df1.merge(df2, on='article.id')
    df3 = df3.applymap(scrub_blank)
    
    storage = {}
    for article in set(df3['article.id']):
        dft = df3[df3['article.id'] == article]
        dft = dft.sort_values('result_y', ascending=False)
        max_obsess = dft.iloc[0,3]
        dft = dft[(dft.iloc[:,1] == max_obsess) & (dft.iloc[:,3] == max_obsess)]
        storage.setdefault('article.id', []).append(article)
        storage.setdefault('obsession', []).append(max_obsess)
        storage.setdefault('total time spent', []).append(int(dft.iloc[:,4].values))
    
    df = pd.DataFrame(storage)
    storage = {}    
    for obsession in set(df['obsession']):
        dft = df[df['obsession'] == obsession]
        x1 = (dft['total time spent'].sum() / len(dft)) / 3600
        storage.setdefault('avg hours per article', []).append(int(x1))
        storage.setdefault('obsession', []).append(obsession)
    
    return pd.DataFrame(storage).sort_values('avg hours per article', 
                       ascending=False)