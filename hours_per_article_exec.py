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
        if val == "":
            return "ObsessionLess - SAD!"
        else:
            return val
def main(pv_data, time_data, num=500):
    """
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