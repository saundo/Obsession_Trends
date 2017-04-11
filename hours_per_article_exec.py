#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Calculates average time spent per article for a given obsession, with a min 
page view threshold (num) to include
Formula - for a given obsession sum of total time spent for all articles, 
divided by number of articles that comprise of that obsession
"""

import numpy as np
import pandas as pd

def scrub_blank(val):    
    """replaces the "" for no obsession with the value contained below
    """
    if val == "":
        return "## NO OBSESSION ##"
    else:
        return val
    
def unique_obsessions(dfpv, dftime, num = 500):
    """
    """
    dfpv = dfpv.groupby('article.id').sum().reset_index()
    dfpv = dfpv[dfpv['result'] >= num]
    dftime = dftime.groupby('article.id').sum().reset_index()
    df = dfpv.merge(dftime, on='article.id')
    
    #remove articles that don't make the cut; IN says that the articles are in the article_list
    article_list = set(df['article.id'])
    dfpv['in'] = dfpv['article.id'].apply(lambda x: x in article_list)
    
    #do a first cut of removing duplicates
    dft = dfpv[dfpv['in'] == True]
    dft = dft[['article.id', 'article.obsessions']].drop_duplicates()
    
    #find the remaining duplicates
    dftt = dft.groupby('article.id').count().reset_index()
    remaining_dupes = set(dftt[dftt['article.obsessions'] > 1]['article.id'])
    
    dft['dupe'] = dft['article.id'].apply(lambda x: x in remaining_dupes)
    dft_unique = dft[dft['dupe'] != True]
    dft_dupe = dft[dft['dupe'] == True].copy()
    
    obsess_storage = {}
    tracker = len(set(dft_dupe['article.id']))
    for i, article in enumerate(set(dft_dupe['article.id'])):
        x1 = dfpv[dfpv['article.id'] == article]
        obsess_storage.setdefault('article.obsessions', []).append(x1['article.obsessions'].values[0])
        obsess_storage.setdefault('article.id', []).append(article)
        if int(i / tracker * 100) % 10 == 0:
            print(int(i / tracker * 100), '% ', end='')        
        
    df_FINAL = dft_unique[['article.id', 'article.obsessions']].append(pd.DataFrame(obsess_storage))
    return df_FINAL


def main(dfpv, dftime, num=500):
    """primary function: pv_data and time_data should be from same time 
    interval; 
    num refers to threshold of pageviews; so default num=500 means that only
    articles with a minimum of 500 page views will be considered
    returns- sorted DataFrame
    """
    
    dfpv = dfpv.groupby('article.id').sum().reset_index()
    dfpv = dfpv[dfpv['result'] >= num]
    dftime = dftime.groupby('article.id').sum().reset_index()
    df = dfpv.merge(dftime, on='article.id')
    df.columns = ['article.id', 'page views', 'time']
    df = df[['article.id', 'page views', 'time']]
    
    df_unique_obsess = unique_obsessions(dfpv, dftime, num=num)
    df = df.merge(df_unique_obsess, on='article.id')
    df = df.sort_values('page views', ascending=False)
    
    storage = {}
    for obsession in set(df['article.obsessions']):
        dft = df[df['article.obsessions'] == obsession]
        storage.setdefault('total time (h)', []).append(int(dft['time'].sum() / 3600))
        storage.setdefault('unique articles > 500 pv', []).append(dft['article.id'].nunique())
        storage.setdefault('obsession', []).append(obsession)
    
    df_obsess_totals = pd.DataFrame(storage).sort_values('total time (h)', ascending=False)
    df_obsess_totals['avg time per article (m)'] = (df_obsess_totals['total time (h)'] / df_obsess_totals['unique articles > 500 pv']) / 60
    
    df_obsess_totals = df_obsess_totals.sort_values('avg time per article (m)', ascending=False)
    df_obsess_totals.index = np.arange(len(df_obsess_totals))
    
    return df_obsess_totals