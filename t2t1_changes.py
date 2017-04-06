#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:13:18 2017

@author: csaunders
"""
import pandas as pd
from standard_imports import time_API
from API_calls import time_spent
from API_calls import articles_obsessions

def combine_pv_time(pv, time):
    df_pv = pd.DataFrame(pv)
    df_pv = df_pv.groupby('article.obsessions', sort=False).sum().reset_index()

    df_time = pd.DataFrame(time)
    df_time = df_time.groupby('article.obsessions', sort=False).sum().reset_index()
    
    df = df_pv.merge(df_time, on='article.obsessions')
    df.columns = ['obsession', 'page views', 'time']
    return df


def main(t1, t2):
    time = time_API()
    # get high level page views for t2
    time.start = t2['start']
    time.end = t2['end']
    start, end = time.get_time()
    t2_pv_data = articles_obsessions(start, end)
    t2_time_data = time_spent(start, end)
    
    # get high level page views for t1
    time.start = t1['start']
    time.end = t1['end']
    start, end = time.get_time()
    t1_pv_data = articles_obsessions(start, end)
    t1_time_data = time_spent(start, end)
    
    
    t2_pv_time = combine_pv_time(t2_pv_data, t2_time_data)
    t1_pv_time = combine_pv_time(t1_pv_data, t1_time_data)
    tc = t2_pv_time.merge(t1_pv_time, on='obsession', how='left')
    tc.columns=['obsession', 'pv t2', 'time t2', 'pv t1', 'time t1']
    
    tc['pv % change'] = round((tc['pv t2'] / tc['pv t1'] - 1) *100, 1)
    tc['time % change'] = round((tc['time t2'] / tc['time t1'] -1) * 100, 1)
    tc = tc.sort_values('pv t2', ascending=False)
    return t1_pv_data, t2_pv_data, t1_time_data, t2_time_data, tc    