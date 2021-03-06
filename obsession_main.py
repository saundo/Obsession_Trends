#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 08:02:30 2017

@author: csaunders
"""

import macro_trend_exec
import hours_per_article_exec
import engaged_time_exec
import non_obsession_exec

# get time periods of comparison: t2 / t1; end dates are exclusive
t1 = {'start':'2017-03-01', 'end':'2017-03-03'}
t2 = {'start':'2017-03-03', 'end':'2017-03-05'}

def macro_trend(t1, t2, dump_dir, API_interval=24):
    """returns high level trends, t2 / t1
    t1 and t2 MUST be dictionaries of the form:
    t2 = {'start':'2017-03-03', 'end':'2017-03-05'}
    
    returns 5 objects:
    pv1 - DataFrame keen API return: page views, articles and obsessions, t1
    pv2 - DataFrame keen API return: page views, articles and obsessions, t2
    t1 - DataFrame keen API return: total time, articles and obsessions, t1 
    t2 - DataFrame keen API return: total time, articles and obsessions, t2
    tc - sorted DataFrame, merging pvs and time, calculating difference
    """
    pv1, pv2, t1, t2, tc = macro_trend_exec.main(t1, t2, dump_dir, 
                                                 API_interval=API_interval)

    print('obsession pv change', tc.iloc[1:]['pv t2'].sum() / 
          tc.iloc[1:]['pv t1'].sum())
    print('obsession time change', tc.iloc[1:]['time t2'].sum() / 
      tc.iloc[1:]['time t1'].sum())
    
    return pv1, pv2, t1, t2, tc

def hours_per_article(pv_data, time_data, num=500):
    """num is the minimum number of page views to include in this analysis
    """
    df = hours_per_article_exec.main(pv_data, time_data, num=num)
    return df

def engaged_time(article_list, timeframe, dump_dir):
    """returns the 'average engaged_time' for an article; which is calculated
    as the sum of IQR time for each article quartile divided by the count
    """
    df_time = engaged_time_exec.main(article_list, timeframe, dump_dir)
    return df_time

def non_obsession(pv_data, time_data, timeframe, dump_dir, num=500):
    """
    
    returns:
    df1 = article level dataframe
    df2 - avg cumulative time per article by topic
    """
    df1, df2 = non_obsession_exec.main(pv_data, time_data, 
                                       timeframe, dump_dir, num)
    return df1, df2

    