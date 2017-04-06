#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 08:02:30 2017

@author: csaunders
"""

import pickle
import numpy as np 
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import scipy
import math
import sys
import pdb
from queue import Queue
from threading import Thread
from retrying import retry
from datetime import datetime
from datetime import timedelta
from keen.client import KeenClient

readKey = ("55cc20862508b1fae033656ba4bdb8dd0a0d71fdb6aa973c6f5856847d2e0889"
            "1236c5e79f7f51d4b3dc4d547373180758d666a1b4321e743a2cf0edfe1399f88"
            "2857b0bc3abe566f92f0c7f5fb8eda5e7cf638f8036b31b3574222f58e2f97ac2"
            "33769e271a0d4185f633821565c620")

projectID = '5605844c46f9a7307bca48aa'
keen = KeenClient(project_id=projectID, read_key=readKey)

def dumpin():
    dump_dir = r'/Users/csaunders/google drive/python dumps/incremental'
    return dump_dir

@retry(stop_max_attempt_number=3)
def time_spent(start, end):

    """
    keen API call ......
    """
    
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}
    interval = None
    
    group_by = ('article.obsessions')

    target_property = 'read.time.incremental.seconds'
 
    property_name1 = 'read.type'
    operator1 = 'in'
    property_value1 = (25, 50, 75, 'complete')

    property_name3 = 'read.time.incremental.seconds'
    operator3 = 'gt'
    property_value3 = 0.5

    property_name4 = 'read.time.incremental.seconds'
    operator4 = 'lt'
    property_value4 = 500

    property_name5 = 'article.obsessions'
    operator5 = 'exists'
    property_value5 = True

    filters = [{"property_name":property_name1, "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name3, "operator":operator3,
                "property_value":property_value3},
               {"property_name":property_name4, "operator":operator4, 
               "property_value":property_value4},
               {"property_name":property_name5, "operator":operator5, 
               "property_value":property_value5}]

    t = datetime.now()

    data = keen.sum(event, 
                    timeframe=timeframe, interval=interval,
                    group_by=group_by,
                    target_property=target_property,
                    filters=filters)

    print(start, end, datetime.now() - t)
    
    return data

@retry(stop_max_attempt_number=3)
def articles_obsessions(start, end):
    """
    """
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}
    interval = None

    group_by = ('article.obsessions', 'article.id')

    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    property_name2 = 'article.obsessions'
    operator2 = 'exists'
    property_value2 = True

    filters = [{"property_name":property_name1, "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2,
                "property_value":property_value2}]

    t = datetime.now()

    data = keen.count(event, 
                      timeframe=timeframe, interval=interval,
                      group_by=group_by,
                      filters=filters)

    print(start, end, datetime.now() - t)
    return data

@retry(stop_max_attempt_number=3)
def inc_time_by_article(start, end, article_list):

    """
    keen API call ......
    """
    
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}
    
    group_by = ('article.id', 'article.obsessions',
                'read.time.incremental.seconds', 'read.type')
 
    property_name1 = 'read.type'
    operator1 = 'in'
    property_value1 = (25, 50, 75, 'complete')

    property_name2 = 'article.id'
    operator2 = 'in'
    property_value2 = list(article_list)
    

    filters = [{"property_name":property_name1, "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2,
                "property_value":property_value2}]

    t = datetime.now()

    data = keen.count(event, 
                      timeframe=timeframe,
                      group_by=group_by,
                      filters=filters)

    
    dumpDir = dumpin()
    ref = start[:16] + '--' + end[:16] + '--' + 'name?'

    file = dumpDir +'/' + ref + '.pickle'

    with open(file, 'wb') as f:
        pickle.dump(data, f)

    print(start, datetime.now() - t)

class DownloadWorker1(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
 
            start, end, article_list = self.queue.get()
            inc_time_by_article(start, end, article_list)
            self.queue.task_done()
            
            
def run_thread_inc_time(time_index, article_list):
    queue = Queue()

    for x in range(8):
        worker = DownloadWorker1(queue)
        worker.daemon = True
        worker.start()
    
    for start, end in time_index:
        queue.put((start, end, article_list))

    queue.join()

def combine_pv_time(pv, time):
    df_pv = pd.DataFrame(pv)
    df_pv = df_pv.groupby('article.obsessions', 
                          sort=False).sum().reset_index()

    df_time = pd.DataFrame(time)
    df_time = df_time.groupby('article.obsessions', 
                              sort=False).sum().reset_index()
    
    df = df_pv.merge(df_time, on='article.obsessions')
    df.columns = ['obsession', 'page views', 'time']
    return df

def get_top_articles(data, num=500, obsession=None):
    df = pd.DataFrame(data)
    
    if obsession == None:
        df = df[df['article.obsessions'] == ""]
        df = df.sort_values('result', ascending=False)
        print('% of total', df.iloc[:num]['result'].sum() / df['result'].sum())
        return df['article.id'].iloc[:num].values
    else:
        df = df[df['article.obsessions'] != ""]
        df = df.sort_values('result', ascending=False)
        print('% of total', df.iloc[:num]['result'].sum() / df['result'].sum())
        return df['article.id'].iloc[:num].values

####### MAKE THIS BEGING TO GET USER INFORMATION    
# get time periods of comparison: t2 / t1; end dates are exclusive
t1 = {'start':'2017-03-01', 'end':'2017-03-08'}
t2 = {'start':'2017-03-08', 'end':'2017-03-15'}

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

print('obsession pv change', tc.iloc[1:]['pv t2'].sum() / 
      tc.iloc[1:]['pv t1'].sum())
print('obsession time change', tc.iloc[1:]['time t2'].sum() / 
      tc.iloc[1:]['time t1'].sum())   
    

t2_NO_articles = get_top_articles(t2_pv_data, num=60)
t1_NO_articles = get_top_articles(t1_pv_data, num=60)

t2_obsess_articles = get_top_articles(t2_pv_data, num=100, obsession=True)
t1_obsess_articles = get_top_articles(t1_pv_data, num=100, obsession=True)

article_lists = {'t1': {'NO':t1_NO_articles, 'obsess': t1_obsess_articles},
                 't2': {'NO':t2_NO_articles, 'obsess': t2_obsess_articles},
                }

# get daily timeframes for timerange t1 through t2
t2start = [n for n, (i, j) in enumerate(time.timeframe) if t2['start'] in i][0] 
t2end = [n for n, (i, j) in enumerate(time.timeframe) if t2['end'] in j][0]
t2_time_index = time.timeframe[t2start:t2end+1]

t1end = [n for n, (i, j) in enumerate(time.timeframe) if t1['end'] in j][0]
t1_time_index = time.timeframe[:t1end+1]

time_indices = {'t1':t1_time_index, 't2': t2_time_index}