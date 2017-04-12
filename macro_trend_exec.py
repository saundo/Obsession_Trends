#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:13:18 2017

Executable for macro_trend function
Returns high level trends, t2 / t1
t1 and t2 MUST be dictionaries of the form:
t2 = {'start':'2017-03-03', 'end':'2017-03-05'}

returns 5 objects:
t1pv - keen API return: page views, articles and obsessions, t1
t2pv - keen API return: page views, articles and obsessions, t2
t1time - keen API return: total time, articles and obsessions, t1 
t2time - keen API return: total time, articles and obsessions, t2
tc - sorted DataFrame, merging pvs and time, calculating difference

@author: csaunders
"""

import os
import pickle
import pandas as pd
from standard_imports import time_API
from API_calls import time_spent
from API_calls import articles_obsessions
from queue import Queue
from threading import Thread


def combine_pv_time(df_pv, df_time):
    """returns merged dataframe of page views and times
    """
    df_pv = df_pv.groupby('article.obsessions', sort=False).sum().reset_index()
    df_time = df_time.groupby('article.obsessions', sort=False).sum().reset_index()
    
    df = df_pv.merge(df_time, on='article.obsessions')
    df.columns = ['obsession', 'page views', 'time']
    df = df.sort_values('page views', ascending=False)
    return df

class DownloadWorker1(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            func, start, end, dump_dir = self.queue.get()
            pump_and_dump(func, start, end, dump_dir)
            self.queue.task_done()
                        
def run_thread(func, timeframe, dump_dir):
    """func - the API call to run; 
    timeframe - needs to be a tuple of start, end; 
    dump_dir - where to temp store the data
    """
    queue = Queue()

    for x in range(8):
        worker = DownloadWorker1(queue)
        worker.daemon = True
        worker.start()
    
    for start, end in timeframe:
        queue.put((func, start, end, dump_dir))

    queue.join()

def pump_and_dump(func, start, end, dump_dir):
    """makes a KEEN API call, and then saves the file to the given dump_dir
    """
    
    data = func(start, end)
    
    dump_dir = dump_dir
    ref = start[:16] + '--' + end[:16] + '--' + 'name?'
    file = dump_dir +'/' + ref + '.pickle'

    with open(file, 'wb') as f:
        pickle.dump(data, f)

def read_data(dump_dir):
    """used to collect the files that are dumped by the threading
    """
    
    os.chdir(dump_dir)
    file_list = os.listdir()
    file_list = [i for i in file_list if 'name?' in i]
    
    storage = []
    for file in file_list:
        with open(file, 'rb') as f:
            x1 = pickle.load(f)
        df = pd.DataFrame(x1)
        storage.append(df)
        os.remove(file)
        
    return pd.concat(storage)

def main(t1, t2, dump_dir, API_interval=24):
    """
    """
    time = time_API()
    
    # t2 time setting
    time.start = t2['start']
    time.end = t2['end']
    time.set_time()
    timeframe = time.custom_time(API_interval)
    
    # t2 API calls via threading
    run_thread(articles_obsessions, timeframe, dump_dir) 
    t2_pv_data = read_data(dump_dir).sort_values('result', ascending=False)
    
    run_thread(time_spent, timeframe, dump_dir) 
    t2_time_data = read_data(dump_dir).sort_values('result', ascending=False)
    
    # t1 time setting
    time.start = t1['start']
    time.end = t1['end']
    time.set_time()
    timeframe = time.custom_time(API_interval)
    
    # t1 API calls via threading
    run_thread(articles_obsessions, timeframe, dump_dir) 
    t1_pv_data = read_data(dump_dir).sort_values('result', ascending=False)
    
    run_thread(time_spent, timeframe, dump_dir) 
    t1_time_data = read_data(dump_dir).sort_values('result', ascending=False)
    
    # make tc
    df1_tot = combine_pv_time(t1_pv_data, t1_time_data)
    df2_tot = combine_pv_time(t2_pv_data, t2_time_data)
    tc = df2_tot.merge(df1_tot, on='obsession', how='left')
    
    # clean up columns
    tc.columns=['obsession', 'pv t2', 'time t2', 'pv t1', 'time t1']
    
    # caluculate pv and time changes of t2 / t1; then sort
    tc['pv % change'] = round((tc['pv t2'] / tc['pv t1'] - 1) *100, 1)
    tc['time % change'] = round((tc['time t2'] / tc['time t1'] -1) * 100, 1)
    tc = tc.sort_values('pv t2', ascending=False)
    
    return t1_pv_data, t2_pv_data, t1_time_data, t2_time_data, tc   