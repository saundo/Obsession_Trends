#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
non_obsession_exec


"""
import os
import pickle
import pandas as pd
from API_calls import non_obsession_info
from queue import Queue
from threading import Thread


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

def main(pv_data, time_data, timeframe, dump_dir, num):
    
    """
    timeframe needs to cover the timerange of pv_data and time_data
    """
    timeframe = timeframe[::int(len(timeframe) / 8)]
    run_thread(non_obsession_info, timeframe, dump_dir)
    df = read_data(dump_dir)
    df['dupe'] = df['article.id'].duplicated()
    dfz = df[df['dupe'] == False]
    del dfz['dupe']
    del dfz['result']
    
    df_pv = pv_data[pv_data['article.obsessions'] == '']
    df_pv = df_pv.groupby('article.id').sum().reset_index()
    dfz = dfz.merge(df_pv, on='article.id')
    
    df_t = df_t = time_data[time_data['article.obsessions'] == '']
    df_t = df_t.groupby('article.id').sum().reset_index()
    dfz = dfz.merge(df_t, on='article.id')
    
    dfz.columns = ['article.id', 'article.headline.content', 
    'article.content.words.count', 'article.topic', 'page views', 'time (s)']
    
    dfz = dfz[['article.id', 'article.headline.content', 'article.topic', 
    'article.content.words.count', 'page views', 'time (s)']]
    

    storage = {}
    dfzz = dfz[dfz['page views'] >= num]
    thresh_string = ('article count >= ' + str(num) + 'pvs')
    for topic in set(dfzz['article.topic']):
        dft = dfzz[dfzz['article.topic'] == topic]
        storage.setdefault(thresh_string, []).append(dft['article.id'].nunique())
        storage.setdefault('total time (h)', []).append(
                int(dft['time (s)'].sum() / 3600))
        storage.setdefault('page views', []).append(dft['page views'].sum())
        storage.setdefault('topic', []).append(topic)
    df = pd.DataFrame(storage)
    cumul_string = 'avg cumulative time per article (h)'
    df[cumul_string] = df['total time (h)'] / df[thresh_string]
    df[cumul_string] = df[cumul_string].apply(lambda x: int(x))
    df = df.sort_values(cumul_string, ascending=False)
    
    return dfz, df
        
        
        