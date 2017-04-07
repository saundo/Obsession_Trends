#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 08:55:10 2017
Executable for engaged_time function
Returns the 'average engaged_time' for an article; which is calculated
    as the sum of IQR time for each article quartile divided by the count
@author: csaunders
"""

import pickle
import numpy as np
import pandas as pd
import os
from queue import Queue
from threading import Thread
from API_calls import inc_time_by_article

def pump_and_dump(start, end, article_list, dump_dir):
    """makes a KEEN API call, and then saves the file to the given dump_dir
    """
    
    data = inc_time_by_article(start, end, article_list)
    
    dump_dir = dump_dir
    ref = start[:16] + '--' + end[:16] + '--' + 'name?'
    file = dump_dir +'/' + ref + '.pickle'

    with open(file, 'wb') as f:
        pickle.dump(data, f)
        
class DownloadWorker1(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            start, end, article_list, dump_dir = self.queue.get()
            pump_and_dump(start, end, article_list, dump_dir)
            self.queue.task_done()
            
            
def run_thread(article_list, timeframe, dump_dir):
    """timeframe needs to be a tuple of start, end
    """
    queue = Queue()

    for x in range(8):
        worker = DownloadWorker1(queue)
        worker.daemon = True
        worker.start()
    
    for start, end in timeframe:
        queue.put((start, end, article_list, dump_dir))

    queue.join()
    
def engaged_time_calc(df, article_id, low, high):
    storage = []
    df = df[df['article.id'] == article_id]
    
    for i in (25, 50, 75, 'complete'):
        dft = df[df["read.type"] == i]
        dft = dft[(dft['read.time.incremental.seconds'] > low) & (dft['read.time.incremental.seconds'] < high)]
        dftt = np.repeat(dft['read.time.incremental.seconds'], dft['result'])
        q25 = dftt.quantile(.25)
        q75 = dftt.quantile(.75)
        dft = dft[(dft['read.time.incremental.seconds'] >= q25) & (dft['read.time.incremental.seconds'] <=q75)]
        total = (dft['read.time.incremental.seconds'] * dft['result']).sum()
        storage.append(total / dft['result'].sum())
        
    return storage

def main(article_list, timeframe, dump_dir):
    print('running API calls')
    run_thread(article_list, timeframe, dump_dir)
    
    os.chdir(dump_dir)
    file_list = os.listdir()
    file_list = [i for i in file_list if 'name?' in i]
        
    print('compiling files from API calls together')
    storage = []
    for file in file_list:
        with open(file, 'rb') as f:
            x1 = pickle.load(f)
        df = pd.DataFrame(x1)
        storage.append(df)
        os.remove(file)
        
    df = pd.concat(storage)
    
    print('calculating engaged time per article')
    storage = {}
    for article in set(df['article.id']):
        x1 = engaged_time_calc(df, article, 0.5, 500)
        storage.setdefault('article.id', []).append(article)
        storage.setdefault('avg engaged time (s)', []).append(int(sum(x1)))
        print(article, 'complete')
    
    return pd.DataFrame(storage)