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



from API_calls import dumpin
from API_calls import time_spent
from API_calls import articles_obsessions

import t2t1_changes
import hours_article_obsession

# get time periods of comparison: t2 / t1; end dates are exclusive
t1 = {'start':'2017-03-01', 'end':'2017-03-03'}
t2 = {'start':'2017-03-03', 'end':'2017-03-05'}

def macro_trend(t1, t2):
    """returns high level trends, t2 / t1
    t1 and t2 MUST be dictionaries of the form:
    t2 = {'start':'2017-03-03', 'end':'2017-03-05'}
    
    returns 5 objects:
    t1pv - keen API return: page views, articles and obsessions, t1
    t2pv - keen API return: page views, articles and obsessions, t2
    t1time - keen API return: total time, articles and obsessions, t1 
    t2time - keen API return: total time, articles and obsessions, t2
    tc - sorted DataFrame, merging pvs and time, calculating difference
    """
    t1pv, t2pv, t1time, t2time, tc = t2t1_changes.main(t1, t2)

    print('obsession pv change', tc.iloc[1:]['pv t2'].sum() / 
          tc.iloc[1:]['pv t1'].sum())
    print('obsession time change', tc.iloc[1:]['time t2'].sum() / 
      tc.iloc[1:]['time t1'].sum())
    
    return t1pv, t2pv, t1time, t2time, tc

def hours_per_article(pv_data, time_data, num=500):
    """num is the minimum number of page views to include in this analysis
    """
    df = hours_article_obsession.main(pv_data, time_data, num=num)
    return df
        
    
    