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

from standard_imports import time_API

from API_calls import dumpin
from API_calls import time_spent
from API_calls import articles_obsessions

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