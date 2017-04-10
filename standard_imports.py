#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core imports for python     

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

class time_API:
    def __init__(self):
        self.start = (datetime.now() + timedelta(-7)).strftime('%Y-%m-%d') 
        self.end = datetime.now().strftime('%Y-%m-%d')
        self.timeframe_daily = None
        self.timeframe_hourly = None 
        self.time_parameter = None
        self.start_UTC = None
        self.end_UTC = None
        
    def set_time(self):
        """used to set the time and timeframes for making API calls, default
        time setting will be previous_7_days, unless self.start and self.end
        are explicitly set
        
        running will update all of the class attributes that are by default
        set to None
        """
        
        start_datetime = datetime.strptime(self.start, '%Y-%m-%d')
        days = ((datetime.now() - start_datetime).days)
                              
        self.time_parameter = 'previous_' + str(days) + '_days'
        self.timeframe_daily = self.timeframe_API_call(interval='daily')

        start = [i for i,(j,k) in enumerate(self.timeframe_daily) if self.start in j]
        end = [i for i,(j,k) in enumerate(self.timeframe_daily) if self.end in k]

        self.start_UTC = self.timeframe[start[0]][0]
        self.end_UTC = self.timeframe[end[0]][1]
        end = [i for i,(j,k) in enumerate(self.timeframe_daily) if self.end_UTC in k]
        self.timeframe_daily = self.timeframe_daily[:end[0]+1]
        
        self.timeframe_hourly = self.timeframe_API_call(interval='hourly')
        end = [i for i,(j,k) in enumerate(self.timeframe_hourly) if self.end_UTC in k]
        self.timeframe_hourly = self.timeframe_hourly[:end[0]+1]
        
        return self.start_UTC, self.end_UTC
    
    def timeframe_API_call(self, interval='daily'):
        """API call against dummy serach, meant to have zero results but to 
        return a clean timeframe / interval on which to conduct API calls
        """
        from collections import namedtuple
        
        event = 'click_article_link'
    
        timeframe= self.time_parameter
        interval = interval
        timezone = "US/Eastern"
    
        group_by = None
    
        property_name1 = 'article.bulletin.campaign.id'
        operator1 = 'eq'
        property_value1 = 666
    
        filters = [{"property_name":property_name1, "operator":operator1,
                    "property_value":property_value1}]
    
        t = datetime.now()
    
        data = keen.count(event, 
                           timeframe=timeframe, interval=interval,
                           timezone=timezone,
                           group_by=group_by, 
                           filters=filters)
        
        Timeframe = namedtuple('time', 'start end')
        timeframe = [Timeframe(i['timeframe']['start'], 
                               i['timeframe']['end']) for i in data]
        
        print(datetime.now() - t)
        
        self.timeframe = timeframe
        return timeframe
    
    def custom_time(self, hours=6):
        """used to create a custom, more narrowly defined timeframe for making
        API calls for when Keen is being a bitch and unable to handle 
        lot's of groups or whatever happens to be the lame reason
        defaults to 6 hours, but can be set to any interval; note that last
        time is likely not going to be this interval
        """
        tz = self.timeframe_hourly[::hours]
        tz = [(tz[i][0], tz[i+1][0]) for i in range(len(tz)-1)]
        tz.append((tz[-1][-1], self.end_UTC))
        return tz