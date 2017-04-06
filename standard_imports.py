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

class time_API:
    def __init__(self):
        self.start = (datetime.now() + timedelta(-7)).strftime('%Y-%m-%d') 
        self.end = datetime.now().strftime('%Y-%m-%d')
        self.timeframe = None
        self.time_parameter = None
    
    def get_time(self):
        """returns start, end to be used in keen API calls
        """
        
        start_datetime = datetime.strptime(self.start, '%Y-%m-%d')
        days = ((datetime.now() - start_datetime).days)
        try:
            left = self.time_parameter.find('_') + 1
            right = self.time_parameter.rfind('_')
            old_days = int(self.time_parameter[left:right])
        except:
            pass
        
        if self.time_parameter != None and old_days > days:
            start = [i for i,(j,k) in enumerate(self.timeframe) if self.start in j]
            end = [i for i,(j,k) in enumerate(self.timeframe) if self.end in k]

            start = self.timeframe[start[0]][0]
            end = self.timeframe[end[0]][1]

            return start, end

        else:                               
        
            self.time_parameter = 'previous_' + str(days) + '_days'
            timeframe = self.timeframe_API_call()

            start = [i for i,(j,k) in enumerate(timeframe) if self.start in j]
            end = [i for i,(j,k) in enumerate(timeframe) if self.end in k]

            start = timeframe[start[0]][0]
            end = timeframe[end[0]][1]

            return start, end

                                                 
    def timeframe_API_call(self):
        """
        """
        from collections import namedtuple
        
        event = 'click_article_link'
    
        timeframe= self.time_parameter
        interval = 'daily'
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
        
        Timeframe = namedtuple('daily_time', 'start end')
        timeframe = [Timeframe(i['timeframe']['start'], 
                               i['timeframe']['end']) for i in data]
        
        print(datetime.now() - t)
        
        self.timeframe = timeframe
        return timeframe