#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 10:58:05 2017

@author: csaunders
"""

from retrying import retry
from datetime import datetime
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
    """return time spent over time frame, grouped by obsessions and article ids
    """
    
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}
    interval = None
    
    group_by = ('article.id','article.obsessions')

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

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name3, "operator":operator3, "property_value":property_value3},
               {"property_name":property_name4, "operator":operator4, "property_value":property_value4},
               {"property_name":property_name5, "operator":operator5, "property_value":property_value5}]

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
    """return page views over time range, grouped by obesssions and article ids
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

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

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
    keen API call - incremental time; MANY groupbys so the start, end timeframe needs to be a short interval
    else Keen will fail
    """
    
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}
    
    group_by = ('article.id', 'article.obsessions', 'read.time.incremental.seconds', 'read.type')
 
    property_name1 = 'read.type'
    operator1 = 'in'
    property_value1 = (25, 50, 75, 'complete')

    property_name2 = 'article.id'
    operator2 = 'in'
    property_value2 = list(article_list)
    

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    t = datetime.now()

    data = keen.count(event, 
                      timeframe=timeframe,
                      group_by=group_by,
                      filters=filters)

    print(start, end, datetime.now() - t)
    return data

@retry(stop_max_attempt_number=3)
def headline_word_count(start, end, article_list):
    event = 'read_article' 

    timeframe ={'start':start, 'end':end}

    group_by =('article.id', 'article.headline.content', 'article.content.words.count') 


    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    property_name2 = 'article.id'
    operator2 = 'in'
    property_value2 = article_list


    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    t = datetime.now()

    data = keen.count(event, 
                    timeframe=timeframe,
                    group_by=group_by,
                    filters=filters)

    print(start, end, datetime.now() - t)
    return data

@retry(stop_max_attempt_number=3)
def non_obsession_info(start, end):
    """
    """
    event = 'read_article'
    
    timeframe = {'start':start, 'end':end}
    
    group_by = ('article.id', 'article.topic',
                'article.content.words.count', 'article.headline.content')
    
    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'
    
    property_name2 = 'article.obsessions'
    operator2 = 'eq'
    property_value2 = ''
    
    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]
    
    
    t = datetime.now()
    data = keen.count(event, timeframe=timeframe,
                      group_by=group_by, 
                      filters=filters)
    
    print(start, end, datetime.now() - t)
    return data