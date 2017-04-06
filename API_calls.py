#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 10:58:05 2017

@author: csaunders
"""

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

