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