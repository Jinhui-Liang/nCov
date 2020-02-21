#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import
import pandas as pd
import numpy as np
import pickle
# Load
with open('im.txt','rb') as f:
    im = pickle.load(f)


# In[31]:


# Reshape the im matrix
li = []
for d in im.keys():
    temp = im[d].unstack().reset_index()
    temp['city'] = d
    li.append(temp)
imdf = pd.concat(li,axis=0)    
imdf.rename(columns={"level_0":"date","level_1":"origin",0:"ratio"},inplace=True)
imdf


# In[30]:




