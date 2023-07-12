# -*- coding: utf-8 -*-
"""
Created on Sat Jul  8 06:46:03 2023

@author: richie bao
"""
from multiprocessing import Pool
import multiprocessing 
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from sklearn.utils import Bunch
import pickle

from LC_LST_dataset_pool import data_target_func

def purify(o):
    if hasattr(o, 'items'):
        oo = type(o)()
        for k in o:
            if k != None and o[k] != None:
                oo[k] = purify(o[k])
    elif hasattr(o, '__iter__'):
        oo = [ ] 
        for it in o:
            if it != None:
                oo.append(purify(it))
    else: return o
    return type(o)(oo)

LST_rank_fn=r'I:\data\london\LST_rank_10.shp'
LST_rank=gpd.read_file(LST_rank_fn)

def LC_LST_dataset(LST_rank,dataset_fn,ratio_cpu=0.5,ratio_split=1):
    cpus = multiprocessing.cpu_count()

    cpus_used=int(cpus*ratio_cpu)
    with Pool(cpus_used) as p:
        # data_target=p.map(args, tqdm(np.array_split(LST_rank[:10],cpus_used)))
        data_target=p.map(data_target_func,tqdm(np.array_split(LST_rank,cpus_used*ratio_split)))
        
    data_target=[x for x in data_target if x is not None]   
    data_lst,target_lst=zip(*data_target)
    data=np.vstack(data_lst)
    target=np.hstack(target_lst)
    
    LC2LST_dataset=Bunch(data=data, target=target)
    
    with open(dataset_fn,'wb') as f:
        pickle.dump(LC2LST_dataset,f)        

if __name__=="__main__":
    dataset_fn='I:\data\london\LC2LST_dataset.pickle'
    LC_LST_dataset(LST_rank,dataset_fn,ratio_cpu=0.7,ratio_split=30) # 30

    # with open(dataset_fn,'rb') as f:
    #     ds=pickle.load(f)