# -*- coding: utf-8 -*-
"""
Created on Sat Jul  8 07:17:53 2023

@author: richie bao
"""
import rioxarray as rxr
from sklearn.utils import Bunch
import os
import uuid
import numpy as np
import pickle

LC_fn=r'I:\\data\\ESA_London\\ESA_WorldCover_10m_2020_v100_N51W003_Map.tif'
LC= rxr.open_rasterio(LC_fn)

# dataset_root=r'I:\data\london\lc_lst_dataset'

def data_target_func(df):
    data = []
    target = []
    for idx,row in df.iterrows():
        #print(idx,row)
        try:
            clipped_LC=LC.rio.clip([row.geometry],from_disk=True)
            cell_rank=row['rank']
            #print(clipped_LC.data.shape,cell_rank)
            clipped_LC_adj=clipped_LC.data[:,5:95,10:160] # [:,5:95,10:167]
            if clipped_LC_adj.shape==(1,90,150): #(1,90,157)
                data.append(clipped_LC_adj)
                target.append(cell_rank)   
        except:
            pass
    if len(data)>0:    
        data = np.array(data)
        target = np.array(target)
        # LC2LST_dataset=Bunch(data=data, target=target)
        
        # dataset_fn=os.path.join(dataset_root,f'lcNlst_{uuid.uuid4()}.pickle') #r'I:\data\london\LC2LST_dataset.pickle'
        # with open(dataset_fn,'wb') as f:
        #     pickle.dump(LC2LST_dataset,f)         
        return [data,target]
    
        
if __name__=="__main__":
    with open(r'I:\data\london\lc_lst_dataset\lcNlst_e261062e-eb93-4bfa-8c13-6d5029c7d4e4.pickle','rb') as f:
        a=pickle.load(f)