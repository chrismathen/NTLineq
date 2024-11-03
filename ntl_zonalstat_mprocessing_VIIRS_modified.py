# -*- coding: utf-8 -*-
"""
Created on Tue Dec 20 16:41:57 2022

@author: Chris
"""
#%% Imports
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 13:22:35 2022

@author: Chris
"""
import pandas as pd
import geopandas as gpd
import pyproj
from pyproj import CRS
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import matplotlib as mpl
import rasterio as rio
from rasterio.plot import show
from rasterio.mask import mask
from rasterstats import zonal_stats
import plotly.express as px
import json
import numpy as np
import time
import glob
from multiprocessing import Pool, cpu_count
import time
import os


# Now df is no longer an argument but read from global storage:
def poly2(data):## i is for year 
    data['file_data']=rio.open(data['file_path'])
    array_name, trans_name = mask(data['file_data'], shapes=data['vector'].geometry, crop=True, nodata=np.nan)
    zs= zonal_stats(data['vector'], array_name[0], affine=trans_name, stats=['mean'], nodata=np.nan, all_touched=True)
    # file_data=
    return {
        'file_name':data['file_name'],
        'ld':[x[('mean')] for x in zs]
    }



# import poly

# Required by Windows (but okay even if not Windows):
if __name__ == '__main__':

        
    cwd = os.getcwd() 
    #%% 
    ntl_path=[]
    ntl_path=glob.glob('ntlpath/*.tif', recursive=False)
    # for i in range(len(ntl_dmsp_path)): ##print profile
    #     print(rio.open(ntl_dmsp_path[i]).profile)

    # filenames=[]   ###naming the files in list
    data_list=[] 
    # data_dict=dict()## pass list thru multiprocessing pool
    def data_list():
        for i in range(len(ntl_path)):
            data=dict()
            filename=ntl_path[i][ntl_path[i].find("p_")+2 : ntl_path[i].find("_global")]
            data["file_name"]=filename
            data['file_path']=ntl_path[i]
            data['vector']=df_hr_1km
            yield data

    
    with Pool(4) as pool:
        km=1
        df_hr_1km = gpd.read_file(fr"gridsource\{km}km\india_level6_city_{km}km.shp")
        ctr=0
        for result in pool.imap_unordered(poly2, data_list()):
            df_hr_1km = gpd.read_file(fr"gridsource\{km}km\india_level6_city_{km}km.shp")
            df_hr_1km['ld_'+result['file_name']] = result['ld']
            ctr+=1
            print(f"{ctr} - Done for {result['file_name']}")
            df_hr_20kmcsv = pd.DataFrame(df_hr_1km)
            df_hr_20kmcsv.to_csv(f"output/{result['file_name']}_VIIRS_df_hr_1km_ntl.csv", index = False)
    pass