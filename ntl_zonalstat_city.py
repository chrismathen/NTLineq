import pandas as pd
import geopandas as gpd
from pyproj import CRS
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import matplotlib as mpl
import rasterio as rio
from rasterio.plot import show
from rasterio.mask import mask
from rasterstats import zonal_stats
import plotly.express as px
import numpy as np
import glob


df_hr = gpd.read_file("gridsource/1km/india_level6_city.shp")



landscan_pop_path=glob.glob('poppath/*.tif', recursive=False)
filenames=[]
for i in range(len(landscan_pop_path)):
    filenames.append(landscan_pop_path[i][landscan_pop_path[i].find("n-")+2 : landscan_pop_path[i].find(".tif")].replace("global-","pop"))

for i in range(len(landscan_pop_path)):
    filenames[i]=rio.open(landscan_pop_path[i])

lst=[]
for i in range(len(landscan_pop_path)):
    lst.append(i)


for i in range(len(landscan_pop_path)):
    array_name, trans_name = mask(filenames[i], shapes=df_hr.geometry, crop=True, nodata=np.nan)
    zs= zonal_stats(df_hr, array_name[0], affine=trans_name, stats=['mean','sum'], nodata=np.nan, all_touched=True)
    df_hr['ld_'+str(landscan_pop_path[i][landscan_pop_path[i].find("n-")+2 : landscan_pop_path[i].find(".tif")].replace("global-","pop"))] = [x[('mean')] for x in zs]
    df_hr['sol_'+str(landscan_pop_path[i][landscan_pop_path[i].find("n-")+2 : landscan_pop_path[i].find(".tif")].replace("global-","pop"))] = [x[('sum')] for x in zs]
    print(i)

ntl_path=glob.glob('ntlpath/*.tif', recursive=False)
filenames=[]
for i in range(len(ntl_path)):
    filenames.append(ntl_path[i][ntl_path[i].find("p_")+2 : ntl_path[i].find("_global")])

for i in range(len(ntl_path)):
    filenames[i]=rio.open(ntl_path[i])

lst=[]
for i in range(len(ntl_path)):
    lst.append(i)


for i in range(len(ntl_path)):
    array_name, trans_name = mask(filenames[i], shapes=df_hr.geometry, crop=True, nodata=np.nan)
    zs= zonal_stats(df_hr, array_name[0], affine=trans_name, stats=['mean','sum'], nodata=np.nan, all_touched=True)
    df_hr['ld_viirs_'+str(ntl_path[i][ntl_path[i].find("p_")+2 : ntl_path[i].find("_global")])] = [x[('mean')] for x in zs]
    df_hr['sol_viirs_'+str(ntl_path[i][ntl_path[i].find("p_")+2 : ntl_path[i].find("_global")])] = [x[('sum')] for x in zs]
    print(i)

list(df_hr.columns)


df_hr = pd.DataFrame(df_hr.drop(columns='geometry'))
df_hr.to_csv('output/controlvar/df_hr_ntl.csv', index = False)