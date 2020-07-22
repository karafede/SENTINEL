
import os
os.chdir('D:/ENEA_CAS_WORK/SENTINEL/viasat_data')
os.getcwd()

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import folium
import osmnx as ox
import networkx as nx
import math
import momepy
from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal
import geopy.distance
import momepy
from shapely import wkb


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

os.chdir('D:/ENEA_CAS_WORK/SENTINEL/viasat_data')
os.getcwd()

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_SA')

## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM "OSM_edges" ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)
## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()



# make a list of unique dates (only dates not times!)
# select an unique table of dates postgresql
unique_DATES = pd.read_sql_query(
    '''SELECT DISTINCT all_dates.dates
        FROM ( SELECT dates.d AS dates
               FROM generate_series(
               (SELECT MIN(timedate) FROM public.mapmatching_2019),
               (SELECT MAX(timedate) FROM public.mapmatching_2019),
              '1 day'::interval) AS dates(d)
        ) AS all_dates
        INNER JOIN public.mapmatching_2019
	    ON all_dates.dates BETWEEN public.mapmatching_2019.timedate AND public.mapmatching_2019.timedate
        ORDER BY all_dates.dates ''', conn_HAIG)

# ADD a new field with only date (no time)
unique_DATES['just_date'] = unique_DATES['dates'].dt.date

# subset database with only one specific date and one specific TRACK_ID)
for idx, row in unique_DATES.iterrows():
    DATE = row[1].strftime("%Y-%m-%d")
    print(DATE)

### read csv file from saved from R
import csv
df = pd.read_csv("FCD_2017_UNISA_2019.csv", delimiter=',')
## filter by one specific DATE
DATE = '2019-09-02'
df['timedate'] = df['timedate'].astype('datetime64[ns]')
df['date'] = df['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
df=df[df.date == DATE]

### get only "vehtype = 2" (mezzi pesanti)
df = df[df.vehtype == 2]

## (25844050, 1110091861) <-- Salerno
## (1110091904, 3371747395)  --> Avellino

## divide data by directions
## get all the IDTERM  vehicles passing through the AUTOSTRADA A2 section chosen by Uni Salerno
df_salerno = df[(df['u'] == 25844050) & (df['v'] == 1110091861) ]
df_avellino = df[(df['u'] == 1110091904) & (df['v'] == 3371747395) ]

df_salerno.drop_duplicates(['idterm'], inplace=True)
df_avellino.drop_duplicates(['idterm'], inplace=True)

# ## make a list of all IDterminals for the direction of Salerno and Avellino
all_idterms_salerno = list(df_salerno.idterm.unique())
all_idterms_avellino = list(df_avellino.idterm.unique())


# ## save 'all_ID_TRACKS_salerno' as list
# with open("all_idterm_salerno_2019.txt", "w") as file:
#     file.write(str(all_idterms_salerno))
# with open("all_idterm_avellino_2019.txt", "w") as file:
#     file.write(str(all_idterms_avellino))


from datetime import datetime
now1 = datetime.now()

viasat_data = pd.read_sql_query('''
                    SELECT  
                       mapmatching_2019.u, mapmatching_2019.v,
                            mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                            mapmatching_2019.idtrace, mapmatching_2019.sequenza,
                            dataraw.idterm, dataraw.vehtype
                       FROM mapmatching_2019
                       LEFT JOIN dataraw 
                                   ON mapmatching_2019.idtrace = dataraw.id  
                                   WHERE date(mapmatching_2019.timedate) = '2019-09-02' 
                                   AND dataraw.vehtype::bigint = 2
                    ''', conn_HAIG)

now2 = datetime.now()
print(now2 - now1)



###############################
##### WORK in progress ########

all_salerno = viasat_data[viasat_data.idterm.isin(all_idterms_salerno)] # (u,v) (25844050, 1110091861) <-- Salerno
all_avellino = viasat_data[viasat_data.idterm.isin(all_idterms_avellino)]  # (u, v) -->  (1110091904, 3371747395)  --> Avellino

## get data with "sequenza" STARTING from the chosen nodes on the A2 for each idterm
pnt_sequenza_salerno = all_salerno[(all_salerno['u'] == 25844050) & (all_salerno['v'] == 1110091861) ]
pnt_sequenza_avellino = all_avellino[(all_avellino['u'] == 1110091904) & (all_avellino['v'] == 3371747395) ]

### initialize an empty dataframe
partial_Salerno = pd.DataFrame([])
partial_Avellino = pd.DataFrame([])

###############
### SALERNO ###
###############
for idx, idterm in enumerate(all_idterms_salerno):
    print(idterm)
    ### get starting "sequenza"
    sequenza_salerno = pnt_sequenza_salerno[pnt_sequenza_salerno.idterm == idterm]['sequenza'].iloc[0]
    sub_salerno = all_salerno[(all_salerno.idterm == idterm) & (all_salerno.sequenza >= sequenza_salerno)]
    partial_Salerno = partial_Salerno.append(sub_salerno)

# ## further filtering on the direction of Avellino
# partial_Salerno_bis = pd.DataFrame([])
#
# for idx, idterm in enumerate(all_idterms_avellino):
#     print(idterm)
#     # get starting "sequenza"
#     sequenza = pnt_sequenza_avellino[pnt_sequenza_avellino.idterm == idterm]['sequenza'].iloc[0]
#     sub = partial_Salerno[(partial_Salerno.idterm == idterm) & (partial_Salerno.sequenza < sequenza)]
#     partial_Salerno_bis = partial_Salerno_bis.append(sub)


################
### AVELLINO ###
################
for idx, idterm in enumerate(all_idterms_avellino):
    print(idterm)
    # get starting "sequenza"
    sequenza = pnt_sequenza_avellino[pnt_sequenza_avellino.idterm == idterm]['sequenza'].iloc[0]
    sub = all_avellino[(all_avellino.idterm == idterm) & (all_avellino.sequenza >= sequenza)]
    partial_Avellino = partial_Avellino.append(sub)



## further filtering on the direction of Salerno
# partial_Avellino_bis = pd.DataFrame([])
#
# for idx, idterm in enumerate(all_idterms_salerno):
#     print(idterm)
#     # get starting "sequenza"
#     sequenza = pnt_sequenza_salerno[pnt_sequenza_salerno.idterm == idterm]['sequenza'].iloc[0]
#     sub = partial_Avellino[(partial_Avellino.idterm == idterm) & (partial_Avellino.sequenza < sequenza)]
#     partial_Avellino_bis = partial_Avellino_bis.append(sub)


# partial_Salerno = partial_Salerno_bis
# partial_Avellino = partial_Avellino_bis

salerno = partial_Salerno[['u','v']]
avellino = partial_Avellino[['u','v']]

####################################
####################################



## filter by idterm (passing through the A2 section of motorway chosen by Uni Salerno
# salerno = viasat_data[viasat_data.idterm.isin(all_idterms_salerno)][['u','v']]
# avellino = viasat_data[viasat_data.idterm.isin(all_idterms_avellino)][['u','v']]

# del viasat_data

#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

counts_uv_salerno = salerno.groupby(salerno.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})
counts_uv_avellino = avellino.groupby(avellino.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts'})

counts_uv_salerno = pd.merge(counts_uv_salerno, gdf_edges, on=['u', 'v'], how='left')
counts_uv_salerno = gpd.GeoDataFrame(counts_uv_salerno)
counts_uv_salerno.drop_duplicates(['u', 'v'], inplace=True)
counts_uv_salerno.plot()

## merge counts with OSM (Open Street Map) edges
counts_uv_avellino = pd.merge(counts_uv_avellino, gdf_edges, on=['u', 'v'], how='left')
counts_uv_avellino = gpd.GeoDataFrame(counts_uv_avellino)
counts_uv_avellino.drop_duplicates(['u', 'v'], inplace=True)
counts_uv_avellino.plot()

counts_uv_salerno["scales"] = (counts_uv_salerno.counts/max(counts_uv_salerno.counts)) * 7
counts_uv_avellino["scales"] = (counts_uv_avellino.counts/max(counts_uv_avellino.counts)) * 7


################################################################################
# create basemap SALERNO (Fisciano, localita' Penta)
ave_LAT = 40.760773
ave_LON = 14.788383
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################

folium.GeoJson(
counts_uv_salerno[['u','v', 'counts', 'scales', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'red',
        'color': 'red',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'counts']),
    ).add_to(my_map)


my_map.save("traffic_pesanti_partial_counts_salerno_02_Sept_2019.html")


################################################################################
# create basemap SALERNO (Fisciano, localita' Penta)
ave_LAT = 40.760773
ave_LON = 14.788383
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#################################################################################


folium.GeoJson(
counts_uv_avellino[['u','v', 'counts', 'scales', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'blue',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'counts']),
    ).add_to(my_map)

my_map.save("traffic_pesanti_partial_counts_avellino_02_Sept_2019.html")



