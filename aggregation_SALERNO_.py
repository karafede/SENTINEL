
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
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import datetime
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from folium_stuff_FK_map_matching import plot_graph_folium_FK
from shapely.geometry import Point, LineString, MultiLineString
from shapely import geometry, ops
import glob
import db_connect
from shapely import wkb
import sqlalchemy as sal


conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_SA')

# get all map-matched data from the DB
# gdf_all_EDGES = pd.read_sql_query(
#     ''' SELECT *
#         FROM public.mapmatching_2017''', conn_HAIG)

## transform Geometry from text to LINESTRING
# wkb.loads(gdf_all_EDGES.geom, hex=True)
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# gdf_all_EDGES['geometry'] = gdf_all_EDGES.apply(wkb_tranformation, axis=1)
# gdf_all_EDGES.drop(['geom'], axis=1, inplace= True)
# gdf_all_EDGES = gpd.GeoDataFrame(gdf_all_EDGES)
# gdf_all_EDGES.plot()


# os.chdir('C:\\ENEA_CAS_WORK\\Catania_RAFAEL\\postprocessing')
## select only columns 'u' and 'v'
# gdf_all_EDGES_sel = gdf_all_EDGES[['u', 'v']]
# time --> secs
# distance --> km
# speed --> km/h
# gdf_all_EDGES_time = gdf_all_EDGES[['u', 'v', 'mean_speed']]

#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

# df_all_EDGES_sel = gdf_all_EDGES.groupby(gdf_all_EDGES_sel.columns.tolist(), sort=False).size().reset_index().rename(columns={0:'records'})

df_all_EDGES_sel = pd.read_sql_query('''
                SELECT u, v, COUNT(*)
                FROM  public.mapmatching_2017
                GROUP BY u, v ''', conn_HAIG)

# make a copy
df_all_EDGES_records = df_all_EDGES_sel

### add colors based on 'records'
vmin = min(df_all_EDGES_records['count'])
vmax = max(df_all_EDGES_records['count'])
# df_all_EDGES_records.iloc[-1] = np.nan
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.Reds)  # scales of reds
df_all_EDGES_records['color'] = df_all_EDGES_records['count'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

# df_all_EDGES_sel = df_all_EDGES_sel[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
# clean_edges_matched_route = pd.merge(df_all_EDGES_sel, gdf_all_EDGES, on=['u', 'v'],how='left')

clean_edges_matched_route = pd.read_sql_query('''
                                      WITH df_all_EDGES_sel AS(
                                      SELECT u, v, COUNT(*)
                                      FROM  mapmatching_2017
                                      GROUP BY u, v)
                                      SELECT df_all_EDGES_sel.u, 
                                             df_all_EDGES_sel.v,
                                             df_all_EDGES_sel.count, 
                                      mapmatching_2017.idtrajectory, 
                                      mapmatching_2017.idtrace, 
                                      mapmatching_2017.sequenza, 
                                      mapmatching_2017.mean_speed,
                                      mapmatching_2017.timedate,
                                      mapmatching_2017.totalseconds, 
                                      "TRIP_ID", 
                                      mapmatching_2017.length, 
                                      mapmatching_2017.highway, 
                                      mapmatching_2017.name,
                                      mapmatching_2017.ref, 
                                      mapmatching_2017.geom
                                      FROM df_all_EDGES_sel
                                      LEFT JOIN mapmatching_2017 ON df_all_EDGES_sel.u = mapmatching_2017.u
                                      AND df_all_EDGES_sel.v = mapmatching_2017.v
                                      LIMIT 500000''', conn_HAIG)

CCC = clean_edges_matched_route[(clean_edges_matched_route['u'] == 1110091904) & (clean_edges_matched_route['v'] == 3371747395) ][['u', 'v', 'count']]
DDD = clean_edges_matched_route[(clean_edges_matched_route['u'] == 25844050) & (clean_edges_matched_route['v'] == 1110091861) ][['u', 'v', 'count']]
# CCC = clean_edges_matched_route[clean_edges_matched_route['u'] == 3371747395]


clean_edges_matched_route['geometry'] = clean_edges_matched_route.apply(wkb_tranformation, axis=1)
clean_edges_matched_route.drop(['geom'], axis=1, inplace= True)
clean_edges_matched_route = gpd.GeoDataFrame(clean_edges_matched_route)
## eventually....remove duplicates
clean_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)
clean_edges_matched_route.plot()

############################################################################
############################################################################


### get vehicle tpye from "obu"
idterm_category = pd.read_sql_query('''
    WITH ids AS (SELECT
    split_part("TRIP_ID"::TEXT,'_', 1) idterm
    FROM
    mapmatching_2017)
    select ids.idterm,
    /*  obu.idterm, */
    obu.idvehcategory
    FROM ids
    LEFT JOIN obu ON ids.idterm::bigint = obu.idterm
    LIMIT 100000''', conn_HAIG)





## some 'u' and 'v'
# 0  3081517788  3321024237    398
# 1  3321024237  3160955736    390
# 2  3160955736   429505213    414
# 3   429505213  5517309323    215
# 4   983659016  1586340139   1086
## https://www.postgresqltutorial.com/postgresql-where/


idterm_type = pd.read_sql_query('''
    SELECT
    split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID"
    FROM
    mapmatching_2017
    LIMIT 100''', conn_HAIG)


idterm_type = pd.read_sql_query('''
    SELECT
    split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID"
    FROM
    mapmatching_2017
    WHERE ("u", "v") in (VALUES (1110091904, 3371747395))
    UNION
    SELECT
    split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID"
    FROM
    mapmatching_2017
    WHERE  ("u", "v") in (VALUES (25844050, 1110091861))
    LIMIT 100''', conn_HAIG)

## nodes of the selected area near Fisciano (u,v order)
## (1110091857, 1110091904)   Avellino-Salerno (--> uscita IKEA)
## (1110091904, 3371747395)   Avellino-Salerno
## (25844050, 1110091861)    Salerno-Avellino
## (1110091861, 429505186)  Salerno-Avellino (<--entrata Univerista')

########################################################################################
### get vehicle type from "dataraw" for mapmatched vehicles within selected 'u', 'v' ###
########################################################################################

idterm_type = pd.read_sql_query('''
    WITH ids AS (
            SELECT
            split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID",
            timedate, mean_speed, length
            FROM
            mapmatching_2017
            WHERE ("u", "v") in (VALUES (1110091904, 3371747395))
        UNION
            SELECT
            split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID",
            timedate, mean_speed, length
            FROM
            mapmatching_2017
            WHERE ("u", "v") in (VALUES (25844050, 1110091861))
        LIMIT 100),
    ids_grouped AS (
    SELECT u, v, COUNT(*)
    FROM  ids
    GROUP BY u, v)
    SELECT ids.idterm,
        ids.u, ids.v,
        ids."TRIP_ID",
        ids.timedate,
        ids.mean_speed,
        ids.length,
        dataraw.vehtype,
        ids_grouped.count
    FROM ids
    LEFT JOIN dataraw ON ids.idterm::bigint = dataraw.idterm
    LEFT JOIN ids_grouped ON ids.u = ids_grouped.u AND ids.v = ids_grouped.v
    ''', conn_HAIG)


obu = pd.read_sql_query('''
select *
from public.obu''', conn_HAIG)

dataraw = pd.read_sql_query('''
select *
from dataraw
limit 1000''', conn_HAIG)


route_check_2017_vehtype = pd.read_sql_query('''
                                    SELECT
                                        routecheck_2019.id,
                                        dataraw.id,
                                        dataraw.vehtype
                                    FROM
                                        routecheck_2019
                                    LEFT JOIN dataraw ON routecheck_2019.id = dataraw.id
                                    LIMIT 100''', conn_HAIG)

viasat_fleet = pd.read_sql_query('''
              SELECT *
              FROM public.dataraw
              WHERE vehtype = '2' 
              LIMIT 100''', conn_HAIG)

#########################################################
####### HOURLY AGGREGATION ##############################

# https://dba.stackexchange.com/questions/68000/sql-hourly-data-aggregation-in-postgresql
# https://stackoverflow.com/questions/42117796/how-do-i-group-by-by-hour-in-postgresql-with-a-time-field
# https://www.postgresqltutorial.com/postgresql-date_trunc/

idterm_type = pd.read_sql_query('''
    WITH ids AS (
            SELECT
            split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v, "TRIP_ID", 
            timedate, mean_speed, length
            FROM
            mapmatching_2017
            WHERE ("u", "v") in (VALUES (25844050, 1110091861),
                                        (1110091904, 3371747395)) 
        LIMIT 100),
    ids_grouped AS (
    SELECT u, v, COUNT(*)
    FROM  ids
    GROUP BY u, v)
    SELECT ids.idterm,
        ids.u, ids.v, 
        ids."TRIP_ID",
        ids.timedate,
        ids.mean_speed, 
        ids.length,
        dataraw.vehtype,
        ids_grouped.count
    FROM ids
    LEFT JOIN dataraw ON ids.idterm::bigint = dataraw.idterm
    LEFT JOIN ids_grouped ON ids.u = ids_grouped.u AND ids.v = ids_grouped.v
    ''', conn_HAIG)



IDs_hourly = pd.read_sql_query('''  WITH ids AS 
                                    (SELECT
                                    split_part("TRIP_ID"::TEXT,'_', 1) idterm, u, v,
                                    timedate, mean_speed
                                    FROM mapmatching_2017
                                    WHERE ("u", "v") in (VALUES (25844050, 1110091861),
                                                                (1110091904, 3371747395))
                                    LIMIT 100),
                                ids_grouped AS(
                                    SELECT  
                                    date_trunc('hour', ids.timedate) timehour,
                                    date_trunc('day', ids.timedate) timeday,
                                    date_part('hour', ids.timedate) ora,
                                    AVG(mean_speed),
                                    dataraw.vehtype,
                                    ids.idterm,
                                    ids.u, ids.v, COUNT(*)
                                FROM  ids
                                LEFT JOIN dataraw ON ids.idterm::bigint = dataraw.idterm
                                    GROUP BY 
                                date_trunc('day', ids.timedate),
                                date_trunc('hour', ids.timedate),
                                date_part('hour', ids.timedate),
                                dataraw.vehtype,
                                ids.u, ids,v,
                                ids.idterm)
                                    SELECT ids_grouped.AVG,
                                ids_grouped.timeday,
                                ids_grouped.timehour,
                                ids_grouped.ora,
                                ids_grouped.vehtype,
                                ids_grouped.idterm,
                                ids_grouped.u,
                                ids_grouped.v,
                                ids_grouped.count
                                    FROM ids_grouped
                                    ''', conn_HAIG)
IDs_hourly.to_csv('IDs_hourly.csv')



###################################################################
#### create table with 'idterm', 'vehtype' and 'portata' ##########

idterm_vehtype_portata = pd.read_sql_query('''
                       WITH ids AS (SELECT idterm, vehtype
                                    FROM
                               dataraw)
                           select ids.idterm,
                                  ids.vehtype,
                                  obu.portata
                        FROM ids
                        LEFT JOIN obu ON ids.idterm = obu.idterm
                        ''', conn_HAIG)

## drop duplicates ###
idterm_vehtype_portata.drop_duplicates(['idterm'], inplace=True)
idterm_vehtype_portata.to_csv('D:/ENEA_CAS_WORK/SENTINEL/viasat_data/idterm_vehtype_portata.csv')
## relaod .csv file
idterm_vehtype_portata = pd.read_csv('D:/ENEA_CAS_WORK/SENTINEL/viasat_data/idterm_vehtype_portata.csv')
idterm_vehtype_portata = idterm_vehtype_portata[['idterm', 'vehtype', 'portata']]
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_SA')
connection = engine.connect()
idterm_vehtype_portata.to_sql("idterm_portata", con=connection, schema="public",
          if_exists='append', index=False)


# IDs_hourly = pd.read_sql_query('''  WITH ids AS
#                                     (SELECT
#                                     split_part("TRIP_ID"::TEXT,'_', 1) idterm, timedate
#                                     FROM mapmatching_2017
#                                     LIMIT 100)
#                                     SELECT date_trunc('day', ids.timedate),
#                                     ids.timedate,
#                                     ids.idterm
#                                     FROM ids
#                                     ''', conn_HAIG)


#################################################################################
#################################################################################
##################################################################################


# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
MERGED_clean_EDGES = pd.merge(clean_edges_matched_route, df_all_EDGES_records, on=['u', 'v'], how='inner')
# remove duplicates nodes
MERGED_clean_EDGES.drop_duplicates(['u', 'v'], inplace=True)
MERGED_clean_EDGES['records'] = round(MERGED_clean_EDGES['records'], 0)
MERGED_clean_EDGES['length(km)'] = MERGED_clean_EDGES['length']/1000
MERGED_clean_EDGES['length(km)'] = round(MERGED_clean_EDGES['length(km)'], 3)
# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_records = max(MERGED_clean_EDGES['records'])
MERGED_clean_EDGES['frequency(%)'] = (MERGED_clean_EDGES['records']/max_records)*100
MERGED_clean_EDGES['frequency(%)'] = round(MERGED_clean_EDGES['frequency(%)'], 0)

df_MERGED_clean_EDGES = pd.DataFrame(MERGED_clean_EDGES)

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=14, tiles='cartodbpositron')
###################################################

# add colors to map
my_map = plot_graph_folium_FK(MERGED_clean_EDGES, graph_map=None, popup_attribute=None,
                              zoom=15, fit_bounds=True, edge_width=2, edge_opacity=0.7)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    MERGED_clean_EDGES[['u','v', 'frequency(%)', 'records', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'length(km)', 'frequency(%)', 'records']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
##########################################

MERGED_clean_EDGES.to_file(filename='DB_FREQUENCIES_and_RECORDS_by_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_frequecy_all_EDGES_2019-04-15_May-26-2020.html")


#######################################################################
######### get the travelled TIME in each edge, when available #########
#######################################################################


### get AVERAGE of traveled "time" and travelled "speed" for each edge
df_all_EDGES_time = (gdf_all_EDGES_time.groupby(['u', 'v'], sort = False).mean()).reset_index()
df_all_EDGES_time.columns = ["u", "v", "travel_speed"]
df_all_EDGES_time = pd.merge(MERGED_clean_EDGES, df_all_EDGES_time, on=['u', 'v'], how='inner')
df_all_EDGES_time = pd.DataFrame(df_all_EDGES_time)

## get selected columns
df_all_EDGES_time = df_all_EDGES_time[["u", "v", "length(km)", "travel_speed"]]
## get travelled time (in seconds)
df_all_EDGES_time['travel_time'] = ((df_all_EDGES_time['length(km)']) / (df_all_EDGES_time['travel_speed'])) *3600 # seconds

# make a copy
df_all_timeEDGES = df_all_EDGES_time
# add colors based on 'time' (seconds)
vmin = min(df_all_timeEDGES.travel_time[df_all_timeEDGES.travel_time > 0])
vmax = max(df_all_timeEDGES.travel_time)
AVG = np.average(df_all_timeEDGES.travel_time)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.cool)  # scales of reds (or "coolwarm" , "bwr", °cool°)
df_all_timeEDGES['color'] = df_all_timeEDGES['travel_time'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_time = df_all_EDGES_time[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
times_edges_matched_route = pd.merge(df_all_EDGES_time, gdf_all_EDGES, on=['u', 'v'],how='left')
times_edges_matched_route = gpd.GeoDataFrame(times_edges_matched_route)
times_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)


# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
TIME_EDGES = pd.merge(times_edges_matched_route, df_all_timeEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
TIME_EDGES.drop_duplicates(['u', 'v'], inplace=True)
TIME_EDGES['travel_time'] = round(TIME_EDGES['travel_time'], 1)
TIME_EDGES['travel_speed'] = round(TIME_EDGES['travel_speed'], 0)

TIME_EDGES=TIME_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
TIME_EDGES=TIME_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})

df_TIME_EDGES = pd.DataFrame(TIME_EDGES)
merged_data = pd.merge(df_TIME_EDGES, df_MERGED_clean_EDGES, on=['u', 'v',
                                                         'index', 'idtrajectory', 'idtrace', 'sequenza',
                                                         'mean_speed', 'timedate', 'totalseconds', 'TRIP_ID',
                                                         'track_ID', 'length', 'highway', 'name', 'ref',
                                                         'length(km)'], how = 'inner')
merged_data.drop(['color_x', 'color_y', 'geometry_y'], axis=1, inplace = True)
merged_data = merged_data.rename(columns={'geometry_x': 'geometry'})


#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################

# add colors to map
my_map = plot_graph_folium_FK(TIME_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    TIME_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

TIME_EDGES.to_file(filename='DB_TIME_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_time_all_EDGES_2019-04-15_May-26-2020.html")

#######################################################################
######### get the travelled SPEED in each edge, when available ########
#######################################################################

### get average of traveled "time" and travelled "speed" for each edge
df_all_EDGES_speed = (gdf_all_EDGES_time.groupby(['u', 'v'], sort = False).mean()).reset_index()
df_all_EDGES_speed.columns = ["u", "v", "travel_speed"]
df_all_EDGES_speed = pd.merge(MERGED_clean_EDGES, df_all_EDGES_speed, on=['u', 'v'], how='inner')
df_all_EDGES_speed = pd.DataFrame(df_all_EDGES_speed)

## get selected columns
df_all_EDGES_speed = df_all_EDGES_speed[["u", "v", "length(km)", "travel_speed"]]
## get travelled time (in seconds)
df_all_EDGES_speed['travel_time'] = ((df_all_EDGES_speed['length(km)']) / (df_all_EDGES_speed['travel_speed'])) *3600 # seconds


# make a copy
df_all_speedEDGES = df_all_EDGES_speed
# add colors based on 'time' (seconds)
vmin = min(df_all_EDGES_speed.travel_speed[df_all_EDGES_speed.travel_speed > 0])
vmax = max(df_all_EDGES_speed.travel_speed)
AVG = np.average(df_all_EDGES_speed.travel_speed)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.YlGn)  # scales of reds (or "coolwarm" , "bwr")
df_all_EDGES_speed['color'] = df_all_EDGES_speed['travel_speed'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))

df_all_EDGES_speed = df_all_EDGES_speed[['u','v']]

# filter recover_all_EDGES (geo-dataframe) with df_recover_all_EDGES_sel (dataframe)
speeds_edges_matched_route = pd.merge(df_all_EDGES_speed, gdf_all_EDGES, on=['u', 'v'],how='left')
speeds_edges_matched_route = gpd.GeoDataFrame(times_edges_matched_route)
speeds_edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)


# get same color name according to the same 'u' 'v' pair
# merge records and colors into the geodataframe
SPEED_EDGES = pd.merge(speeds_edges_matched_route, df_all_speedEDGES, on=['u', 'v'], how='inner')
# remove duplicates nodes
SPEED_EDGES.drop_duplicates(['u', 'v'], inplace=True)
SPEED_EDGES['travel_time'] = round(SPEED_EDGES['travel_time'], 1)
SPEED_EDGES['travel_speed'] = round(SPEED_EDGES['travel_speed'], 0)

SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_time':'travel time (sec)'})
SPEED_EDGES=SPEED_EDGES.rename(columns = {'travel_speed':'travelled speed (km/h)'})

df_SPEED_EDGES = pd.DataFrame(SPEED_EDGES)
merged_data = pd.merge(merged_data, df_SPEED_EDGES, on=['u', 'v',
                                                         'index', 'idtrajectory', 'idtrace', 'sequenza',
                                                         'mean_speed', 'timedate', 'totalseconds', 'TRIP_ID',
                                                         'track_ID', 'length', 'highway', 'name', 'ref',
                                                         'length(km)','travelled speed (km/h)',
                                                         'travel time (sec)'], how = 'inner')
merged_data.drop(['color', 'geometry_y'], axis=1, inplace = True)
merged_data = merged_data.rename(columns={'geometry_x': 'geometry'})
## change names to be able to write on the DB
merged_data = merged_data.rename(columns={'length(km)': 'length_km'})
merged_data = merged_data.rename(columns={'travelled speed (km/h)': 'travelled speed_km_h'})
merged_data = merged_data.rename(columns={'travel time (sec)': 'travel_time_secs'})
merged_data = merged_data.rename(columns={'frequency(%)': 'frequency'})
merged_data = gpd.GeoDataFrame(merged_data)

#############################################################################################
# create basemap
ave_LAT = 37.53988692816245
ave_LON = 15.044971594798902
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
#############################################################################################


# add colors to map
my_map = plot_graph_folium_FK(SPEED_EDGES, graph_map=None, popup_attribute=None,
                              zoom=1, fit_bounds=True, edge_width=2, edge_opacity=1)
style = {'fillColor': '#00000000', 'color': '#00000000'}
# add 'u' and 'v' as highligths for each edge (in blue)
folium.GeoJson(
    # data to plot
    SPEED_EDGES[['travel time (sec)', 'travelled speed (km/h)', 'length(km)', 'geometry']].to_json(),
    show=True,
    style_function=lambda x:style,
    highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['travel time (sec)', 'travelled speed (km/h)', 'length(km)']
    ),
).add_to(my_map)
folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)

SPEED_EDGES.to_file(filename='DB_SPEED_EDGES.geojson', driver='GeoJSON')
my_map.save("clean_matched_route_travel_speed_all_EDGES_2019-04-15_May-26-2020.html")


#########################################################
### insert into the DB  #################################
#########################################################

### Connect to a DB and populate the DB  ###
# connection = engine.connect()
# merged_data['geom'] = merged_data['geometry'].apply(wkb_hexer)
# merged_data.drop('geometry', 1, inplace=True)
# ## copy into the DB
# merged_data.to_sql("paths_postprocess_temp", con=connection, schema="public")
# connection.close()

## drop one column
cur_HAIG.execute("""
ALTER TABLE "paths_postprocess_temp" DROP "level_0"
     """)
conn_HAIG.commit()

conn_HAIG.close()
cur_HAIG.close()


'''
# copy temporary table to a permanent table with the right GEOMETRY datatype
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')
with engine.connect() as conn, conn.begin():
    sql = """create table paths_postprocess as (select * from paths_postprocess_temp)"""
    conn.execute(sql)

# Convert the `'geom'` column back to Geometry datatype, from text
with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE public.paths_postprocess
                                  ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
                                    USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)

'''
