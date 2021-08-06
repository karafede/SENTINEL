
#### from Gaetano Valenti
#### modified by Federico Karagulian

import os
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
import kmeans_clusters
import dbscan_clusters
import db_connect


## Connect to an existing database
conn_HAIG = db_connect.connect_HAIG_SALERNO()
cur_HAIG = conn_HAIG.cursor()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_SALERNO')


timedate = []
timeins = []
longitude =[]
latitude =[]
panel =[]
deltaT=[]
idTerminale=[]
sep=","
#Query to create db_residencePY
cur_HAIG.execute("DROP TABLE IF EXISTS public.residenze CASCADE")
cur_HAIG.execute("CREATE  TABLE public.residenze "
"(id bigserial primary key, "
" idTerm integer  NOT NULL, "
" n_points smallint NOT NULL,"
" avgparkingtime_s integer  NOT NULL,"
" residence integer DEFAULT NULL, "
" geom geometry(Point,4326))");


#Query the database to obtain the list of idterm
cur_HAIG.execute("SELECT idterm FROM idterm_portata order by idterm;")
records = cur_HAIG.fetchall()
for row in records:
    idTerminale.append(str(row[0]))

for idTerm in idTerminale:
    # Query the database and obtain data as Python objects
    cur_HAIG.execute("SELECT ST_X(ST_Transform(dt_d.geom, 32632)), ST_Y(ST_Transform(dt_d.geom, 32632)), route.breaktime_s "
                "FROM route "
                "inner join dataraw as dt_d on dt_d.id=route.idtrace_d "
                "where route.idterm="+idTerm+" and route.breaktime_s>10*60 "
                "order by dt_d.timedate; ")
    records = cur_HAIG.fetchall()
    if(len(records)<3): continue
    lon=[]
    lat=[]
    parkingTime=[]
    for row in records:
        lon.append(row[0])
        lat.append(row[1])
        parkingTime.append(float(row[2]))

    labels=dbscan_clusters.dbscan(lon,lat,120.0)
    labels_bis = list( dict.fromkeys(labels) ) # labels non duplicati
    for lab in labels_bis:
        indices=[index for index, value in enumerate(labels) if value == lab]
        x = [lon[i] for i in indices]
        y = [lat[i] for i in indices]
        pTime= [parkingTime[i] for i in indices]
        frequenza=len(x)
        xm= sum(x)/float(frequenza)
        ym= sum(y)/float(frequenza)
        ptm=int(float(sum(pTime))/float(len(pTime)))
        print(idTerm, xm, ym, frequenza, ptm)
        input = "(" + str(idTerm) + sep + str(frequenza) + sep + str(ptm)+ sep + "ST_Transform(st_setsrid(st_makepoint(" + str(xm) + "," + str(ym) + "), 32632), 4326))";
        cur_HAIG.execute("INSERT INTO public.residenze (idTerm, n_points, avgparkingtime_s, geom)" + " VALUES " + input + "");
cur_HAIG.execute("CREATE INDEX residenze_idterm ON public.residenze USING btree (idTerm);");

'''    
    labels=kmeans_clusters.kmeans(lon,lat, len(records))
    labels_bis = list( dict.fromkeys(labels) ) # labels non duplicati
    for lab in labels_bis:
        indices=[index for index, value in enumerate(labels) if value == lab]
        x = [lon[i] for i in indices]
        y = [lat[i] for i in indices]
        pTime= [parkingTime[i] for i in indices]
        frequenza=len(x)
        xm= sum(x)/frequenza
        ym= sum(y)/frequenza
        ptm=int(sum(pTime)/len(pTime))
        print("KMEANS",idTerm, xm, ym, frequenza, ptm)
'''

# Make the changes to the database persistent
conn_HAIG.commit()
# Close communication with the database
cur_HAIG.close()
conn_HAIG.close()
