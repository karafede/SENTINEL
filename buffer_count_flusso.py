
import os
os.getcwd()
os.chdir('C:/ENEA_CAS_WORK/SENTINEL/viasat_data')

import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm
# import projections as pj
import csv
#import matplotlib as plt
import matplotlib.pyplot as plt
import psycopg2
import db_connect

# ox.config(log_console=True, use_cache=True)

conn=db_connect.connect_viasat()
cur = conn.cursor()

# erase existing table
cur.execute("DROP TABLE IF EXISTS buffer_10m_catania CASCADE")
cur.execute("DROP TABLE IF EXISTS vehicles_in_edges CASCADE")
cur.execute("DROP TABLE IF EXISTS vehicles_counts_in_edges CASCADE")
cur.execute("DROP TABLE IF EXISTS averages_vehicles_counts CASCADE")
conn.commit()

# read OSM file
G = ox.load_graphml('network_Catania_20km_epgs4326.graphml')
ox.plot_graph(G)
edge_file = G.edges(keys=True,data=True)

# create two virtual portals at the position START and END
start=(37.5781510,  14.9779825)
end=(37.55734, 14.980967)
# portale_1_node = ox.get_nearest_node(G, start, return_dist=True)
# portale_2_node = ox.get_nearest_node(G, end, return_dist=True)
portale_1_node = ox.get_nearest_node(G, start)
portale_2_node = ox.get_nearest_node(G, end)
print(portale_1_node, portale_2_node)

# route between the two Portal_nodes (the edge)
route = nx.shortest_path(G, portale_1_node, portale_2_node, weight='cost')
ox.plot_graph_route(G, route, route_color='blue', fig_height=12, fig_width=12)
path_edges = list(zip(route, route[1:])) # only two, there are 2 portals
print(path_edges)
# Out[27]: [(383494726, 383494566)]
path = ox.plot_route_folium(G, route, route_color='green')
path.save('Catania_selected_path.html')

gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)
# path = nx.shortest_path(G, G.nodes()[0], G.nodes()[1])
# gdf_nodes.loc[path]
# check projections and geometry
nodes_pooj = gdf_nodes.crs
edges_proj = gdf_edges.crs

# identifications of nodes
print(G[383494726][383494566])
# {0: {'osmid': [69597193, 33571658], 'maxspeed': '50', 'lanes': '2', 'highway': 'secondary',
# 'name': ['Strada Provinciale 14', 'Via Carlo Marx'], 'oneway': False, 'length': 2927.566999999998,
# 'ref': 'SP14', 'geometry': <shapely.geometry.linestring.LineString object at 0x0000025BCE820860>}}

# G.node[383494726]
print(G[383494726])
print(G[383494566])


# How many points near two edges?
# add geometry to viasat data in Catania

'''
cur.execute("""
alter table sequence_catania add column geom geometry(POINT,4326)
""")

cur.execute("""
update sequence_catania set geom = st_setsrid(st_point(longitude,latitude),4326)
""")
conn.commit()
'''

# create buffers around each edge with a diameter of about 10 meter
# units are in km (so 10m is 0.0001 degrees)
# this is a polygon now
cur.execute(" CREATE TABLE buffer_10m_catania AS"
            " SELECT ST_Buffer(geom, 0.0001) AS the_geom, osmid, name"
            " FROM edges")
conn.commit()
# 7209 rows same as the edges

# get vehicles within the buffer polygons (edges)
cur.execute("""
            CREATE TABLE vehicles_in_edges AS
            (SELECT sc.geom, sc.datetime, sc.deviceid, sc.idrequest, sc.speedkmh, sc.heading, bc.osmid, bc.name
             FROM
                 buffer_10m_catania as bc,
                 viasat_py_temp as sc 
             WHERE 
             ST_Intersects(sc.geom, bc.the_geom))
             """)
conn.commit()


# count the vehicles within the buffer polygon (osmid)
cur.execute("""
            CREATE TABLE vehicles_counts_in_edges AS
            (SELECT buffer_10m_catania.osmid, buffer_10m_catania.name, count(vehicles_in_edges.idrequest) AS totale 
            FROM buffer_10m_catania LEFT JOIN vehicles_in_edges 
            ON st_contains(buffer_10m_catania.the_geom, vehicles_in_edges.geom) 
            GROUP BY buffer_10m_catania.osmid, buffer_10m_catania.name)
            """)
conn.commit()
# Select all rows
cur.execute("SELECT * FROM public.vehicles_counts_in_edges")
table=pd.DataFrame(cur.fetchall())

print(max(table[2]))
print(len(table)) # 5220 (edge tabe has ~ 7200 rows

# show only non null results
table = table[table[2] > 0]
print(table.shape)

# assign column names
table.columns = ['osmid', 'name', 'vehicles_count']
table.head()

# select the osmid we want to consider (we find it through the 2 extreme nodes)
print(G[383494726][383494566])
# osmid': [69597193, 33571658]
# number of car in a given edge
print(table.loc[table['osmid'] == '[69597193, 33571658]'])
print(table.loc[table['name'] == 'Strada Provinciale 14'])


# average counts within 5 minutes
cur.execute("""
           CREATE TABLE averages_vehicles_counts AS(
           SELECT *
           FROM(
           SELECT osmid, name, generate_series(min(datetime), max(datetime), interval '5 min') AS ts
                  FROM   vehicles_in_edges
                  /* WHERE  deviceid = '4363297' */
                  WHERE    datetime >= '2019-04-11'
                  /* AND    datetime <  '2018-01-02' */
                  GROUP  BY osmid, name
           ) grid
   
           CROSS  JOIN LATERAL (
           SELECT count(idrequest) AS vei_counts,
                  round(avg(speedkmh), 2) AS speed_avg,
                  round(avg(heading), 2) AS heading_avg
           FROM   vehicles_in_edges
           /* WHERE  deviceid =  grid.deviceid */
                  WHERE    datetime >= grid.ts
                  AND    datetime <  grid.ts + interval '5 min'
           ) avg);
           """)
conn.commit()

# 5/60 = 0.0833 (5 min/ 60 min)
# add flow by hour....
cur.execute("""ALTER TABLE averages_vehicles_counts add column flow_by_hour float""")
cur.execute("""UPDATE averages_vehicles_counts SET flow_by_hour = vei_counts/0.0833""")
conn.commit()


# Select all rows
cur.execute("SELECT * FROM public.averages_vehicles_counts")
table=pd.DataFrame(cur.fetchall())
# assign column names
table.columns = ['osmid', 'name', 'ts', 'vei_counts', 'speed_avg', 'heading_avg', 'flow_by_hour']


conn.close()
cur.close()


# assign geometry to lat lon coordinates
# SELECT ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) FROM sequence_catania
# SELECT ST_MakePoint(sequence_catania.longitude, sequence_catania.latitude) FROM sequence_catania

# cur.execute("WITH sequences_with_geom AS( "
#             " SELECT idrequest, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) as geom FROM sequence_catania )"
#             " SELECT osmid, count(idrequest) FROM edges c, sequences_with_geom s WHERE ST_Within(s.geom, c.geom) "
#             " GROUP BY c.osmid")
# table=pd.DataFrame(cur.fetchall())
