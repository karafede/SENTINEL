
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

# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

# ## load grafo
# file_graphml = 'Fisciano__Italy_50km.graphml'
# grafo_ALL = ox.load_graphml(file_graphml)
# ## ox.plot_graph(grafo_ALL)
# # gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)
# gdf_nodes_ALL, gdf_edges_ALL = ox.graph_to_gdfs(grafo_ALL)

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()


# from datetime import datetime
# now1 = datetime.now()
#
# ### get TRIPS from SENTINEL edge ####
# df =  pd.read_sql_query('''
#                      SELECT
#                             split_part("TRIP_ID"::TEXT,'_', 1) idterm,
#                             u, v, "TRIP_ID"
#                             FROM mapmatching_2019
#                             WHERE (u, v) in (VALUES (25844050, 1110091861),
#                            (1110091904, 3371747395))
#                             /*LIMIT 1000*/
#                                  ''', conn_HAIG)
#
# now2 = datetime.now()
# print(now2 - now1)
#
# ## save csv file
# df.to_csv('all_trips_on_SENTINEL_edged.csv')


### function to get consecutive edges...
def consec_sort(lst):
    def key(x):
        nonlocal index
        if index <= lower_index:
            index += 1
            return -1
        return abs(x[0] - lst[index - 1][1])
    for lower_index in range(len(lst) - 2):
        index = 0
        lst = sorted(lst, key=key)
    return lst


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

# Create an SQL connection engine to the output DB
# engine = sal.create_engine('postgresql://postgres:vaxcrio1@localhost:5432/HAIG_Viasat_CT')
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_SA')

all_EDGES = pd.DataFrame([])
all_ACCURACY = []     # ratio: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100

# get all ID terminal of Viasat data
# all_VIASAT_IDterminals = pd.read_sql_query(
#    ''' SELECT "idterm"
#        FROM public.routecheck_2019''', conn_HAIG)



# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
# all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())

with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_ID_TRACKS_2019.txt", "r") as file:
     all_ID_TRACKS = eval(file.readline())

 ## reload 'idterms_fleet' as list
with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/idterms_fleet.txt", "r") as file:
 idterms_fleet = eval(file.readline())

#####---------------------------######
### selected track_ID and trips ######
######################################

track_ID = '3507623'  ## GOOD
trip = '3507623_37'


track_ID = '2401121'  ## NI
trip = '2401121_206'



track_ID = '6761098'   ## OK
trip = '6761098_19'



track_ID = '4309918'  ## OK GOOD
trip = '4309918_7'



track_ID = '3546456'  ## GOOD
trip = '3546456_conc_0'


track_ID = '4496100'  ## too long distance between two adjacent points
trip = '4496100_conc_7'



track_ID = '3218018'  ## OK
trip = '3218018_0'



track_ID = '3503635'  ## OK
trip = '3503635_0'


track_ID = '3539493'  ## GOOD
trip = '3539493_81'


track_ID = '4400602'  ## OK (urban in Naples)
trip = '4400602_conc_2'


track_ID = '3526845'   ## NI
trip = '3526845_conc_3'


track_ID = '5930473'  ## OK (buffer = 0.00004)
trip = '5930473_conc_4'


track_ID = '3525430' ## GOOD  (buffer = 0.00005)
trip = '3525430_conc_1'


track_ID = '4276008'  ## OK (NAPOLI SCAMPIA)
trip = '4276008_1'


track_ID = '4403808'  # NO
trip = '4403808_conc_2'


track_ID = '5424271'  ## OK
trip = '5424271_conc_1'


track_ID = '4461387'   ## very GOOD
trip = '4461387_conc_9'


track_ID = '4461387'   ## very GOOD  (buffer 0.00012)
trip = '4461387_conc_15'


track_ID = '5423624'   ## GOOD
trip = '5423624_conc_65'



track_ID = '3512129'   ## GODD
trip = '3512129_conc_70'


track_ID = '4096880'   ## very GOOD (tunnel considered)
trip = '4096880_conc_3'


track_ID = '3533803'   ## GOOD A2" Autostrade del Mediterraneo
trip = '3533803_conc_2'


track_ID = '3540565'   ## NOT GOOD A" Autostrade del Mediterraneo
trip = '3540565_conc_5'


track_ID = '4216672'   ## GOOD A2" Autostrade del Mediterraneo
trip = '4216672_5'


track_ID = '3505721'   ## long trip on the A2 Autostrada del Mediterraneo GOOD
trip = '3505721_0'


# track_ID = '4362477'  #
# trip = '4362477_conc_69'


################################################################################
# create basemap SALERNO (Fisciano, localita' Penta)
ave_LAT = 40.760773
ave_LON = 14.788383
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
# folium.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
#                 attr="Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community").add_to(my_map)
################################################################################

## read each TRIP from each idterm (TRACK_ID or idtrajectory)
viasat_data = pd.read_sql_query('''
              SELECT * FROM public.routecheck_2019 
              WHERE "idterm" = '%s' ''' % track_ID, conn_HAIG)


### FILTERING #############################################
# viasat_data = viasat_data[viasat_data.anomaly != "IQc345d"]
viasat_data = viasat_data[viasat_data.anomaly != "EQc3456"]
viasat_data = viasat_data[viasat_data.anomaly != "EQc3T5d"]
if int(track_ID) not in idterms_fleet:
    print("+++++++ vehtype = car ++++++++++++++++")
    viasat_data = viasat_data[viasat_data.anomaly != "IQ2C4V6"]
    TRIP_ID = trip
    viasat = viasat_data[viasat_data.TRIP_ID == trip]

    ## sort by 'time and date'
    viasat = viasat.sort_values('timedate')
    viasat.reset_index(drop=True, inplace=True)

    if len(viasat) > 5:
        ## introduce a dynamic buffer (optional)
        dx = max(viasat.longitude) - min(viasat.longitude)
        dy = max(viasat.latitude) - min(viasat.latitude)
        if dx < 0.007:
            buffer_diam = 0.00020
        else:
            buffer_diam = 0.00005

        buffer_diam = 0.00008   ### best choice...so far 0.00008
        buffer_diam_pic = 0.00020   ## this is the diamteter to show on the pics
        # buffer_diam_pic = 0.00004  ## this is the diamteter to show on the pics

        ## get extent of viasat data
        ext = 0.030
        ## top-right corner
        p1 = Point(np.min(viasat.longitude)-ext, np.min(viasat.latitude)-ext)
        ## bottom-right corner
        p2 = Point(np.max(viasat.longitude)+ext,np.min(viasat.latitude)-ext)
        ## bottom-left corner
        p3 = Point(np.max(viasat.longitude)+ext, np.max(viasat.latitude)+ext)
        ## top-left corner
        p4 = Point(np.min(viasat.longitude)-ext,np.max(viasat.latitude)+ext)

        # Initialize a test GeoDataFrame where geometry is a list of points
        viasat_extent = gpd.GeoDataFrame([['box', p1],
                               ['box', p2],
                               ['box', p3],
                               ['box', p4]],
                             columns = ['shape_id', 'geometry'],
                             geometry='geometry')

        # Extract the coordinates from the Point object
        viasat_extent['geometry'] = viasat_extent['geometry'].apply(lambda x: x.coords[0])
        # Group by shape ID
        #  1. Get all of the coordinates for that ID as a list
        #  2. Convert that list to a Polygon
        viasat_extent = viasat_extent.groupby('shape_id')['geometry'].apply(lambda x: Polygon(x.tolist())).reset_index()
        # Declare the result as a new a GeoDataFrame
        viasat_extent = gpd.GeoDataFrame(viasat_extent, geometry = 'geometry')
        # viasat_extent.plot()

        ## get graph only within the extension of the rectangular polygon
        ## filter some features from the OSM graph
        filter = (
            '["highway"!~"living_street|abandoned|steps|construction|service|pedestrian|'
            'bus_guideway|bridleway|corridor|escape|rest_area|track|sidewalk|proposed|path|footway"]')
        grafo = ox.graph_from_polygon(viasat_extent.geometry[0], custom_filter=filter)
        ## ox.plot_graph(grafo)

        # for polygon in viasat_extent['geometry']:
        #     intersecting_nodes = gdf_nodes_ALL[gdf_nodes_ALL.intersects(polygon)].index
        #     grafo = grafo_ALL.subgraph(intersecting_nodes)
        #     # ox.plot_graph(grafo)
        #     # fig, ax = ox.plot_graph(grafo)
        #     ## get geodataframes for edges and nodes from the subgraph
        # if len(grafo) > 0:
        #     gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

        os.chdir('D:\\ENEA_CAS_WORK\\SENTINEL\\viasat_data')
        ox.save_graphml(grafo, filename='partial.graphml')
        file_graphml = 'partial.graphml'
        from funcs_network_FK import cost_assignment
        place_country = "Salerno, Italy"
        # cost_assignment(file_graphml, place_country)

        ## make a geo-dataframe from the grapho
        gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

        # reset indices
        viasat.reset_index(drop=True, inplace=True)

        # create an index column
        viasat["ID"] = viasat.index
        ## sum of progressive distance (true distance travelled by the vehicle)
        diff_progressive = viasat.progressive.diff()
        diff_progressive = diff_progressive.dropna()
        sum_progressive = sum(diff_progressive)  ## in meters (these are increments)

        ######################################################

        # add VIASAT GPS track on the base map (defined above)
        for i in range(len(viasat)):
            folium.CircleMarker(location=[viasat.latitude.iloc[i], viasat.longitude.iloc[i]],
                                popup= (TRIP_ID + '_' + str(viasat.ID.iloc[i])),
                                radius= 4,
                                color="black",
                                fill=True,
                                fill_color="black",
                                fill_opacity=1).add_to(my_map)
        # my_map.save("matched_route_VIASAT_" + DATE + '_' + today + ".html")
        # my_map.save("matched_route_VIASAT_" + trip + ".html")

        ######################################################

        # build a geodataframe with VIASAT data
        geometry = [Point(xy) for xy in zip(viasat.longitude, viasat.latitude)]
        # viasat = viasat.drop(['longitude', 'latitude'], axis=1)
        crs = {'init': 'epsg:4326'}
        viasat_gdf = GeoDataFrame(viasat, crs=crs, geometry=geometry)
        # viasat_gdf.plot()

        # Buffer the points by some units (unit is kilometer)
        buffer = viasat_gdf.buffer(buffer_diam)
        buffer_pic = viasat_gdf.buffer(buffer_diam_pic)
        # buffer.plot()
        # make a dataframe and assign 'geometry'
        buffer_viasat = pd.DataFrame(buffer)
        buffer_viasat.columns = ['geometry']
        buffer_viasat_pic = pd.DataFrame(buffer_pic)
        buffer_viasat_pic.columns = ['geometry']

        # geodataframe with edges
        type(gdf_edges)
        # gdf_edges.plot()

        # add buffered viasat polygons
        # save first as geojson file
        buffer_pic.to_file(filename='buffer_viasat.geojson', driver='GeoJSON')
        viasat_extent.to_file(filename='viasat_extent.geojson', driver='GeoJSON')
        # folium.GeoJson('buffer_viasat.geojson').add_to((my_map))
        # folium.GeoJson('viasat_extent.geojson').add_to((my_map))
        # change directory
        os.chdir('D:\\ENEA_CAS_WORK\\SENTINEL\\viasat_data')
        my_map.save("matched_route_with_buffer.html")

        # 1 = 100 km
        # 0.1 = 10 km
        # 0.01 = 1 km
        # 0.001 = 100m
        # 0.0001 = 10m
        # 0.00001 = 1m

        ############################################################
        ###====================================================#####
        ############################################################

        from datetime import datetime

        ## find all edges intersect by the buffers defined above
        ## and get distance between Viasat measurement and edge
        buff = []
        index_edges = []
        index_nodes = []
        index_buff = []
        edge = []
        DISTANCES = []
        now1 = datetime.now()
        DISTANCES_dict = {}

        for streets in gdf_edges.itertuples(index=True):
            for via_buff in buffer_viasat.itertuples(index=True):
                if streets.geometry.intersects(via_buff.geometry) is True:
                    print("OK=======================OK")
                    index_edges.append(streets.Index)
                    index_buff.append(via_buff.Index)
                    STREET = streets.u, streets.v, via_buff.Index
                    # get distance between Viasat measurement and edge (only the part intesecting the buffer)
                    distance = (Point(viasat[['longitude', 'latitude']].iloc[via_buff.Index]).distance(streets.geometry))*100000 # roughly meter conversion
                    print("distance track-edge: ", distance, " meters")
                    edge.append(STREET)
                    dist = distance
                    distance = distance, via_buff.Index
                    DISTANCES.append(distance)
                    DISTANCES_dict[streets.u] = dist
                    # list all buffers in sequence
                    # buff.append(via_buff.name)
        now2 = datetime.now()
        print(now2 - now1)


        ############################################################
        ###====================================================#####
        ############################################################

        ## filter gdf_edges based on index_edges (edges in the buffer)
        ## near neighbour edges (near the viasat measurements)
        nn_gdf_edges = gdf_edges[gdf_edges.index.isin(index_edges)]
        ## plot selects edges
        # nn_gdf_edges.plot()
        nn_gdf_edges = nn_gdf_edges[['geometry']]
        nn_gdf_edges.to_file(filename='nn_gdf_edges.geojson', driver='GeoJSON')
        # folium.GeoJson('nn_gdf_edges.geojson').add_to((my_map))
        my_map.save("matched_route_with_buffer.html")


        '''
        #############################
        # plot in a Folium map ######
        #############################
        
        # make main map "my_map"
        place_country = "Fisciano, Italy"
        road_type = ['motorway', 'motorway_link', 'secondary', 'primary', 'tertiary', 'residential', 
                     'unclassified', 'trunk', 'trunk_link', 'tertiary_link', 'secondary_link', 'service']
        ## this is a partial grapho
        grafo = ox.load_graphml(file_graphml)
        # ## ox.plot_graph(grafo)
        my_map = roads_type_folium(file_graphml, road_type, place_country)
        
        # add neighbour edges crossed by the buffer of the viasat data
        points = []
        for i in range(len(nn_gdf_edges)):
            nn_edges = ox.make_folium_polyline(edge=nn_gdf_edges.iloc[i], edge_color="blue", edge_width=3,
                                                    edge_opacity=1, popup_attribute=None)
            points.append(nn_edges.locations)
        folium.PolyLine(points, color="black", weight=2, opacity=1).add_to(my_map)
        
        # add buffered viasat polygons
        # save first as geojson file
        buffer_pic.to_file(filename='buffer_viasat.geojson', driver='GeoJSON')
        # folium.GeoJson('buffer_viasat.geojson').add_to((my_map))

        # add VIASAT GPS track on the base map (defined above)
        for i in range(len(viasat)):
            folium.CircleMarker(location=[viasat.latitude.iloc[i], viasat.longitude.iloc[i]],
                                popup=(TRIP_ID + '_' + str(viasat.ID.iloc[i])),
                                radius=1,
                                color="black",
                                fill=True,
                                fill_color="black",
                                fill_opacity=1).add_to(my_map)

        ## add buffers
        folium.GeoJson('buffer_viasat.geojson').add_to((my_map))
        ## add intersected edges
        folium.GeoJson('nn_gdf_edges.geojson').add_to((my_map))
        my_map.save("near_neighbours_SALERNO.html")
        '''


        ##########################################################
        ########## VALHALL ALGORITHM MAP MATCHING  ###############
        ##########################################################

        from math import radians, cos, sin, asin, sqrt

        # sort edges and associated buffer
        if len(edge) > 0:
            df_edges = pd.DataFrame(edge)
            df_edges.columns = ['u', 'v', 'buffer_ID']
            df_edges.sort_values(by=['buffer_ID'], inplace=True)
            ## count the number of edges in each buffer
            COUNTS_buffer = df_edges.groupby(df_edges[['buffer_ID']].columns.tolist(), sort=False).size().reset_index().rename(
                 columns={0: 'counts'})
            buffer_to_remove = list(((COUNTS_buffer[COUNTS_buffer.counts >= 4]).buffer_ID))
            df_edges = df_edges[~df_edges.buffer_ID.isin(buffer_to_remove)]

            ## merge "df_edges" with "viasat" to get the "id"
            EDGES = df_edges
            EDGES = EDGES.rename(columns={'buffer_ID': 'ID'})
            KKK = pd.merge(EDGES, viasat, on=['ID'],how='left')

            # sort df by u and v
            # df_edges.sort_values(['u','v'],ascending=False, inplace=True)

            # make a dictionary: for each buffer/track/measurement (key) assign u and v
            ID_TRACK = list(df_edges.buffer_ID.unique())
            df_edges_dict = {}
            keys = ID_TRACK
            for track in keys:
                    df_edges_dict[track] = df_edges[['u', 'v']][df_edges['buffer_ID']==track ].values.tolist()
            print(df_edges_dict)


            # nodes associated to tracks
            nodes_u = list(df_edges.u.unique())
            u_dict = {}
            keys = nodes_u
            for u in keys:
                    u_dict[u] = df_edges[df_edges['u']==u].values.tolist()
            print(u_dict)

            nodes_v = list(df_edges.v.unique())
            v_dict = {}
            keys = nodes_v
            for v in keys:
                    v_dict[v] = df_edges[df_edges['v']==v].values.tolist()
            print(v_dict)

            # join two dictionaries
            nodes_dict = {**u_dict, **v_dict}

            # define distance between GPS track (viasat measurements) and node
            def great_circle_track_node(u):
                """
                Calculate the great circle distance between two points
                on the earth (specified in decimal degrees)
                """
                # u_track = u_dict.get(u)[0][2]
                u_track = nodes_dict.get(u)[0][2]
                coords_track = viasat[viasat.ID == u_track].values.tolist()
                lon_track = coords_track[0][2]
                lat_track = coords_track[0][3]
                coords_u = gdf_nodes[gdf_nodes.index == u][['x', 'y']].values.tolist()
                lon_u = coords_u[0][0]
                lat_u = coords_u[0][1]
                # convert decimal degrees to radians
                lon1, lat1, lon2, lat2 = map(radians, [lon_track, lat_track, lon_u, lat_u])

                # haversine formula
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * asin(sqrt(a))
                r = 6371 # Radius of earth in kilometers. Use 3956 for miles
                return c * r # Kilometers


            # define distance between two GPS tracks (viasat measurements)
            def great_circle_track(u):
                # Calculate the great circle distance between two points from viasat data (progressive)
                u_track = nodes_dict.get(u)[0][2]
                v_track = u_track+1
                if v_track <= max(viasat.ID):
                    distance =int((viasat[viasat['ID'] == v_track]).progressive) - int((viasat[viasat['ID'] == u_track]).progressive)
                    distance = distance/1000 # in Km
                else:
                    distance = 0
                return distance


            # "sigma" has been calculated ad the standard deviation of all the distances between viasat measurements and nodes
            # SIGMA_Z = 1.4826*np.median(DISTANCES) # meters
            SIGMA_Z = 1.4826*np.median([x[0] for x in DISTANCES]) # meters
            # SIGMA_Z = SIGMA_Z/1000 # Kilometers
            print(SIGMA_Z)


            ###############################
            ### emission probability ######
            ###############################

            # Gaussian distribution of all NODES close to Viasat measurements.
            def emission_prob(u):
                c = 1 / (SIGMA_Z * math.sqrt(2 * math.pi))
                return 1 * math.exp(-0.5*(great_circle_track_node(u)/SIGMA_Z)**2)
                # return great_circle_track_node(u)/SIGMA_Z


            prob = []
            # for u in u_dict:
            for u in nodes_dict:
                emiss_prob = emission_prob(u)
                prob.append(emiss_prob)
                # print(prob)
            print("max_probability: ", max(prob))
            print("min_probability: ", min(prob))


            #################################
            ### Transition probability ######
            #################################

            # transition probability (probability that the distance u-->v is from the mesasurements's distances at nodes u and v
            def transition_prob(u, v):
                BETA = 1
                c = 1 / BETA
                # Calculating route distance is expensive.
                # We will discuss how to reduce the number of calls to this function later.
                # distance on the route
                delta = abs(nx.shortest_path_length(grafo, u, v, weight='length')/1000 -
                            great_circle_track(u))  # in Kilometers
                shortest_path = nx.shortest_path_length(grafo, u, v, weight='length')/1000
                # print('## u:',u, '## v:', v, '## delta = direct_distance - shortest distance (km):', delta, '## shortest_path (km):',shortest_path)
                # print('## u:', u, '## v:', v, '## shortest_path (km):', shortest_path)
                return c * math.exp(-delta)


            # calculate BETA
            deltaB = []
            # for u in u_dict:
            #     for v in [item[1] for item in u_dict.get(u)]:
            for u in nodes_dict:
                for v in [item[1] for item in nodes_dict.get(u)]:
                    LEN_ROUTE = nx.shortest_path_length(grafo, u, v, weight='length') / 1000  # in Km
                    # print("Len_Route", LEN_ROUTE)  # in Km
                    # distance on the sphere (cartesian distance)
                    DIST = great_circle_track(u)  # in Km
                    if DIST != None:
                        delta = abs(DIST - LEN_ROUTE)
                        # print(DIST, "=============================")  # in Km
                        # print("DELTA: ", delta)  # in Km
                        deltaB.append(delta)
                BETA = (1 / math.log(2)) * np.median(deltaB)
                # print("BETA: ", BETA)


            trans_prob = []
            # for u in u_dict:
            #     for v, track in [(item[1], item[2]) for item in u_dict.get(u)]:
            for u in nodes_dict:
                for v, track in [(item[1], item[2]) for item in nodes_dict.get(u)]:
                    # print(v)
                    print('track:', track)
                    t_prob = transition_prob(u, v)
                    # print(t_prob)
                    trans_prob.append(t_prob)
            print("## max_transition_prob: ", max(trans_prob))
            print("## min_transition_prob: ", min(trans_prob))


            ####################################################
            ####################################################

            # define the adjaceny list
            adjacency_list = {}
            for key in df_edges_dict.keys():
                print(key)
                track = df_edges_dict.get(key)
                print(track)
                unique_list = set(x for l in track for x in l)
                adjacency_list[key] = unique_list

            # if two lists of the adjacency list are identical, then take only the last one...
            result = {}
            for key,value in adjacency_list.items():
                if value not in result.values():
                    result[key] = value
            # if len(result) == 1:
                adjacency_list = result

            # get all keys names from the adjacency list
            from operator import itemgetter
            def getList(dict):
                return list(map(itemgetter(0), dict.items()))
            track_list = getList(adjacency_list)

            # exception...
            if len(track_list)==2:
                track_list = [1]

            #######################
            ### MAP MATCHING ######
            #######################

            # Inititate empty dictionaries to store distances between points and times (secs) between points
            distance_between_points = {}
            speed_between_points = {}
            time_track = {}
            HOUR_track = {}
            timedate_track = {}

            if len(track_list) > 1:
                max_prob_node = []
                for i in range(len(track_list)):
                    print(track_list[i])
                    if track_list[i] == max(track_list):
                        break

                    trans_prob = {}
                    emiss_prob = {}
                    shortest_path = {}
                    SHORT_PATH = []

                    # i = 0
                    for u in adjacency_list[track_list[i]]:
                        for v in adjacency_list[track_list[i+1]]:
                            print(u,v)
                            # distance travelled from one point to the next one (in km)
                            distance_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).progressive) - int(
                                (viasat[viasat['ID'] == track_list[i]]).progressive)
                            distance_VIASAT = abs(distance_VIASAT) / 1000  # in Km
                            # add distance to a dictionary in function of edge "u"
                            distance_between_points[u] = distance_VIASAT
                            # time spent to travel from one point to te next one (in seconds)
                            time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                (viasat[viasat['ID'] == track_list[i]]).path_time)
                            # add time to a dictionary in function of edge "u"
                            time_track[u] = time_VIASAT
                            HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                            timedate_track[u] = ((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]

                            # mean speed between two points (tracks)
                            if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                            elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                            else:
                                speed_VIASAT = (int(
                                    (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                    (viasat[viasat['ID'] == track_list[i]]).speed)) / 2

                            # add speed to a dictionary in function of edge "u"
                            speed_between_points[u] = speed_VIASAT
                            # print(u, v, distance_VIASAT)
                            if u != v:
                                print(u,v)
                            try:
                                if u != v:
                                    shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                    short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                    SHORT_PATH.append(short_path)
                                    if shortest_path[u] <= distance_VIASAT:
                                        print('===== KEEP ========')
                                        print('#u:', u, '#v:', v, 'shortest_path:',
                                              nx.shortest_path_length(grafo, u, v, weight='length') / 1000, 'distance_VIASAT:', distance_VIASAT)
                                        trans_prob[u] = transition_prob(u, v)
                                        emiss_prob[u] = emission_prob(u)
                            except nx.NetworkXNoPath:
                                     print('No path', 'u:', u, 'v:', v, )
                    if len(trans_prob) != 0:
                        MAX_trans_key = max(trans_prob, key=trans_prob.get)
                        # MAX_emiss_key = min(emiss_prob, key=emiss_prob.get)
                        MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
                        MAX_trans_value = trans_prob.get(MAX_trans_key)
                        MAX_emiss_value = emiss_prob.get(MAX_emiss_key)
                    else:
                        MAX_trans_key = 0
                        MAX_trans_value = 0
                    if MAX_trans_value !=0:
                        # MAX_prob = max(MAX_trans_value, MAX_emiss_value)
                        print("max_prob_NODE:", MAX_trans_key)
                        max_prob_node.append(MAX_trans_key)


                        while MAX_trans_key not in adjacency_list[track_list[i + 1]]:
                            # if MAX_trans_key != MAX_emiss_key:
                            #     break
                            # do not execute this part of the code if we only have 3 GPS tracks
                            if 1 <= len(track_list) <= 3:
                                break
                            adjacency_list[track_list[i]].remove(MAX_trans_key)
                            max_prob_node.remove(MAX_trans_key)
                            # and start calculation again
                            trans_prob = {}
                            emiss_prob = {}
                            shortest_path = {}
                            SHORT_PATH = []

                            for u in adjacency_list[track_list[i]]:
                                for v in adjacency_list[track_list[i + 1]]:
                                    # distance travelled from one point to the next one (in km)
                                    distance_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).progressive) - int(
                                        (viasat[viasat['ID'] == track_list[i]]).progressive)
                                    distance_VIASAT = abs(distance_VIASAT) / 1000  # in Km
                                    # add distance to a dictionary in function of edge "u"
                                    distance_between_points[u] = distance_VIASAT
                                    # time spent to travel from one point to te next one (in seconds)
                                    time_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).path_time) - int(
                                        (viasat[viasat['ID'] == track_list[i]]).path_time)
                                    # add time to a dictionary in function of edge "u"
                                    time_track[u] = time_VIASAT
                                    HOUR_track[u] = int((viasat[viasat['ID'] == track_list[i]]).hour)
                                    timedate_track[u]=((viasat[viasat['ID'] == track_list[i]]).timedate).to_string()[4:23]

                                    # mean speed between two points (tracks)
                                    if int((viasat[viasat['ID'] == track_list[i + 1]]).speed) == 0:
                                        speed_VIASAT = int((viasat[viasat['ID'] == track_list[i]]).speed)
                                    elif int((viasat[viasat['ID'] == track_list[i]]).speed) == 0:
                                        speed_VIASAT = int((viasat[viasat['ID'] == track_list[i + 1]]).speed)
                                    else:
                                        speed_VIASAT = (int(
                                            (viasat[viasat['ID'] == track_list[i + 1]]).speed) + int(
                                            (viasat[viasat['ID'] == track_list[i]]).speed)) / 2

                                    # add speed to a dictionary in function of edge "u"
                                    speed_between_points[u] = speed_VIASAT
                                    # print(u, v, distance_VIASAT)
                                    if u != v:
                                        print(u, v)
                                    try:
                                        if u != v:
                                            shortest_path[u] = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                            short_path = nx.shortest_path_length(grafo, u, v, weight='length') / 1000
                                            SHORT_PATH.append(short_path)
                                            # print('#u:', u, '#v:', v, 'shortest_path:', nx.shortest_path_length(grafo, u, v, weight='length') / 1000)
                                            # check if the distance between 'track_list[i+1]' and 'track_list[i]' is less than shortest_path[u]
                                            if shortest_path[u] <= distance_VIASAT:
                                                print('===== KEEP ========')
                                                print('#u:', u, '#v:', v, 'shortest_path:',
                                                      nx.shortest_path_length(grafo, u, v, weight='length') / 1000, 'distance_VIASAT:', distance_VIASAT)
                                                trans_prob[u] = transition_prob(u, v)
                                                emiss_prob[u] = emission_prob(u)
                                    except nx.NetworkXNoPath:
                                        print('No path', 'u:', u, 'v:', v, )
                            if len(trans_prob) != 0:
                                MAX_trans_key = max(trans_prob, key=trans_prob.get)
                                MAX_emiss_key = max(emiss_prob, key=emiss_prob.get)
                                MAX_trans_value = trans_prob.get(MAX_trans_key)
                                MAX_emiss_value = emiss_prob.get(MAX_emiss_key)
                            if MAX_trans_value != 0:
                                print("max_prob_NODE:", MAX_trans_key)
                                # compare distance: node-GPS track with node-edge
                                if MAX_emiss_key in DISTANCES_dict.keys():
                                    # if MAX_trans_key != MAX_emiss_key and DISTANCES_dict[MAX_emiss_key] > emiss_prob[MAX_emiss_key]:
                                    #     max_prob_node.append(MAX_emiss_key)
                                    # else:
                                    max_prob_node.append(MAX_trans_key)
                                else:
                                    if MAX_trans_key != MAX_emiss_key:
                                        # max_prob_node.append(MAX_trans_key)
                                        ## distance between Max_key and the GPS track (in Km)
                                        lat = float(viasat.latitude[viasat.ID == track_list[i]])
                                        lon = float(viasat.longitude[viasat.ID == track_list[i]])
                                        distance_MAX_key_to_track = ox.great_circle_vec(lat1 = grafo.nodes[MAX_emiss_key]['y'],
                                                 lng1 = grafo.nodes[MAX_emiss_key]['x'],
                                                 lat2=lat,
                                                 lng2=lon)/1000
                                        max_prob_node.append(MAX_emiss_key)
                                    else:
                                        max_prob_node.append(MAX_trans_key)
                            if MAX_trans_key not in adjacency_list[track_list[i + 1]]:
                                break
                    # check if the first GPS track is assigned to a node
                    if track_list[i]==0:
                        if MAX_trans_key==0 or MAX_emiss_key== 0:
                            # get nearest node to the GPS track along the same edge
                            lat = float(viasat.latitude[viasat.ID == track_list[i]])
                            lon = float(viasat.longitude[viasat.ID == track_list[i]])
                            point = (lat, lon)
                            geom, u, v = ox.get_nearest_edge(grafo, point)
                            nearest_node = min((u, v), key=lambda n: ox.great_circle_vec(lat, lon, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                            # nearest_node = ox.get_nearest_node(grafo, point)
                            print("use nearest node to the GPS track")
                            # distance between nearest node and the GPS track (in Km)
                            distance_nearest_to_track = ox.great_circle_vec(lat1 = grafo.nodes[nearest_node]['y'],
                                                                    lng1 = grafo.nodes[nearest_node]['x'],
                                                                    lat2=lat,
                                                                    lng2=lon)/1000
                            max_prob_node.append(nearest_node)
                    # check if there is a node with the minimum path the the next node
                    if MAX_trans_key in shortest_path.keys() and MAX_trans_key in max_prob_node:
                        if shortest_path[MAX_trans_key] < min(SHORT_PATH):
                            print("find the next shortest path")
                        else:
                            new_node = min(shortest_path, key=shortest_path.get)
                            max_prob_node.remove(MAX_trans_key)
                            max_prob_node.append(new_node)

                    if len(max_prob_node) != 0:
                        distance_to_track_i = ox.great_circle_vec(lat1 = grafo.nodes[max_prob_node[-1]]['y'],
                                                                  lng1 = grafo.nodes[max_prob_node[-1]]['x'],
                                                                  lat2=float(viasat.latitude[
                                                                                 viasat.ID == track_list[i]]),
                                                                  lng2=float(viasat.longitude[
                                                                                 viasat.ID == track_list[
                                                                                     i]])) / 1000

                        distance_to_track_i1 = ox.great_circle_vec(lat1 = grafo.nodes[max_prob_node[-1]]['y'],
                                                                   lng1 = grafo.nodes[max_prob_node[-1]]['x'],
                                                                   lat2=float(viasat.latitude[
                                                                                  viasat.ID == track_list[
                                                                                      i + 1]]),
                                                                   lng2=float(viasat.longitude[
                                                                                  viasat.ID == track_list[
                                                                                      i + 1]])) / 1000
                        dists = [distance_to_track_i, distance_to_track_i1]
                        mean_dist = np.mean(dists)
                        lat1 = float(viasat.latitude[viasat.ID == track_list[i + 1]])
                        lon1 = float(viasat.longitude[viasat.ID == track_list[i + 1]])
                        lat0 = float(viasat.latitude[viasat.ID == track_list[i]])
                        lon0 = float(viasat.longitude[viasat.ID == track_list[i]])
                        point1 = (lat1, lon1)
                        point0 = (lat0, lon0)

                        if np.mean(dists) <= (distance_VIASAT/2):
                            nearest_node = ox.get_nearest_node(grafo, point1, return_dist=True)
                            geom0, u0, v0 = ox.get_nearest_edge(grafo, point0)
                            geom1, u1, v1 = ox.get_nearest_edge(grafo, point1)
                            nn0 = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                            max_prob_node.append(nn0)
                            nn1 = min((u1, v1), key=lambda n: ox.great_circle_vec(lat1, lon1, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
                            max_prob_node.append(nn1)
                            # remove first element of the list
                            # max_prob_node.pop(0)
            elif len(track_list)==1:
                # max_prob_node = list(adjacency_list[0]) # only one edge
                for KEY in adjacency_list.keys():
                    print(KEY)
                    max_prob_node = list(adjacency_list[KEY])  # only one edge

            ###################################################################################
            ###################################################################################
            ###### BUILD the PATH #############################################################
            ###################################################################################
            ###################################################################################

            # get unique values (ordered) - remove duplicates
            from collections import OrderedDict
            max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

            #### get last element of the "adjacency_list" (dictionary)
            last_key_nodes = list(adjacency_list.keys())[-1]
            last_nodes = list(adjacency_list[last_key_nodes])   ## get both of them!
            max_prob_node.extend(last_nodes)

            ### check that the nodes are on the same direction!!!!! ####
            # remove nodes that are not on the same directions..........
            NODE_TO_REMOVE = []
            for i in range(len(max_prob_node)-2):
                # u, v
                # if (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [0]]) or (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [1]]):
                if (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:,[0]]) and (([max_prob_node[1:(len(max_prob_node) - 1)][i]]) not in df_edges.values[:, [1]]):
                        print( ([max_prob_node[1:(len(max_prob_node) - 1)][i]]), "---> OUT..!")
                        node_to_remove = ([max_prob_node[1:(len(max_prob_node) - 1)][i]])[0]
                        NODE_TO_REMOVE.append(node_to_remove)
            # remove node from the max_prob_node list
            if len(NODE_TO_REMOVE) != 0:
                max_prob_node = [i for i in max_prob_node if i not in NODE_TO_REMOVE]


            ## check that the first GPS track point is near the first node of the 'max_prob_node'
            i = 0
            lat0 = float(viasat.latitude[viasat.ID == track_list[i]])
            lon0 = float(viasat.longitude[viasat.ID == track_list[i]])
            point0 = (lat0, lon0)
            geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
            nearest_node_first = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'], grafo.nodes[n]['x']))
            if nearest_node_first in max_prob_node:
                if max_prob_node[0] != nearest_node_first:
                    idx = max_prob_node.index(nearest_node_first)
                    # move 'nearest_node_first' at the first place
                    max_prob_node.insert(0, max_prob_node.pop(idx))

            # ## append the very first node to the max_prob_node list
            # max_prob_node = [u0] + max_prob_node

            ## check that there is continuity between the first and second pair of node (within two consecutive buffers)
            EDGES_BUFFER = df_edges.drop_duplicates('buffer_ID')
            ## get the buffer number in which there is the u0
            if len(EDGES_BUFFER[EDGES_BUFFER.u == u0]) > 0:
                n_buffer = EDGES_BUFFER[EDGES_BUFFER.u == u0]['buffer_ID'].iloc[0]
                # check if the consecutive bugger exists...
                if (n_buffer + 1) in list(EDGES_BUFFER['buffer_ID']):
                    ## append the very first node to the max_prob_node list
                    max_prob_node = [u0] + max_prob_node


            ## remove duplicates
            max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

            ### make a Dataframe with the list of the max_prob_nodes
            DF_max_prob_node = pd.DataFrame(max_prob_node)
            DF_max_prob_node = DF_max_prob_node.rename(columns={0: 'u'})
            ## merge 'max_prob_node' with edges
            ## make a list of all consecutive u,v from EDGES_BUFFER...
            node_EDGES = EDGES_BUFFER[['u', 'v']].values.tolist()
            node_EDGES = [val for sublist in node_EDGES for val in sublist]
            ## remove duplicates
            node_EDGES = list(OrderedDict.fromkeys(node_EDGES))
            ## make a dataframe
            DF_node_EDGES = pd.DataFrame(node_EDGES)
            DF_node_EDGES = DF_node_EDGES.rename(columns={0: 'u'})

            # filter gdf_edges with df_nodes
            keys = list(DF_max_prob_node.columns.values)
            index_edges = DF_node_EDGES.set_index(keys).index
            index_df_nodes = DF_max_prob_node.set_index(keys).index
            max_prob_node = DF_node_EDGES[index_edges.isin(index_df_nodes)]

            max_prob_node = list(max_prob_node['u'])


            ## attach the FIRST NODE of the EDGES initialy found
            ## make a list of all the indices of the VIASAT DATA
            VIASAT_INDICES = list(viasat.ID)
            first_index = VIASAT_INDICES[0]
            last_index = VIASAT_INDICES[-1]
            lat0 = float(viasat.latitude[viasat.ID == first_index ])
            lon0 = float(viasat.longitude[viasat.ID == first_index])
            point0 = (lat0, lon0)
            lat_last = float(viasat.latitude[viasat.ID == last_index])
            lon_last = float(viasat.longitude[viasat.ID == last_index])
            point_last = (lat_last, lon_last)
            ## get the node with the minimu, distance from the track correspondingg to the BUFFER number..
            geom, u0, v0 = ox.get_nearest_edge(grafo, point0)
            very_first_node = min((u0, v0), key=lambda n: ox.great_circle_vec(lat0, lon0, grafo.nodes[n]['y'],
                                                                              grafo.nodes[n]['x']))
            geom_last, u_last, v_last = ox.get_nearest_edge(grafo, point_last)
            very_last_node = min((u_last, v_last), key=lambda n: ox.great_circle_vec(lat_last, lon_last, grafo.nodes[n]['y'],
                                                                              grafo.nodes[n]['x']))
            # ox.get_nearest_node(grafo, point0)
            ## append the 'very_first_node' and the 'very_last_node'
            max_prob_node = [very_first_node] + max_prob_node + [very_last_node]

            ## remove duplicates
            max_prob_node = list(OrderedDict.fromkeys(max_prob_node))

            ### get ORIGIN and DESTINATION
            ORIGIN = max_prob_node[0]
            DESTINATION = max_prob_node[-1]


            #### build matched route with all max_prob_node  #####
            matched_route = []
            all_matched_edges = []
            for origin, destination in zip(max_prob_node, max_prob_node[1:]):
                try:
                    # print(origin, destination)
                    ## use full complete graph to build the final path
                    route = nx.dijkstra_path(grafo, origin, destination, weight='length')
                    # route = nx.dijkstra_path(grafo, origin, destination, weight='cost') # using cost (time)
                    path_edges = list(zip(route, route[1:]))
                    # print(path_edges)
                    all_matched_edges.append(path_edges)
                    matched_route.append(route)
                except nx.NetworkXNoPath:
                    print('No path', 'u:', origin, 'v:', destination)


            ##########///////////////////////////////////////////////////// ##########################################
            # if more than 2 element of the matched_route[2] are in matched_route[3], then delete matched_route[3]
            len_matched_edges = len(all_matched_edges)
            if len_matched_edges >1:
                list1 = all_matched_edges[len_matched_edges-2]
                list2 = all_matched_edges[len_matched_edges-1]
                common_nodes_last=[elem for elem in list1 if elem in list2]
                if len(common_nodes_last) >= 2:
                    all_matched_edges.remove(list2)
            ##########///////////////////////////////////////////////////// ##########################################


            if len(all_matched_edges) > 1:
                # isolate edges in the grafo from 'all_matched_edges'
                df_nodes = []
                for i in range(len(all_matched_edges)):
                    # print(all_matched_edges[i])
                    route = all_matched_edges[i]
                    for nodes in route:
                        # print('nodes:',nodes)
                        df_nodes.append(nodes)

                df_nodes = pd.DataFrame(df_nodes)
                df_nodes.columns = ['u', 'v']

                ## merge ordered list of nodes with edges from grafo
                # GRAFO = pd.DataFrame(gdf_edges)
                edges_matched_route = pd.merge(df_nodes, gdf_edges, on=['u', 'v'],how='left')
                edges_matched_route = gpd.GeoDataFrame(edges_matched_route)
                edges_matched_route.drop_duplicates(['u', 'v'], inplace=True)

                # edges_matched_route = edges_matched_route[edges_matched_route['lanes'].isna()]
                # edges_matched_route.dropna(subset=['lanes'], inplace= True)

                # filter 'edges_matched_route' (remove key = 1, that is the level above the street)
                filter_edge = edges_matched_route[edges_matched_route.key != 0]
                if len(filter_edge) !=0:
                    selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                    # get the with row with key == 0 (to be then removed
                    idx_edge = list(selected_edges[selected_edges.key == 0].index)
                    # filter row in 'edges_matched_route' with index == idx_edge
                    edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]
                # select edges with 'key' == 1


                if len(filter_edge) > 1:
                    selected_edges = edges_matched_route[edges_matched_route.u.isin(list(pd.to_numeric(filter_edge.u)))]
                    if len(selected_edges) == 1:
                        idx_edge = list(selected_edges[selected_edges.key == 1].index)
                        # filter row in 'edges_matched_route' with index == idx_edge
                        edges_matched_route = edges_matched_route[~edges_matched_route.index.isin(idx_edge)]

                # duplicated_U = edges_matched_route[edges_matched_route.duplicated(['u'], keep=False)]
                # duplicated_V = edges_matched_route[edges_matched_route.duplicated(['v'], keep=False)]
                # edges_matched_route.drop_duplicates(subset='u', keep='first', inplace=True)
                # edges_matched_route.drop_duplicates(subset='v', keep='last', inplace=True)

                ##################################################################################
                ######## Build the final table as from Gaetano ###################################
                ##################################################################################

                ## merge with adjacency list to assign corresponding tracks to each edge.....
                DF_edges_matched_route = pd.DataFrame(edges_matched_route)
                ## consider using KKK to get the 'idtrace'
                KKK = KKK.rename(columns={'ID': 'buffer_ID'})
                HHH = pd.merge(DF_edges_matched_route, df_edges, on=['u', 'v'], how='left')
                HHH['buffer_ID'] = HHH['buffer_ID'].bfill()
                HHH.drop_duplicates(['u', 'v'], inplace=True)
                HHH['buffer_ID'] = HHH['buffer_ID'].ffill()
                KKK_new = KKK[['u', 'v', 'buffer_ID', 'id', 'progressive', 'totalseconds', 'path_time', 'speed',
                               'timedate', 'TRIP_ID', 'idtrajectory', 'idterm', 'anomaly']]
                edges_matched_route_GV = pd.merge(HHH, KKK_new, on=['u', 'v', 'buffer_ID'], how='left')
                edges_matched_route_GV['id'] = edges_matched_route_GV['id'].bfill()
                edges_matched_route_GV['id'] = edges_matched_route_GV['id'].ffill()
                edges_matched_route_GV['id'] = edges_matched_route_GV['id'].bfill()
                # if (edges_matched_route_GV['id']).isnull().sum() == 0:
                # edges_matched_route_GV['id'].fillna(-1)
                edges_matched_route_GV['id'] = edges_matched_route_GV.id.astype('int')

                edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV['idtrajectory'].ffill()
                edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV['idtrajectory'].bfill()
                edges_matched_route_GV['idtrajectory'] = edges_matched_route_GV.idtrajectory.astype('int')

                edges_matched_route_GV['totalseconds'] = edges_matched_route_GV['totalseconds'].ffill()
                edges_matched_route_GV['totalseconds'] = edges_matched_route_GV['totalseconds'].bfill()
                edges_matched_route_GV['totalseconds'] = edges_matched_route_GV.totalseconds.astype('int')

                edges_matched_route_GV['timedate'] = edges_matched_route_GV['timedate'].ffill()
                edges_matched_route_GV['timedate'] = edges_matched_route_GV['timedate'].bfill()

                # compute the difference between last and first time within the same "progressive" value
                edges_matched_route_GV['progressive'] = edges_matched_route_GV['progressive'].ffill()
                edges_matched_route_GV['progressive'] = edges_matched_route_GV['progressive'].bfill()
                edges_matched_route_GV['progressive'] = edges_matched_route_GV.progressive.astype('int')

                edges_matched_route_GV = edges_matched_route_GV.rename(columns={'id': 'idtrace'})
                edges_matched_route_GV['sequenza'] = edges_matched_route_GV.index

                # last = edges_matched_route_GV.groupby('progressive').nth(-1)
                first = edges_matched_route_GV.groupby('progressive').nth(0)
                first_time = first.set_index('idtrace')
                first_progressive = first.reset_index(level=0)
                diff_time = first_time.totalseconds.diff()
                diff_progressive = first_progressive.progressive.diff()
                # shift
                diff_time = diff_time.shift(-1)
                diff_time = pd.DataFrame(diff_time)
                diff_time['idtrace'] = diff_time.index
                # reset index
                diff_time.reset_index(drop=True, inplace=True)
                diff_progressive = diff_progressive.shift(-1)

                # concatenate diff_time with diff_progressive
                df_speed = pd.concat([diff_time, diff_progressive], axis=1)
                if len(df_speed) == 1:
                    edges_matched_route_GV['mean_speed'] = edges_matched_route_GV[edges_matched_route_GV.speed.notnull()]['speed']
                # df_diff = df_diff.bfill(axis='rows')
                else:
                    df_speed['mean_speed'] = (df_speed.progressive/1000)/(df_speed.totalseconds/3600)
                    # add last instant speed
                    df_speed["mean_speed"].iloc[len(df_speed)-1] = first.speed.iloc[len(first)-1]
                # merge df_speed with main dataframe "edges_matched_route_GV" using "idtrace" as common field
                edges_matched_route_GV = pd.merge(edges_matched_route_GV, df_speed, on=['idtrace'], how='left')
                edges_matched_route_GV.drop(['totalseconds_y'], axis=1, inplace = True)
                edges_matched_route_GV = edges_matched_route_GV.rename(columns={'totalseconds_x': 'totalseconds'})

                edges_matched_route_GV['mean_speed'] = edges_matched_route_GV['mean_speed'].ffill()
                edges_matched_route_GV['mean_speed'] = edges_matched_route_GV['mean_speed'].bfill()
                edges_matched_route_GV['mean_speed'] = edges_matched_route_GV.mean_speed.astype('int')
                edges_matched_route_GV['TRIP_ID'] = edges_matched_route_GV['TRIP_ID'].ffill()
                edges_matched_route_GV['track_ID'] = edges_matched_route_GV['idterm'].ffill()
                ## remove rows with negative "mean_speed"...for now....
                edges_matched_route_GV = edges_matched_route_GV[edges_matched_route_GV['mean_speed'] > 0]
                edges_matched_route_GV = gpd.GeoDataFrame(edges_matched_route_GV)

                # populate a DB
                try:
                    final_map_matching_table_GV = edges_matched_route_GV[['idtrajectory', 'geometry',
                                                                          'u', 'v', 'idtrace', 'sequenza', 'mean_speed',
                                                                          'timedate', 'totalseconds', 'TRIP_ID', 'idterm',
                                                                          'length', 'highway', 'name', 'ref']]

                    final_map_matching_table_GV = gpd.GeoDataFrame(final_map_matching_table_GV)

                    ### Connect to a DB and populate the DB  ###
                    connection = engine.connect()
                    final_map_matching_table_GV['geom'] = final_map_matching_table_GV['geometry'].apply(wkb_hexer)
                    final_map_matching_table_GV.drop('geometry', 1, inplace=True)
                    # final_map_matching_table_GV.to_sql("mapmatching_temp", con=connection, schema="public",
                    #                   if_exists='append')
                    connection.close()
                except KeyError:
                    print("['ref'] not in OSM edge")

                # make a dataframe from the dictionaries "time_track" and "distance_between_points"
                time_dist_speed_edges = pd.DataFrame.from_dict(time_track, orient='index').reset_index()
                distance_edges = pd.DataFrame.from_dict(distance_between_points, orient='index').reset_index()
                speed_edges = pd.DataFrame.from_dict(speed_between_points, orient='index').reset_index()
                hour_edges = pd.DataFrame.from_dict(HOUR_track, orient='index').reset_index()
                timedate_edges = pd.DataFrame.from_dict(timedate_track, orient='index').reset_index()

                try:
                    time_dist_speed_edges['distance'] = distance_edges[0]
                    time_dist_speed_edges['speed'] = speed_edges[0]
                    time_dist_speed_edges['hour'] = hour_edges[0]
                    time_dist_speed_edges['timedate'] = timedate_edges[0]
                    time_dist_speed_edges.columns = ['u', 'time', 'distance', 'speed', 'hour', 'timedate']
                    # merge "time_track and distances" with the "edges_matched_route"
                    edges_matched_route = pd.merge(edges_matched_route, time_dist_speed_edges, on=['u'], how='left')
                    # !!!both 'u' and 'v' must be in the "time_dist_speed_edges" dataframe['u']!!!!
                    list_time_dist_speed_edges = list(time_dist_speed_edges['u'])
                    boolean_filter = edges_matched_route[['u', 'v']].isin(list_time_dist_speed_edges)
                    edges_matched_route_bool = edges_matched_route[
                        (boolean_filter['u'] == True) & (boolean_filter['v'] == True)]
                    edges_matched_route.loc[
                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'time']=np.nan
                    edges_matched_route.loc[
                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'distance']=np.nan
                    edges_matched_route.loc[
                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'speed'] = np.nan
                    edges_matched_route.loc[
                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'hour'] = np.nan
                    edges_matched_route.loc[
                        set(edges_matched_route.index) - set(edges_matched_route_bool.index), 'timedate'] = np.nan

                    ## quick plot
                    # edges_matched_route.plot()
                    ## append all EDGES in an unique dataframe
                    edges_matched_route['idterm'] = track_ID
                    edges_matched_route['trip_ID'] = TRIP_ID
                    edges_matched_route['DESTINATION'] = DESTINATION
                    edges_matched_route['ORIGIN'] = ORIGIN

                    # all_EDGES = all_EDGES.append(edges_matched_route)

                    edges_matched_route.geometry.to_file(filename='matched_route_with_buffer.geojson',
                                                         driver='GeoJSON')
                    folium.GeoJson('matched_route_with_buffer.geojson').add_to((my_map))
                    my_map.save("matched_route_with_buffer.html")

                    #################################################################
                    #################################################################
                    ### find the travelled distance of the matched route
                    sum_distance_mapmatching = sum(final_map_matching_table_GV.length)
                    ## calculate the accuracy of the matched route compared to the sum of the differences of the progressives (from Viasat data)
                    accuracy = str(int((sum_distance_mapmatching/sum_progressive)*100))
                    all_ACCURACY.append(accuracy)
                    ## save list as .csv file
                    import csv
                    # writing the data into the file
                    # Open File
                    resultFyle = open("accuracy_mapmatchng_SALERNO.csv", 'w')
                    # Write data to file
                    for r in all_ACCURACY:
                        resultFyle.write(r + "\n")
                    resultFyle.close()

                except KeyError:
                    print("no distance_edges")

#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################

conn_HAIG.close()
cur_HAIG.close()
