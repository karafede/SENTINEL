
import os
os.getcwd()
os.chdir('C:/ENEA_CAS_WORK/SENTINEL/viasat_data')

import osmnx as ox
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.cm as cm

# Catania = ox.graph_from_address('Catania, Italy',
#                                        distance=60000,
#                                        network_type='drive')

# filter residential and unclassified roads road
filter = ('["highway"!~"residential|unclassified|living_street|track|abandoned|path|footway|service|pedestrian|road|'
          'raceway|cycleway|steps|construction"]')

Catania = ox.graph_from_address('Catania, Italy',
                                       distance=20000,
                                       network_type='drive',
                                       custom_filter=filter)

# check what is inside the edges (type of roads)
edge_file = Catania.edges(keys=True,data=True)
ox.plot_graph(Catania)

Catania_shp = ox.gdf_from_place('Catania, Italy')
ox.save_gdf_shapefile(Catania_shp)

# save street network as GraphML file
# Catania_projected = ox.project_graph(Catania)
# ox.save_graphml(Catania_projected, filename='network_Catania_20km_epgs4326.graphml')
ox.save_graphml(Catania, filename='network_Catania_20km_epgs4326.graphml')

# save street network as ESRI shapefile (includes NODES and EDGES)
# ox.save_graph_shapefile(Catania_projected, filename='networkCatania_20km__shape')
ox.save_graph_shapefile(Catania, filename='networkCatania_20km__shape')

