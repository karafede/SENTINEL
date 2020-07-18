
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2
os.chdir('D:/ViaSat/Salerno')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()

## create extension postgis on the database HAIG_Viasat_SA  (only one time)

# cur_HAIG.execute("""
# CREATE EXTENSION postgis
# """)
# cur_HAIG.execute("""
# CREATE EXTENSION postgis_topology
# """)
# conn_HAIG.commit()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_SA')
connection = engine.connect()

# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS obu CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

###########################
## create OBU table #######
###########################

static_csv = "VST_ENEA_SA_DATISTATICI.csv"
static_data = pd.read_csv(static_csv, delimiter=';', encoding='latin-1')
static_data.columns = ['idterm', 'devicetype', 'idvehcategory', 'brand', 'anno',
                       'portata', 'gender', 'age']
static_data['anno'] = static_data['anno'].astype('Int64')
static_data['portata'] = static_data['portata'].astype('Int64')
len(static_data)

#### create "OBU" table in the DB HAIG_Viasat_SA #####
## insert static_data into the DB HAIG_Viasat_SA
static_data.to_sql("obu", con=connection, schema="public", index=False)


##################################################
########## VIASAT dataraw ########################
##################################################

# match pattern of .csv files
viasat_filenames = ['VST_ENEA_SA_FCD_2017.csv',    # 83206797 lines
                    'VST_ENEA_SA_FCD_2019.csv',    # 106473285 lines
                    'VST_ENEA_SA_FCD_2019_2.csv']  # 79426790 lines

## erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS dataraw CASCADE")
# conn_HAIG.commit()


## loop over all the .csv file with the raw VIASAT data
for csv_file in viasat_filenames:
    print(csv_file)
# csv_file = viasat_filenames[2]
    file = open(csv_file)
    reader = csv.reader(file)
    ## get length of the csv file
    lines = len(list(reader))
    print(lines)

    slice = 100000  # slice of data to be insert into the DB during the loop
    ## calculate the neccessary number of iteration to carry out in order to upload all data into the DB
    iter = int(round(lines/slice, ndigits=0)) +1
    for i in range(0, iter):
        print(i)
        print(i, csv_file)
        # csv_file = viasat_filenames[0]
        df = pd.read_csv(csv_file, header=None ,delimiter=';', skiprows=i*slice ,nrows=slice)
        ## define colum names
        df.columns = ['idrequest', 'idterm', 'timedate', 'latitude', 'longitude',
                      'speed', 'direction', 'grade', 'panel', 'event', 'vehtype',
                      'progressive', 'millisec', 'timedate_gps', 'distance']
        # df['id'] = pd.Series(range(i * slice, i * slice + slice))
        df['timedate'] = df['timedate'].astype('datetime64[ns]')
        df['timedate_gps'] = df['timedate_gps'].astype('datetime64[ns]')
        ## upload into the DB
        df.to_sql("dataraw", con=connection, schema="public",
                                       if_exists='append', index = False)


'''

###########################################################
### ADD a SEQUENTIAL ID to the dataraw table ##############
###########################################################

## drop one column
cur_HAIG.execute("""
ALTER TABLE "dataraw" DROP "new_id"
     """)
conn_HAIG.commit()


## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "dataraw" add id serial PRIMARY KEY
     """)
conn_HAIG.commit()


## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "dataraw" add new_id serial PRIMARY KEY
     """)
conn_HAIG.commit()


## add geometry WGS84 4286 (Salerno, Italy)
cur_HAIG.execute("""
alter table dataraw add column geom geometry(POINT,4326)
""")

cur_HAIG.execute("""
update dataraw set geom = st_setsrid(st_point(longitude,latitude),4326)
""")
routecheck_2017

conn_HAIG.commit()

'''

######################################################################
## create an 'index only' to make faster queries in "dataraw" table ##
######################################################################

cur_HAIG.execute("""
CREATE index dataraw_idterm_idx on public.dataraw(idterm);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index dataraw_vehtype_idx on public.dataraw(vehtype);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_geom_idx on public.dataraw(geom);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index dataraw_lat_idx on public.dataraw(latitude);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index dataraw_lon_idx on public.dataraw(longitude);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index dataraw_timedate_idx on public.dataraw(timedate);
""")
conn_HAIG.commit()

conn_HAIG.close()
cur_HAIG.close()


##############################################################################
## create an 'index only' to make faster queries in "routecheck_2017" table ##
##############################################################################

#### !!!! to create indexes on two columns together...
# https://www.postgresql.org/docs/12/indexes-multicolumn.html

## add geometry WGS84 4286 (Salerno, Italy)
cur_HAIG.execute("""
alter table routecheck_2019 add column geom geometry(POINT,4326)
""")
conn_HAIG.commit()

cur_HAIG.execute("""
update routecheck_2019 set geom = st_setsrid(st_point(longitude,latitude),4326)
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_track_idx on public.routecheck_2017("track_ID");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_lat_idx on public.routecheck_2017(latitude);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_lon_idx on public.routecheck_2017(longitude);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_lat_idx on public.routecheck_2019(latitude);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index routecheck_2019_lon_idx on public.routecheck_2019(longitude);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_geom_idx on public.routecheck_2017(geom);
""")
conn_HAIG.commit()

cur_HAIG.execute("""
CREATE index routecheck_2019_geom_idx on public.routecheck_2019(geom);
""")
conn_HAIG.commit()


### create index on routecheck_2019
cur_HAIG.execute("""
CREATE index routecheck_2019_track_idx on public.routecheck_2019("track_ID");
""")
conn_HAIG.commit()

##########################################################
## Additional stuff ######################################
##------------------######################################

cur_HAIG.execute("""
ALTER TABLE public.routecheck_2019
  RENAME COLUMN "track_ID" TO idterm;
""")
conn_HAIG.commit()


cur_HAIG.execute("""
ALTER TABLE public.routecheck_2019 ALTER COLUMN "idterm" TYPE bigint USING "idterm"::bigint
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index dataraw_id_idx on public.dataraw("id");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_id_idx on public.routecheck_2017("id");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2017_temp_id_idx on public.routecheck_2017_temp("new_id");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_id_idx on public.routecheck_2019("id");
""")
conn_HAIG.commit()



viasat_data = pd.read_sql_query('''
              SELECT idterm, vehtype 
              FROM public.dataraw 
              LIMIT 100 ''', conn_HAIG)

### get all terminals corresponding to 'fleet'
viasat_data = pd.read_sql_query('''
              SELECT idterm, vehtype 
              FROM public.dataraw 
              WHERE vehtype = 2 ''', conn_HAIG)
# make an unique list
idterms_fleet = list(viasat_data.idterm.unique())
len(idterms_fleet)

[2400053] in idterms_fleet

## select only 'fleet' from the routecheck
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT "idterm" 
        FROM public.routecheck_2017
        LIMIT 100''', conn_HAIG)

all_ID_TRACKS = list(all_VIASAT_IDterminals.track_ID.unique())
all_ID_TRACKS = [int(i) for i in all_ID_TRACKS]
all_ID_TRACKS.append(2400053)

# https://stackoverflow.com/questions/34288403/how-to-keep-elements-of-a-list-based-on-another-list
idterms_fleet = set(idterms_fleet)
all_ID_TRACKS_filtered = [x for x in all_ID_TRACKS if x in idterms_fleet]


all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.routecheck_2019
        WHERE "idterm" = '2400053' ''', conn_HAIG)


'''
# dateTime timestamp NOT NULL,
cur_HAIG.execute("""
     CREATE  TABLE dataraw(
     idrequest bigint,
     idterm bigint  ,
     timedate timestamp ,
     latitude numeric ,
     longitude numeric ,
     speed integer ,
     direction integer ,
     grade integer ,
     panel integer ,
     event integer ,
     vehtype integer ,
     progressive bigint,
     millisec integer,
     timedate_gps timestamp,
     distance bigint)
     """)

conn_HAIG.commit()

sep = ","
# populate table from the .csv data
for csv_file in viasat_filenames:
    print(csv_file)
    # csv_file = viasat_filenames[0]
    with open(csv_file, 'r') as f:
        viasat_data = csv.reader(f, delimiter = ';')
        print(viasat_data)
        next(viasat_data)
        for col in viasat_data:
            print(col)
            # print(len(col))
            if len(col) == 15:
                print(col[1])
                idrequest=col[0]
                idterm=col[1]
                timedate=col[2]
                latitude=col[3]
                longitude=col[4]
                speed=col[5]
                direction=col[6]
                grade=col[7]
                panel=col[8]
                event = col[9]
                vehtype=col[10]
                progressive=col[11]
                millisec = col[12]
                timedate_gps = col[13]
                distance = col[14]
                input = "(" + str(idrequest) + sep\
                        + str(idterm) + sep\
                        + "'" + str(timedate) + "'" + sep\
                        + str(latitude) + sep\
                        + str(longitude) + sep\
                        + str(speed) + sep\
                        + str(direction) + sep\
                        + str(grade) + sep\
                        + str(panel) + sep \
                        + str(event) + sep \
                        + str(vehtype) + sep \
                        + str(progressive) + sep \
                        + str(millisec) + sep \
                        + "'" + str(timedate_gps) + "'" + sep \
                        + str(distance) + ")"
                try:
                    cur_HAIG.execute("INSERT INTO dataraw (idrequest, idterm, timedate, latitude, longitude, speed, "
                                "direction, grade, panel, event, vehtype, progressive, "
                                "millisec, timedate_gps, distance)" + " VALUES " +input + "")
                except psycopg2.errors.NumericValueOutOfRange:
                    print("skip VALUES")

    conn_HAIG.commit()
'''


'''
### rename the table in order to create a new one with columns in a different order
cur_HAIG.execute("""
ALTER TABLE "prova_viasat_files_csv" rename to "prova_viasat_files_csv_old"
     """)
conn_HAIG.commit()

## check the dateTime format
cur_HAIG.execute("""
ALTER TABLE prova_viasat_files_csv_old ALTER COLUMN "dateTime" TYPE timestamp USING "dateTime"::timestamp
     """)
conn_HAIG.commit()


## create empty table to host all VIASAT data...with the WANTED order of the columns
cur_HAIG.execute("""
     CREATE  TABLE "prova_viasat_files_csv"(
     id bigint,
     deviceId integer  ,
     dateTime timestamp ,
     latitude numeric ,
     longitude numeric ,
     speedKmh integer ,
     heading integer ,
     accuracyDop integer ,
     EngnineStatus integer ,
     Type integer ,
     Odometer integer)
     """)
conn_HAIG.commit()


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_CT()
cur_HAIG = conn_HAIG.cursor()

## create empty table to host all VIASAT data
cur_HAIG.execute("""
    INSERT into prova_viasat_files_csv (id, deviceid, datetime, latitude, longitude, speedKmh,
                        heading, accuracydop, engninestatus, type, odometer)
    SELECT id, "deviceId", "dateTime", latitude, longitude, "speedKmh",
                        heading, "accuracyDop", "EngnineStatus", "Type", "Odometer" FROM "prova_viasat_files_csv_old";
    """)
conn_HAIG.commit()


conn_HAIG.close()
cur_HAIG.close()

'''

##########################################################
### Check mapmatching DB #################################
##########################################################

conn_HAIG = db_connect.connect_HAIG_Viasat_SA()
cur_HAIG = conn_HAIG.cursor()

#### check how many TRIP ID we have #############

# get all ID terminal of Viasat data
all_VIASAT_TRIP_IDs = pd.read_sql_query(
    ''' SELECT "TRIP_ID" 
        FROM public.mapmatching_2019 ''', conn_HAIG)

# make a list of all unique trips
all_TRIP_IDs = list(all_VIASAT_TRIP_IDs.TRIP_ID.unique())
### save and treat result in R #####
with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_TRIP_IDs_2019.txt", "w") as file:
    file.write(str(all_TRIP_IDs))


print(len(all_VIASAT_TRIP_IDs))
print("trip number:", len(all_TRIP_IDs))

## get all terminals (unique number of vehicles)
idterm = list((all_VIASAT_TRIP_IDs.TRIP_ID.str.split('_', expand=True)[0]).unique())
print("vehicle number:", len(idterm))


## reload 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_ID_TRACKS_2019.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and matched idterms
all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(idterm))
print(len(all_ID_TRACKS_DIFF))
# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/SENTINEL/viasat_data/all_ID_TRACKS_2019_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))

######################################
### check the size of a table ########
######################################

## create index on the column (u,v) togethers in the table 'mapmatching_2017' ###
cur_HAIG.execute("""
CREATE INDEX UV_idx ON public.mapmatching_2017(u,v);
""")
conn_HAIG.commit()


## create index on the "TRIP_ID" column
cur_HAIG.execute("""
CREATE index trip_id_match2017_idx on public.mapmatching_2017("TRIP_ID");
""")
conn_HAIG.commit()


## create index on the "idtrace" column
cur_HAIG.execute("""
CREATE index trip_idrace_match2017_idx on public.mapmatching_2017("idtrace");
""")
conn_HAIG.commit()



## DROP columns in mapmatching_2017
cur_HAIG.execute("""
ALTER TABLE "mapmatching_2017" DROP "name"
     """)
conn_HAIG.commit()

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('mapmatching_2019') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('mapmatching_2017') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('dataraw') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('routecheck_2017') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('routecheck_2019') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public."OSM_edges"') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.idterm_portata') )''', conn_HAIG)

### check the size of the WHOLE DB "HAIG_Viasat_SA"
pd.read_sql_query('''
SELECT pg_size_pretty( pg_database_size('HAIG_Viasat_SA') )''', conn_HAIG)


###########################################################
### get right geometry al linestring and plot data ########
###########################################################
viasat_data = pd.read_sql_query('''
                        SELECT * 
                        FROM public.mapmatching_2017 
                        LIMIT 300000 ''', conn_HAIG)

viasat_data = pd.read_sql_query('''
                        SELECT * 
                        FROM public.mapmatching_2017 
                        WHERE "idtrajectory" = '75614866' ''', conn_HAIG)    ## vehtype = 2 (fleet)
viasat_data = viasat_data.sort_values('sequenza')

## transform Geometry from text to LINESTRING
# wkb.loads(gdf_all_EDGES.geom, hex=True)
from shapely import wkb
import geopandas as gpd

def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

viasat_data['geometry'] = viasat_data.apply(wkb_tranformation, axis=1)
viasat_data.drop(['geom'], axis=1, inplace= True)
viasat_data = gpd.GeoDataFrame(viasat_data)
viasat_data.plot()


#######################################################################
## count how many times an edge ('u', 'v') occur in the geodataframe ##
#######################################################################

count_AAA = pd.read_sql_query('''
                SELECT u, v, COUNT(*)
                FROM  public.mapmatching_2017
                GROUP BY u, v ''', conn_HAIG)

OSM_edges = pd.read_sql_query('''
                        SELECT * 
                        FROM public."OSM_edges"
                        LIMIT 300 ''', conn_HAIG)

OSM_nodes = pd.read_sql_query('''
                        SELECT * 
                        FROM public."OSM_nodes"
                        LIMIT 300 ''', conn_HAIG)

idterm_portata = pd.read_sql_query('''
                        SELECT * 
                        FROM public."idterm_portata"
                        ''', conn_HAIG)
