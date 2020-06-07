
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

## loop over all the .csv file with the raw VIASAT data
for csv_file in viasat_filenames:
# csv_file = viasat_filenames[0]
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
        df['id'] = pd.Series(range(i * slice, i * slice + slice))
        df['timedate'] = df['timedate'].astype('datetime64[ns]')
        df['timedate_gps'] = df['timedate_gps'].astype('datetime64[ns]')
        ## upload into the DB
        df.to_sql("dataraw", con=connection, schema="public",
                                       if_exists='append', index = False)



## add geometry WGS84 4286 (Catania, Italy)
cur_HAIG.execute("""
alter table dataraw add column geom geometry(POINT,4326)
""")

cur_HAIG.execute("""
update dataraw set geom = st_setsrid(st_point(longitude,Latitude),4326)
""")

conn_HAIG.commit()


## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "prova_viasat_files_csv" add id serial
     """)
conn_HAIG.commit()


## drop one column
cur_HAIG.execute("""
ALTER TABLE "prova_viasat_files_csv" DROP "idRequest"
     """)
conn_HAIG.commit()

conn_HAIG.close()
cur_HAIG.close()


## check the "id"
AAA = pd.read_sql_query('''
                    SELECT id 
                    FROM public.prova_viasat_files_csv''', conn_HAIG)


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