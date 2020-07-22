
rm(list = ls())

library(RPostgreSQL)
library(lubridate)
library(threadr)
library(stringr)
library(ggplot2)
library(dplyr)
library(openair)
library(gsubfn)
library(mgsub)


setwd("D:/ENEA_CAS_WORK/SENTINEL/viasat_data")

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

# Connection to postdev-01 server where DB with TomTom data from Gibraltar is stored
conn_HAIG <- dbConnect(drv, dbname = "HAIG_Viasat_SA",
                 host = "192.168.132.18", port = 5432,       
                 user = "postgres", password = "superuser")

conn_HAIG <- dbConnect(drv, dbname = "HAIG_Viasat_SA",
                       host = "10.0.0.1", port = 5432,       
                       user = "postgres", password = "superuser")

# load "idterm_vehtype_portata"
idterm_vehtype_portata <- read.csv(paste0("D:/ViaSat/Salerno/idterm_vehtype_portata.csv"),
         header = T, sep=",")[-1]
idterm_vehtype_portata$idterm <- as.factor(idterm_vehtype_portata$idterm)
idterm_vehtype_portata$vehtype <- as.factor(idterm_vehtype_portata$vehtype)

idterm_stats <- idterm_vehtype_portata %>%
    group_by(vehtype) %>%
    summarise(count = length(idterm))

## select only rows with not null values in portata
# idterm_vehtype_portata[complete.cases(idterm_vehtype_portata[, "portata"]), ]
idterm_stats <- idterm_vehtype_portata[complete.cases(idterm_vehtype_portata[, "portata"]), ] %>%
    group_by(vehtype) %>%
    summarise(count = length(idterm))



# DF[complete.cases(DF), ]

dbListTables(conn_HAIG)
# check for the public
dbExistsTable(conn_HAIG, "idterm_portata")
## get fields names of tables in the DB
dbListFields(conn_HAIG, "mapmatching_2017")
dbListFields(conn_HAIG, "routecheck_2017")
dbListFields(conn_HAIG, "routecheck_2019")
dbListFields(conn_HAIG, "routecheck_2017_temp")
dbListFields(conn_HAIG, "dataraw")
dbListFields(conn_HAIG, "OSM_edges")


# idterm_portata = dbGetQuery(conn_HAIG, "
#                         SELECT * 
#                         FROM public.idterm_portata
#                              LIMIT 100" )

## get all FCD data on the network section of the "Autostrada del Mediterraneo"

# (32048592, 246509515), (246509515, 1110091820)  --> Avellino
#  (1110091823,25844069), (25844069, 25844113)  <--- Salerno

# data = dbGetQuery(conn_HAIG, statement= paste ("
#     SELECT
#     split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm, u, v, \"TRIP_ID\",
#                                     timedate, mean_speed
#     FROM
#     mapmatching_2017
#     WHERE (u, v) in (VALUES (32048592, 246509515), (246509515, 1110091820),
#                             (1110091823,25844069), (25844069, 25844113))
#     "))


## add "speed" from routecheck_2017 using field "idtrace" = id
# data =  dbGetQuery(conn_HAIG, "
#                      WITH path AS(SELECT 
#                             split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
#                             u, v, idtrace,
#                             timedate, mean_speed
#                             FROM mapmatching_2017
#                            WHERE (u, v) in (VALUES (32048592, 246509515),
#                            (246509515, 1110091820),
#                             (1110091823,25844069), (25844069, 25844113))
#                                 )
#                              SELECT path.idterm, path.u, path.v, path.timedate,
#                                     path.mean_speed,
#                                     path.idtrace,
#                                     \"OSM_edges\".length,
#                                     \"OSM_edges\".highway,
#                                     \"OSM_edges\".name,
#                                     \"OSM_edges\".ref
#                         FROM path
#                             LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
#                                 ")

# start_time = Sys.time()
# data =  dbGetQuery(conn_HAIG, "
#                      WITH path AS(SELECT 
#                             split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
#                             u, v,
#                             timedate, mean_speed, idtrace
#                             FROM mapmatching_2017
#                            WHERE (u, v) in (VALUES (32048592, 246509515),
#                            (246509515, 1110091820),
#                             (1110091823,25844069), (25844069, 25844113))
#                                 )
#                              SELECT path.idterm, path.u, path.v, path.timedate,
#                                     path.mean_speed,
#                                     \"OSM_edges\".length,
#                                     \"OSM_edges\".highway,
#                                     \"OSM_edges\".name,
#                                     \"OSM_edges\".ref,
#                                     routecheck_2017.speed,
#                                     routecheck_2017.id
#                         FROM path
#                             LEFT JOIN routecheck_2017 ON path.idtrace = routecheck_2017.id
#                             LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
#                                 ")
# 
# stop_time = Sys.time()
# diff <- (stop_time - start_time)
# diff


###################################################
### load MAP-MATCHING data from DB ################
###################################################
### or just using dataraw.... #####################

start_time = Sys.time()

data =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (32048592, 246509515),
                           (246509515, 1110091820),
                            (1110091823,25844069), (25844069, 25844113))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff

## left join with "type" and "portata"
data <- data %>%
    left_join(idterm_vehtype_portata, by = "idterm")

write.csv(data, "FCD_data_speed_2019.csv")

min(data$timedate)
max(data$timedate)

############################################################################################
### tratto stradale in corrispodenza della PIASTRA UNIverista' SAlerno #####################
############################################################################################

## (25844050, 1110091861) <-- Salerno
## (1110091904, 3371747395)  --> Avellino



start_time = Sys.time()

data_UNISA =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (25844050, 1110091861),
                           (1110091904, 3371747395))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff


# start_time = Sys.time()
# ### https://www.postgresqltutorial.com/postgresql-left-join/
# data =  dbGetQuery(conn_HAIG, "
# SELECT
#     routecheck_2017.id,
#     routecheck_2017.speed,
#     mapmatching_2017.idtrace,
#     mapmatching_2017.u,
#     mapmatching_2017.v,
#     mapmatching_2017.mean_speed
# FROM
#     routecheck_2017
# LEFT JOIN mapmatching_2017 ON routecheck_2017.id::bigint = mapmatching_2017.idtrace::bigint ")
# 
# stop_time = Sys.time()
# diff <- (stop_time - start_time)
# diff


# head(data)
# data <- data[complete.cases(data[, "idtrace"]), ]

## filter data with speel < 200 km/h
data_UNISA <- data_UNISA %>%
    filter(mean_speed < 200)

n_data <- data_UNISA %>%
    group_by(u,v) %>%
    summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
              MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
data_UNISA <- data_UNISA %>%
    left_join(idterm_vehtype_portata, by = "idterm")
write.csv(data_UNISA, "FCD_2017_UNISA_2019.csv")


# data_UNISA$direzione <- as.factor(data_UNISA$u)
# data_UNISA$direzione <- gsub("25844050", "--> Salerno", (data_UNISA$direzione))
# data_UNISA$direzione <- gsub("1110091904", "<-- Avellino", (data_UNISA$direzione))
# 
# 
# idterms_salerno <- data_UNISA %>%
#     filter(direzione == "--> Salerno") 
# 
# idterms_avellino <- data_UNISA %>%
#     filter(direzione == "<-- Avellino")
# 
# idterms_salerno <- (unique(idterms_salerno$idterm))
# idterms_avellino <- (unique(idterms_avellino$idterm))



# start_time = Sys.time()
# prova =  dbGetQuery(conn_HAIG, " 
#                             WITH path AS(SELECT 
#                             u, v,
#                             timedate, mean_speed, idtrace, sequenza
#                             FROM mapmatching_2019
#                             WHERE date(timedate) = '2019-09-02')
#                           SELECT  path.u, path.v,
#                                   dataraw.idterm
#                            FROM path
#                             LEFT JOIN dataraw 
#                             ON path.idtrace = dataraw.id
#                             WHERE dataraw.idterm::bigint = 4198677
#                             ")
# stop_time = Sys.time()
# diff <- (stop_time - start_time)
# diff




start_time = Sys.time()
prova =  dbGetQuery(conn_HAIG, " 
                            WITH path AS(SELECT
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                            WHERE date(timedate) = '2019-09-02')
                          SELECT  path.u, path.v, path.timedate, path.mean_speed,
                                  dataraw.idterm, 
                                  \"OSM_edges\".length,
                                    \"OSM_edges\".highway
                           FROM path
                            LEFT JOIN dataraw 
                                   ON path.idtrace = dataraw.id
                          LEFT JOIN \"OSM_edges\" 
                                    ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                            ")
stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



start_time = Sys.time()
prova =  dbGetQuery(conn_HAIG, " 
                           SELECT  
                       mapmatching_2019.u, mapmatching_2019.v,
                            mapmatching_2019.timedate, mapmatching_2019.mean_speed, 
                            mapmatching_2019.idtrace,
                            dataraw.idterm
                        from mapmatching_2019
                        LEFT JOIN dataraw 
                                   ON mapmatching_2019.idtrace = dataraw.id  
                        WHERE date(mapmatching_2019.timedate) = '2019-09-02' 
                            ")
stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



# write.csv(prova, "flow_mapmatching_A2_salerno_2019.csv")




###########################################################################
###########################################################################
###########################################################################
######### Distribution of SPEED ###########################################
###########################################################################

data <- read.csv(paste0("FCD_data_speed.csv"),
                                   header = T, sep=",")[-1]
min(data$timedate)
max(data$timedate)

# (32048592, 246509515), (246509515, 1110091820)  --> Avellino
#  (1110091823,25844069), (25844069, 25844113)  <--- Salerno

data$u <- as.factor(data$u)
data$u <- gsub("25844069", "--> Salerno", (data$u))
data$u <- gsub("1110091823", "--> Salerno", (data$u))
data$u <- gsub("32048592", "<-- Avellino", (data$u))
data$u <- gsub("246509515", "<-- Avellino", (data$u))

direzione <- data[, c("u", "idterm", "speed")]
names(direzione)[names(direzione) == 'u'] <- 'direzione'

total_counts_direzione <- direzione %>%
    group_by(direzione) %>%
    summarize(instant_speed=mean(speed),
              count = length(idterm))


p <- data %>%
    # ggplot(aes((speed), fill = as.factor(vehtype))) +
    ggplot(aes((speed))) +
    # ggplot(aes( (mean_speed))) + 
    geom_density(alpha = 0.5) +
    # geom_histogram(binwidth=.05) +   ### counts
    stat_function(fun = dnorm, args = list(mean = mean(data$speed),
                                           sd = sd(data$speed)), colour = "red") +  ## fit with Gaussian
    # scale_x_continuous(trans='log10') +
    guides(fill=guide_legend(title="tipo veicolo")) +
    theme_bw() +
    theme(axis.title.x = element_text(face="bold", colour="black", size=13),     
          axis.text.x=element_text(angle=0,hjust=0,vjust=1, size=14)) +
    ylab("densita' (unita' arbitrarie)") +
    # ylab("conteggi") +
    xlab("velocita' (km/h)") +
    theme(axis.title.y = element_text(face="bold", colour="black", size=13),
          axis.text.y  = element_text(angle=0, vjust=0.5, size=13, colour="black")) +
    geom_vline(xintercept=90, color="red", linetype="dashed", size=0.5) +
    ggtitle("Distribuzione delle velocita' instantanee") +
    theme(plot.title = element_text(lineheight=.8, face="bold", size = 13))
p


# estimate paramters
library(MASS)
library(fitdistrplus)
# fitta <- fitdistr(data$mean_speed, "normal")
# fitta
## fit with a "gaussian/normal" distribution
fit_g  <- fitdist(data$speed, "norm")
summary(fit_g)
denscomp(ft = fit_g, legendtext = "Normal")
# par(mar = rep(2, 4))
# plot(fit_g)

# format DateTime
# make daily SUMS
names(data)[names(data) == 'timedate'] <- 'date'
### selecectd columns
data[ , c("date", "mean_speed")]
data_hourly <- data.frame(timeAverage(mydata = data[ , c("date", "mean_speed")],
                                    avg.time   = "hour", statistic  = "mean"))
AAA <- data %>%
    group_by(Date=floor_date(date, "1 hour"), u,v) %>%
    summarize(instant_speed=mean(speed),
              count = length(idterm))


total_counts_edges <- data %>%
    group_by(u,v) %>%
    summarize(instant_speed=mean(speed),
              count = length(idterm))





###############################################################################
###############################################################################
###############################################################################

IDs_hourly = dbGetQuery(conn_HAIG, statement= paste ("WITH ids AS 
                                    (SELECT
                                    split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm, 
                                    u, v,
                                    timedate, mean_speed
                                    FROM mapmatching_2017
                                    WHERE (u, v) in (VALUES (25844050, 1110091861),
                                                                (1110091904, 3371747395))
                                                                
                                    LIMIT 10),
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
                                    FROM ids_grouped"))


# write.csv(IDs_hourly, "IDs_hourly.csv")


AAA = dbGetQuery(conn_HAIG, "SELECT
                                new_id, idterm, \"TRIP_ID\", anomaly, path_time
                                FROM routecheck_2017_temp
                                WHERE routecheck_2017_temp.idterm::bigint = 3184004
                             ")

BBB = dbGetQuery(conn_HAIG, " SELECT
                                 id, idterm, timedate, longitude, latitude,
                                        panel, speed, progressive
                                 FROM dataraw
                                 WHERE dataraw.idterm::bigint = 4334573
                             ")
# 
# 
# JJJ = dbGetQuery(conn_HAIG, " SELECT
#                                  id, idterm, timedate, longitude, latitude,
#                                         panel, speed, progressive
#                                  FROM dataraw
#                                  WHERE dataraw.id = 10979881
#                              ")
# 
# KKK = dbGetQuery(conn_HAIG, "SELECT
#                                 id, idterm, \"TRIP_ID\", anomaly, path_time
#                                 
#                                 FROM routecheck_2017
#                                 WHERE routecheck_2017.id::bigint = 98181087
#                                 
#                              ")
# 
#
# KKK_idtrace = dbGetQuery(conn_HAIG, "SELECT
#                                  u, v,
#                                     timedate, mean_speed, idtrace
#                                 FROM mapmatching_2017
#                                 WHERE mapmatching_2017.idtrace = 98181087
#                                 
#                              ")


# LLL_idtrace_speed = dbGetQuery(conn_HAIG, "SELECT
#                             routecheck_2017.id,
#                             routecheck_2017.speed,
#                             mapmatching_2017.mean_speed
#                             FROM routecheck_2017
#                             INNER JOIN 
#                             mapmatching_2017 ON  routecheck_2017.id = mapmatching_2017.idtrace
#                            WHERE mapmatching_2017.idtrace = 98181087")

u_v_idtrace_speed = dbGetQuery(conn_HAIG, "SELECT
                            routecheck_2017.id,
                            routecheck_2017.speed,
                            mapmatching_2017.mean_speed,
                            mapmatching_2017.u,
                            mapmatching_2017.v
                            FROM routecheck_2017
                            LEFT JOIN 
                            mapmatching_2017 ON  routecheck_2017.id::bigint = mapmatching_2017.idtrace:bigint
                            WHERE (mapmatching_2017.u, mapmatching_2017.v) in (VALUES (32048592, 246509515),
                           (246509515, 1110091820),
                            (1110091823,25844069), (25844069, 25844113))")
                            

start_time = Sys.time()
dataraw_anomaly = dbGetQuery(conn_HAIG, "SELECT
                       dataraw.id,
                       dataraw.idterm,
                       dataraw.timedate,
                       dataraw.longitude,
                       dataraw.latitude,
                       dataraw.panel,
                       dataraw.speed,
                       dataraw.progressive,
                       routecheck_2017.anomaly,
                       routecheck_2017.\"TRIP_ID\",
                       routecheck_2017.path_time,
                       routecheck_2017.id
                            FROM dataraw
                            LEFT JOIN
                        routecheck_2017 ON dataraw.id::bigint = routecheck_2017.id::bigint
                             WHERE dataraw.idterm::bigint = 4334573")
stop_time = Sys.time()
diff <- (stop_time - start_time)
diff

###########################################################################
###########################################################################
###########################################################################
#### very important to check...JOIN dataraw with routecheck by "new_id" ###
###########################################################################
###########################################################################

## 1) in "dataraw" add id serial PRIMARY KEY
## 2) in "dataraw" create index on "id"
## 3) in "routecheck set "id" as bigint
## 4) in "routecheck create index on "id"
## 5) in "dataraw" create index on "iderm"


# 3163842
# 3272621
# 2712721
# 3183733
# 3204142


## "idterm" is a text
routecheck_2019 = dbGetQuery(conn_HAIG, "
                                    SELECT
                                           anomaly,
                                           totalseconds,
                                           idtrajectory,
                                           segment,
                                           \"TRIP_ID\",
                                           path_time
                                    FROM routecheck_2019
                                    WHERE routecheck_2019.idterm = '4491848'
                                    ")



start_time = Sys.time()
join_routecheck_dataraw = dbGetQuery(conn_HAIG, "
                                    WITH ids AS(
                                    SELECT
                                        new_id, idterm, timedate, longitude, latitude,
                                        panel, speed, progressive
                                    FROM dataraw
                                    WHERE dataraw.idterm::bigint = 3163842
                                    )
                                    SELECT
                                           /*ids.new_id,*/
                                           ids.idterm,
                                           ids.timedate,
                                           ids.longitude,
                                           ids.latitude,
                                           ids.panel,
                                           ids.speed,
                                           ids.progressive,
                                           routecheck_2017_temp.anomaly,
                                           routecheck_2017_temp.totalseconds,
                                           routecheck_2017_temp.idtrajectory,
                                           routecheck_2017_temp.segment,
                                           routecheck_2017_temp.\"TRIP_ID\",
                                           routecheck_2017_temp.path_time
                                           /*routecheck_2017_temp.new_id*/
                                        FROM ids
                                     LEFT JOIN
                        routecheck_2017_temp ON ids.new_id::bigint = routecheck_2017_temp.new_id::bigint
                                 ")
stop_time = Sys.time()
diff <- (stop_time - start_time)
diff

## remove rows with Na values
join_routecheck_dataraw <- join_routecheck_dataraw[complete.cases(join_routecheck_dataraw), ]










# viasat_fleet = dbGetQuery(conn_HAIG, "
#               SELECT *
#               FROM public.dataraw
#               WHERE vehtype::bigint = 2 
#               LIMIT 100")

OSM_join_mapmatch =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT u, v,
                            timedate, mean_speed
                            FROM mapmatching_2017
                            WHERE (u, v) in (VALUES (25844050, 1110091861),
                                                     (1110091904, 3371747395))
                                )
                             SELECT path.u, path.v, path.timedate,
                                    path.mean_speed,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    \"OSM_edges\".geom
                        FROM path
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")
                                                    
                    

