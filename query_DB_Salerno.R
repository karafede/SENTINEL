
library(RPostgreSQL)
library(lubridate)
library(threadr)
library(stringr)
library(ggplot2)
library(dplyr)
library(openair)

rm(list = ls())


setwd("D:/ENEA_CAS_WORK/SENTINEL/ANAS")

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
idterm_vehtype_portata[complete.cases(idterm_vehtype_portata[, "portata"]), ]
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
dbListFields(conn_HAIG, "dataraw")
dbListFields(conn_HAIG, "OSM_edges")


# idterm_portata = dbGetQuery(conn_HAIG, "
#                         SELECT * 
#                         FROM public.idterm_portata
#                              LIMIT 100" )

## get all FCD data on the network section of the "Autostrada del Mediterraneo"
data = dbGetQuery(conn_HAIG, statement= paste ("
    SELECT
    split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm, u, v, \"TRIP_ID\",
                                    timedate, mean_speed
    FROM
    mapmatching_2017
    WHERE (u, v) in (VALUES (32048592, 246509515), (246509515, 1110091820),
                            (1110091823,25844069), (25844069, 25844113))
    "))


data =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed
                            FROM mapmatching_2017
                           WHERE (u, v) in (VALUES (32048592, 246509515),
                           (246509515, 1110091820),
                            (1110091823,25844069), (25844069, 25844113))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref
                        FROM path
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

n_data <- data %>%
    group_by(u,v) %>%
    summarise(AVG_speed = mean(mean_speed, na.rm=T))

# WHERE (u, v) in (VALUES (25844045, 25844050), (25844050, 1110091861), (1110091861, 1868714795),
#                  (1110091919, 1110091904), (1110091904,3371747395), (3371747395, 31396695))

# (32048592, 246509515), (246509515, 1110091820)

## left join with "type" and "portata"
data <- data %>%
    left_join(idterm_vehtype_portata, by = "idterm")



set.seed(1)
df <- data.frame(PF = 10*rnorm(1000))
ggplot(df, aes(x = PF)) + 
    geom_histogram(aes(y =..density..),
                   breaks = seq(-50, 50, by = 10), 
                   colour = "black", 
                   fill = "white") +
    stat_function(fun = dnorm, args = list(mean = mean(df$PF), sd = sd(df$PF)))



p <- data %>%
    # ggplot(aes( (mean_speed), fill = as.factor(vehtype))) + 
    ggplot(aes( (mean_speed))) + 
    # geom_density(alpha = 0.5) +
    geom_histogram(binwidth=.05) +   ### counts
    stat_function(fun = dnorm, args = list(mean = mean(data$mean_speed),
                                           sd = sd(data$mean_speed))) +  ## fit with Gaussian
    scale_x_continuous(trans='log10') +
    guides(fill=guide_legend(title="tipo veicolo")) +
    theme_bw() +
    theme(axis.title.x = element_text(face="bold", colour="black", size=13),     
          axis.text.x=element_text(angle=0,hjust=0,vjust=1, size=14)) +
    # ylab("densita' (unita' arbitrarie)") +
    ylab("conteggi") +
    xlab("velocita' (km/h)") +
    theme(axis.title.y = element_text(face="bold", colour="black", size=13),
          axis.text.y  = element_text(angle=0, vjust=0.5, size=13, colour="black")) +
    geom_vline(xintercept=90, color="red", linetype="dashed", size=0.5) +
    ggtitle("Distribuzione delle velocita' dei veicoli per tipo di veicolo") +
    theme(plot.title = element_text(lineheight=.8, face="bold", size = 13))
p


# estimate paramters
library(MASS)
library(fitdistrplus)
# fitta <- fitdistr(data$mean_speed, "normal")
# fitta
fit_g  <- fitdist(data$mean_speed, "norm")
summary(fit_g)
denscomp(ft = fit_g, legendtext = "Normal")
# par(mar = rep(2, 4))
# plot(fit_g)

# format DateTime
# make daily SUMS
names(idterm_type)[names(idterm_type) == 'timedate'] <- 'date'
### selecectd columns
idterm_type[ , c("date", "mean_speed")]
idterm_type_hourly <- data.frame(timeAverage(mydata = idterm_type[ , c("date", "mean_speed")],
                                    avg.time   = "hour", statistic  = "mean"))
#start.date = round(min(DF$General$date, na.rm = TRUE), units = "hours"), 
# end.date   = round(max(DF$General$date, na.rm = TRUE), units = "hours")))
AAA <- idterm_type %>%
    group_by(Date=floor_date(date, "1 hour"), u,v) %>%
    summarize(c1=mean(mean_speed))


# CREATE TABLE albums_artists
# ( album_id integer NOT NULL REFERENCES albums
#     , artist_id integer NOT NULL REFERENCES artists
#     , PRIMARY KEY (album_id, artist_id)
# );
# 
# CREATE UNIQUE INDEX ON albums_artists (artist_id, album_id);


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
                                id, idterm, \"TRIP_ID\", anomaly, path_time
                                FROM routecheck_2017
                                WHERE routecheck_2017.idterm::bigint = 4334573
                             ")

BBB = dbGetQuery(conn_HAIG, " SELECT
                                 id, idterm, timedate, longitude, latitude,
                                        panel, speed, progressive
                                 FROM dataraw
                                 WHERE dataraw.idterm::bigint = 4334573
                             ")


JJJ = dbGetQuery(conn_HAIG, " SELECT
                                 id, idterm, timedate, longitude, latitude,
                                        panel, speed, progressive
                                 FROM dataraw
                                 WHERE dataraw.id::integer = 10979881
                             ")

KKK = dbGetQuery(conn_HAIG, "SELECT
                                id, idterm, \"TRIP_ID\", anomaly, path_time
                                FROM routecheck_2017
                                WHERE routecheck_2017.id::bigint = 108847431
                             ")


CCC = dbGetQuery(conn_HAIG, "SELECT
                        dataraw.id,
                        dataraw.idterm,
                       dataraw.timedate,
                       dataraw.longitude,
                       dataraw.latitude,
                       dataraw.panel,
                       dataraw.speed,
                       dataraw.progressive,
                       anomaly,
                       \"TRIP_ID\",
                       path_time
                            FROM dataraw
                            INNER JOIN
                        routecheck_2017 ON dataraw.id = routecheck_2017.id
                          WHERE dataraw.idterm::bigint = 4334573")


# join_routecheck_dataraw = dbGetQuery(conn_HAIG, "
#                                     WITH ids AS(
#                                     SELECT
#                                         id, idterm, timedate, longitude, latitude,
#                                         panel, speed, progressive
#                                     FROM dataraw
#                                     WHERE dataraw.idterm::bigint = 4334573
#                                     LIMIT 100)
#                                     SELECT 
#                                            ids.id,
#                                            ids.idterm,
#                                            ids.timedate,
#                                            ids.longitude,
#                                            ids.latitude,
#                                            ids.panel,
#                                            ids.speed,
#                                            ids.progressive,
#                                            routecheck_2017.anomaly,
#                                            routecheck_2017.\"TRIP_ID\",
#                                            routecheck_2017.path_time,
#                                            routecheck_2017.id
#                                     FROM
#                                     ids, routecheck_2017
#                                  ")

#  LEFT JOIN routecheck_2017 ON ids.id = routecheck_2017.id


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
                                                    
                    

