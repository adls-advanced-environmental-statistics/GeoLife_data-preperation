library(readr)
library(dplyr)
library(lubridate)
library(sf)
library(ggplot2)
library(duckdb)
library(stringr)

rle_id <- function(vec, include_last_NA = TRUE){

  vec[is.na(vec)] <- FALSE
  x <- rle(vec)$lengths
  y <- rep(seq_along(x), times=x)

  y[!vec] <- NA
  z <- as.integer(as.factor(y))
  z
}


con <- dbConnect(duckdb(dbdir = "data-tmp/tracks.db"))

tracks_join <- tbl(con, "tracks_join") |> 
  collect()


tracks_join  <- tracks_join  |> 
  arrange(id, transportation_mode, datetime) |> 
  group_by(id, transportation_mode) |> 
  mutate(
    # timelag_s = datepart("second", lead(datetime)-datetime), # ← this is a faster, duckb solution. but I seem to get an error when grouping, so I'm doing it in dplyr instead
    timelag_s = as.integer(difftime(lead(datetime), datetime, units = "sec")),
    timelag_small = timelag_s < 10*60,
    track_id = rle_id(timelag_small)
  ) |> 
  select(-timelag_s, -timelag_small)

# Some NAs. Not many.
sum(is.na(tracks_join$track_id)) / length(tracks_join$track_id)

tracks_join <- tracks_join |> 
  filter(!is.na(track_id))

# Quality check: track IDs are NOT unique, they start at 1 for every id, transportation mode.
# (btw, this is not convenient. I want ids per track that are globally unique)
tracks_join |> 
  group_by(id, transportation_mode) |> 
  summarise(
    track_ids = paste(unique(track_id), collapse = ", "),
    n = length(unique(track_id))
  )

# group_indices() gives the index per subgroup. I can use this as a track id. 
tracks_join$track_id <- tracks_join |> 
    group_by(id, transportation_mode, track_id) |> 
    group_indices()


n1 <- nrow(tracks_join)

tracks_join <- tracks_join |> 
  add_count(track_id)  |> 
  filter(n >= 100) |> # a track must have at least 100 locations
  select(-n)


nrow(tracks_join)/n1 # this removes 3% of our data

tracks_join <- tracks_join |> 
    st_as_sf(coords = c("lon","lat"), crs = 4326)


tracks_join <- tracks_join |> 
  group_by(track_id) |> 
    mutate(
      steplength = as.numeric(st_distance(lead(geometry), geometry, by_element = TRUE))
    ) 

# Quality check: How are the steplengths distributed?
# tracks_join |> 
#    ggplot() +
#    geom_histogram(aes(steplength), bins = 50) +
#    scale_y_log10() +
#    scale_x_log10()


# about 2.5% of all steplengths are higher than 100. I will use 100m as a threshold for 
# splitting tracks even further.
sum(tracks_join$steplength >= 100, na.rm = TRUE) / length(tracks_join$steplength)


tracks_join <- tracks_join |> 
  group_by(track_id) |> 
  mutate(
    steplength_small = steplength < 100,
    track_id_2 = rle_id(steplength_small)
  )


check2 <- tracks_join |> 
  st_drop_geometry() |> 
  group_by(track_id) |> 
  summarise(
    n = length(unique(track_id_2))
  ) 

# ggplot(check2) +
#   geom_histogram(aes(n)) +
#   scale_y_log10()

# Now use track_id_2 to create *NEW* track_ids
# that are unique globally
tracks_join$track_id <- tracks_join |> 
  group_by(track_id, track_id_2) |> 
  group_indices()

tracks_join$track_id_2 <- NULL
tracks_join$steplength_small <- NULL
tracks_join$steplength <- NULL

# Now, tracks are shorter and may have less than the desired amount of 
# samples (100). Count the number of samples again and filter again

nrow1 <- nrow(tracks_join)

tracks_join <- tracks_join |> 
  add_count(track_id)  |> 
  filter(n >= 100) |> # a track must have at least 100 locations
  select(-n)

# this removed about 7% of our samples
nrow(tracks_join)/nrow1


tracks_join <- tracks_join |> 
  rename(user_id = id) |> 
  relocate(track_id, .after = user_id) |> 
  relocate(datetime, .after = track_id)



## Write the file to different formats:


## Todo: Project to different CRS first?
# https://crs-explorer.proj.org/?latlng=39.926027,116.372681&ignoreWorld=false&allowDeprecated=false&authorities=EPSG&activeTypes=PROJECTED_CRS&map=osm

# eg to: 
# EPSG:32650	WGS 84 / UTM zone 50N

# classic geopackage
st_write(tracks_join, "data-out/GeoLife_Trajectories.gpkg", "full", append = FALSE)

# fancy new geoparquet
library(sfarrow)
st_write_parquet(tracks_join, "data-out/GeoLife_Trajectories.parquet")

# duckdb (fails)
# con <- dbConnect(duckdb(dbdir = "data-out/GeoLife_Trajectories.db"))
# dbSendQuery(con, "INSTALL spatial from core_nightly; LOAD spatial;")
# dbWriteTable(con, "full", tracks_join)





# Sample some tracks and visualize them. We should now have long, consecutive tracks
# track_smp <- sample(tracks_join$track_id, 1)
# smp <- tracks_join |> 
#   filter(track_id == track_smp) 

# smp_l <- st_cast(st_combine(smp), "LINESTRING")
# ggplot() +
#   geom_sf(data = smp_l) +
#     geom_sf(data = smp) 

  
