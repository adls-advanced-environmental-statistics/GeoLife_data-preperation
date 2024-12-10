library(dplyr)
library(readr)
library(duckdb)

con <- dbConnect(duckdb(dbdir = "data-tmp/tracks.db"))

# if arrange() were even earlyer, collecting and writing back to 
# duckdb would not be necessary
track_ordered <- tbl(con, "tracks") |> 
  arrange(id, datetime) |> 
  collect()

dbWriteTable(con, "track_ordered", track_ordered)

track_ordered <- tbl(con, "track_ordered")

labels <- tbl(con, "labels")



tracks_join <- track_ordered  |> 
  left_join(labels, by = join_by(id, datetime >= start_time,  datetime <= end_time)) |> 
  filter(!is.na(transportation_mode)) |> 
  select(-ends_with("_time")) |> 
  mutate(
      alt = na_if(alt, -777),
      id = as.integer(str_remove(id, "data-in/Data/"))
  ) |> 
  collect()

dbWriteTable(con, "tracks_join", tracks_join, overwrite = TRUE)

