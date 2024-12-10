library(dplyr)
library(readr)
library(duckdb)

# library(nanoparquet)

con <- dbConnect(duckdb(dbdir = "data-tmp/tracks.db"))


txts <- list.files("data-in/Data", "\\.txt$", recursive = TRUE, full.names = TRUE)
labels <- read_tsv(txts,id = "id") |> 
  janitor::clean_names()|> 
  mutate(id = dirname(id)) |> 
  mutate(across(ends_with("time"),\(x)as.POSIXct(x, "%Y/%m/%d %H:%M:%S", tz = "UTC")))
dirs_with_labels <- unique(labels$id)


fi <- list.files(dirs_with_labels, "\\.plt$", recursive = TRUE, full.names = TRUE)
# to get all files
# fi <- list.files("Data", "\\.plt$", recursive = TRUE, full.names = TRUE)

# see "User Guide-1.3pdf"
cols <- c("lat","lon","dummy1","alt","dummy2","date","time")

tracks <- read_csv(fi, skip = 6,col_names = cols, id = "id") |> 
  mutate(
    id = dirname(dirname(id)),
    datetime = as.POSIXct(paste(date,time), tz = "UTC")
  ) |> 
  select(-starts_with("dummy"), -date, -time)



dbWriteTable(con, "tracks", tracks)
dbWriteTable(con, "labels", labels)


# write_parquet(tracks, "data-tmp/tracks.parquet")
