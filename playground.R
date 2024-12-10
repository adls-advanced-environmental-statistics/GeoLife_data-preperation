

smp <- smp |> 
  mutate(
    steplength = as.numeric(st_distance(lead(geometry), geometry, by_element = TRUE))
  ) 



ggplot(smp) +
  geom_histogram(aes(steplength), bins = 100)
