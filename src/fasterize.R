# Load libraries and set theme
library(sf)
library(fasterize)
library(arrow)
library(raster)

aoi <- st_read("/home/sol/gitrepo/iraq-baghdad-transport-planning/data/aoi.geojson")
# %>%
# st_transform(3891)
r<-raster(aoi, nrows = 15000, ncols = 15000)

group <- 'ddf_mid_week'
path <- paste0("/home/sol/gitrepo/iraq-baghdad-transport-planning/notebooks/mobility/", group)
files <- list.files(path, full.names = TRUE)

for (file in files) {
  print(paste("Processing file:", file))
  jams_dir <- open_dataset(file)
  jams <- as.data.frame(jams_dir)%>% 
    st_as_sf(wkt = "geoWKT", crs = 4326) %>%
    # st_transform(3891) %>%
    st_crop(aoi)
  fast <- fasterize(jams %>% st_buffer(dist = 10), r, fun = "count")
  
  writeRaster(
    fast,
    filename = paste0("//home/sol/gitrepo/iraq-baghdad-transport-planning/results/rasters/", group, '_', substring(file, 88, 97), "4326.tif"),
    format = "GTiff",
    overwrite = TRUE
  )
}


