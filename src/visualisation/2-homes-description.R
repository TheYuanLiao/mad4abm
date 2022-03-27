# Title     : Population representation
# Objective : Value difference on maps
# Created by: Yuan Liao
# Created on: 2022-03-27

library(dplyr)
library(ggplot2)
library(ggsci)
library(ggpubr)
library(ggspatial)
library(sf)
library(ggraph)
library(scico)
library(yaml)
library(DBI)
library(scales)

# Load DeSO zones
zones <- read_sf('results/zones_vgr_homes_2019.shp')
zones$rep <- zones$count / zones$befolkning * 100

sweden <- read_sf('dbs/municipalities/sweden_komk.shp')
county_names <- unique(sweden$county)
county <- sweden[sweden$county == county_names[7], ]

g1 <- ggplot(zones) +
  geom_point(aes(x=befolkning, y=count), show.legend = F,
             alpha=1, size=0.2, color='steelblue') +
  labs(x='Population', y='MAD individuals') +
  geom_abline(intercept = 0, slope = 0.013, size=0.3, color='gray45') +
  theme_minimal() +
  theme(plot.margin = margin(1,1,0,0, "cm"))

g2 <- ggplot(zones) +
  geom_sf(aes(fill=rep), color = NA, alpha=1, show.legend = T) +
  scale_fill_gradient(low = "darkblue", high = "yellow",
                      name='Population (%)') +
  coord_sf(datum=st_crs(3006)) +
  theme(legend.position = 'bottom') +
#  geom_sf(data = county, color = 'white', alpha=1, fill=NA, size=0.3) +
  annotation_scale() +
  theme_void() +
  theme(plot.margin = margin(1,1,0,0, "cm"))

G <- ggarrange(g1, g2, ncol = 2, nrow = 1, labels = c('(a)', '(b)'))
ggsave(filename = "figures/homes_desc.png", plot=G,
       width = 7.5, height = 4, unit = "in", dpi = 300)

