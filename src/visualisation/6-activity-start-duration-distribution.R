# Title     : Survey activity patterns
# Objective : Activity start time vs. duration
# Created by: Yuan Liao
# Created on: 2022-12-15

library(ggplot2)
library(dplyr)
library(viridis)
library(ggpubr)
library(scales)

df.survey <- read.csv('dbs/survey/day_act.csv')

g <- ggplot() +
    theme_minimal() +
    geom_hex(data = df.survey, aes(x=h_s, y=dur)) +
    scale_fill_gradient(name = 'No. of activities', low = "#3c40c6", high = 'coral') +
    labs(x = "Activity start (min)", y = 'Activity duration (min)') +
  facet_grid(.~Purpose) +
    theme(legend.position = 'top', legend.key.height= unit(0.3, 'cm'),
          plot.margin = margin(1,0.5,0,0, "cm"))

ggsave(filename = "figures/activity_start_duration.png", plot=g,
       width = 10, height = 4, unit = "in", dpi = 300)