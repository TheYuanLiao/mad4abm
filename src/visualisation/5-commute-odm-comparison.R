# Title     : ODM visualization and similarity comparison
# Objective : Ground truth vs MAD Commute ODMs
# Created by: Yuan Liao
# Created on: 2022-11-08

library(ggplot2)
library(dplyr)
library(viridis)
library(ggpubr)
library(scales)
library(yaml)
library(DBI)

keys_manager <- read_yaml('./dbs/keys.yaml')
user <- keys_manager$database$user
password <- keys_manager$database$password
port <- keys_manager$database$port
db_name <- keys_manager$database$name
con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = "localhost",
                      dbname = db_name,
                      user = user,
                      password = password,
                      port = port)
df <- dbGetQuery(con, "SELECT * FROM odm.commute;")
df.muni <- df[df$level == 'Municipality',]
df.deso <- df[df$level == 'DeSO',]
sizes <- unique(df$sample_size)
df.odm.stats <- read.csv('results/commute_odm_comparison.csv')

odm.processor <- function(data, sample_size, qt){
  data <- data[(data$sv_commute != 0) & (data$mad_commute != 0), ]
  data <- data %>%
    filter({ { sample_size } }==sample_size) %>%
    filter({ { qt } }==qt)
  data$sv_commute <- data$sv_commute / sum(data$sv_commute)
  data$mad_commute <- data$mad_commute / sum(data$mad_commute)
  lower <- -log10(min(data$sv_commute))
  upper <- -log10(max(data$sv_commute))
  data$sv_cat <- cut(data$sv_commute, breaks = unlist(lapply(seq(lower, upper, -(lower-upper)/30),
                                                             function(x){10^(-x)})))

  data.stats <- data %>%
    group_by(sv_cat)  %>%
    summarise(sv_commute = median(sv_commute),
              center = median(mad_commute),
              lower = quantile(mad_commute, 0.25),
              upper = quantile(mad_commute, 0.75))
  return(list('odm'=data, 'odm_stats'=data.stats,
              'min_sv'=min(data$sv_commute)))
}

plot.muni <- function(muni){
  g <- ggplot() +
    theme_minimal() +
    # MAD vs Survey
    geom_bin2d(data = muni$odm, aes(x=sv_commute, y=mad_commute), alpha=0.2) +
    scale_fill_gradient(name = 'No. of OD pairs', low = "#3c40c6", high = 'coral') +
    # MAD
    geom_linerange(data = muni$odm_stats, aes(x=sv_commute, ymin=lower, ymax=upper),
                   color='#3c40c6', size=0.5) +
    geom_point(data = muni$odm_stats, aes(x=sv_commute, y=center),
               color='#3c40c6', shape = 21, fill = "white", size = 1) +
    scale_x_log10(limits = c(muni$min_sv, 0.01),
                  breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", scales::math_format(10^.x))) +
    scale_y_log10(limits = c(muni$min_sv, 0.01),
                  breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", scales::math_format(10^.x))) +
    geom_abline(intercept = 0, slope = 1, size=0.3, color='gray45') +
    labs(x = "Survey", y = 'MAD') +
    theme(legend.position = 'top', legend.key.height= unit(0.3, 'cm'),
          plot.margin = margin(1,0.5,0,0, "cm"))
  return(g)
}

muni.sets <- lapply(seq(1, 10), function(x){odm.processor(data = df.muni, sample_size=max(sizes), qt=x)})
muni.sets.size <- lapply(seq(1, 10), function(x){odm.processor(data = df.muni, sample_size=min(sizes), qt=x)})

plot.deso <- function(deso){
  g <- ggplot() +
    theme_minimal() +
    # MAD vs Survey
    geom_bin2d(data = deso$odm, aes(x=sv_commute, y=mad_commute), alpha=0.2) +
    scale_fill_gradient(name = 'No. of OD pairs', low = "#3c40c6", high = 'coral') +
    # MAD
    geom_linerange(data = deso$odm_stats, aes(x=sv_commute, ymin=lower, ymax=upper),
                   color='#3c40c6', size=0.5) +
    geom_point(data = deso$odm_stats, aes(x=sv_commute, y=center),
               color='#3c40c6', shape = 21, fill = "white", size = 1) +
    scale_x_log10(limits = c(deso$min_sv, 0.01),
                  breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", scales::math_format(10^.x))) +
    scale_y_log10(limits = c(deso$min_sv, 0.01),
                  breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", scales::math_format(10^.x))) +
    geom_abline(intercept = 0, slope = 1, size=0.3, color='gray45') +
    labs(x = "Survey", y = 'MAD') +
    theme(legend.position = 'top', legend.key.height= unit(0.3, 'cm'),
          plot.margin = margin(1,0.5,0,0, "cm"))
  return(g)
}

deso.sets <- lapply(seq(1, 10), function(x){odm.processor(data = df.deso, sample_size=max(sizes), qt=x)})
deso.sets.size <- lapply(seq(1, 10), function(x){odm.processor(data = df.deso, sample_size=min(sizes), qt=x)})

# Commute ODMs comparison at municipality level
g.muni <- lapply(muni.sets, function(x){plot.muni(muni = x)})
G1 <- ggarrange(plotlist=g.muni, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/odms_commute_municipality.png", plot=G1,
       width = 10, height = 5, unit = "in", dpi = 300)

g.muni.size <- lapply(muni.sets.size, function(x){plot.muni(muni = x)})
G2 <- ggarrange(plotlist=g.muni.size, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/odms_commute_municipality_size.png", plot=G2,
       width = 10, height = 5, unit = "in", dpi = 300)

# Commute ODMs comparison at deso level
g.deso <- lapply(deso.sets, function(x){plot.deso(deso = x)})
G3 <- ggarrange(plotlist=g.deso, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/odms_commute_deso.png", plot=G3,
       width = 10, height = 5, unit = "in", dpi = 300)

g.deso.size <- lapply(deso.sets.size, function(x){plot.deso(deso = x)})
G4 <- ggarrange(plotlist=g.deso.size, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/odms_commute_deso_size.png", plot=G4,
       width = 10, height = 5, unit = "in", dpi = 300)

g <- ggplot(data = df.odm.stats[df.odm.stats$level == 'Municipality',]) +
    theme_minimal() +
    # SSI
    geom_point(aes(x=qt, y=ssi, color=as.factor(sample_size)), shape = 21, fill = "white", size = 1) +
    geom_line(aes(x=qt, y=ssi, color=as.factor(sample_size)), size = 0.5) +
    labs(x = "Similarity (descending from 1 to 10)", y = 'SSI') +
    scale_color_discrete(name = "Sample control", labels = c('Yes (ca. 5000 individuals)', 'No')) +
    scale_x_continuous(breaks=seq(1, 10, 1)) +
    scale_y_continuous(breaks=seq(0.6, 1, 0.1)) +
    ylim(c(0.6, 1)) +
    theme(legend.position = 'top', legend.key.height= unit(0.3, 'cm'),
          plot.margin = margin(1,0.5,0,0, "cm"))
ggsave(filename = "figures/odms_commute_muni_stats.png", plot=g,
       width = 5, height = 5, unit = "in", dpi = 300)