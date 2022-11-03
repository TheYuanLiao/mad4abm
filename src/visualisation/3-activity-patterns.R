# Title     : Activity patterns by activity type from Survey
# Objective : Value difference on maps
# Created by: Yuan Liao
# Created on: 2022-10-25

library(dplyr)
library(ggplot2)
library(ggsci)
library(ggpubr)
library(yaml)
library(DBI)
library(scales)

df.survey <- read.csv('results/activity_patterns_survey.csv')
df.mad <- read.csv('results/activity_patterns_mad.csv')

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
df.mad.indi <- dbGetQuery(con, "SELECT * FROM description.tempo_top3
WHERE uid = 'eb6a60f4-7d28-4090-a5e9-1f2ac33c871d'")

g1 <- ggplot(data = df.survey) +
  geom_line(aes(x = half_hour / 2, y = freq,
                group = activity, color = activity)) +
  scale_color_discrete(name = 'Activity') +
  labs(x = 'Hour of day', y = 'Activity prob.') +
  theme_minimal() +
  theme(plot.margin = margin(1,0,0,0, "cm"))


g2 <- ggplot(data = df.mad) +
  geom_line(aes(x = half_hour / 2, y = freq,
                group = activity, color = activity)) +
  scale_color_discrete(name = 'Type') +
  labs(x = 'Hour of day', y = 'Activity prob.') +
  theme_minimal() +
  theme(plot.margin = margin(1,0,0,0, "cm"))

g3 <- ggplot(data = df.mad.indi,
             aes(x = half_hour / 2, y = freq,
                 group = as.factor(cluster), color = as.factor(cluster))) +
  geom_point(alpha=0.2, size=0.5) +
  geom_smooth(method = "loess") +
  scale_color_discrete(name = 'Cluster') +
  labs(x = 'Hour of day', y = 'Cluster prob.') +
  theme_minimal() +
  ylim(0, 1) +
  theme(plot.margin = margin(1,0,0,0, "cm"))

G1 <- ggarrange(g1, g2, ncol = 2, nrow = 1, labels = c('(a) Survey', '(b) MAD'))
ggsave(filename = "figures/activity_tempo_desc.png", plot=G1,
       width = 8, height = 3, unit = "in", dpi = 300)

G2 <- ggarrange(g1, g3, ncol = 2, nrow = 1, labels = c('(a) Survey', '(b) A MAD individual'))
ggsave(filename = "figures/activity_tempo_desc_indi.png", plot=G2,
       width = 8, height = 3, unit = "in", dpi = 300)