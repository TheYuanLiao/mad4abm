# Title     : Distance to home and work templates
# Objective : For determining the distance threshold for work and home places detection
# Created by: Yuan Liao
# Created on: 2022-10-31

library(dplyr)
library(ggplot2)
library(ggsci)
library(ggpubr)
library(scales)

df.survey <- read.csv('results/activity_patterns_survey.csv')
df.mad <- dbGetQuery(con, "SELECT * FROM description.home_work")
qts.home <- quantile(df.mad[,"dist2home_wt"], 1:99/100)
qts.work <- quantile(df.mad[,"dist2work_wt"], 1:99/100)

df.qts.home <- data.frame(qts.home)
names(df.qts.home) <- 'qts'
df.qts.home$n <- seq(1, 99)
df.qts.home$activity <- 'Home'
df.qts.work <- data.frame(qts.work)
names(df.qts.work) <- 'qts'
df.qts.work$n <- seq(1, 99)
df.qts.work$activity <- 'Work'
df.qts <- rbind(df.qts.home, df.qts.work)

df.mad.home <- read.csv('results/home_detection_examples.csv')
df.mad.work <- read.csv('results/work_detection_examples.csv')
df.mad.indi <- read.csv('results/hw_detection_temporal_examples.csv')
plt.example <- function(uid, cluster, act) {
  sv <- df.survey[df.survey$activity == act, c('half_hour', 'freq')]
  sv$src <- 'Survey'
  mad <- df.mad.indi %>%
    filter(({{uid}}==uid) & ({{cluster}}==cluster)) %>%
    select(c('half_hour', 'freq_wt'))
  mad$src <- 'MAD'
  mad <- rename(mad, freq = freq_wt)
  df2plot <- rbind(sv, mad)
  g1 <- ggplot(data = df2plot, aes(x = half_hour / 2, y = freq,
                                   group = src, color = src, alpha = src)) +
    geom_point(size=0.7) +
    geom_line(stat="smooth", method = "loess", size=0.8) +
    scale_color_discrete(name = 'Source') +
    scale_alpha_discrete(range=c(1, 0.3), guide = F) +
    labs(x = 'Hour of day', y = paste0(act, ' prob.')) +
    theme_minimal() +
    ylim(c(0, 1.01)) +
    theme(plot.margin = margin(1,0,0,0, "cm"))
    return(g1)
}
# Distance distribution
g1 <- ggplot(data = df.qts) +
  geom_line(aes(x = n, y = qts,
                group = activity, color = activity)) +
  scale_color_discrete(name = 'Activity') +
  labs(x = 'Percentile (%)', y = 'Distance to the template') +
  theme_minimal() +
  theme(plot.margin = margin(1,0,0,0, "cm"))
ggsave(filename = "figures/activity_tempo_distance.png", plot=g1,
       width = 5, height = 5, unit = "in", dpi = 300)

# Temporal profile of being home vs the template
g.home <- lapply(seq(1, 10), function(x){plt.example(uid = df.mad.home[x, 'uid'],
                  cluster = df.mad.home[x, 'home'],
                  act = 'Home')})
G1 <- ggarrange(plotlist=g.home, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/activity_tempo_vs_survey_home.png", plot=G1,
       width = 10, height = 5, unit = "in", dpi = 300)

# Temporal profile of being at work vs the template
g.work <- lapply(seq(1, 10), function(x){plt.example(uid = df.mad.work[x, 'uid'],
                  cluster = df.mad.work[x, 'work'],
                  act = 'Work')})
G2 <- ggarrange(plotlist=g.work, ncol = 5, nrow = 2, labels = seq(1, 10), common.legend = T)
ggsave(filename = "figures/activity_tempo_vs_survey_work.png", plot=G2,
       width = 10, height = 5, unit = "in", dpi = 300)