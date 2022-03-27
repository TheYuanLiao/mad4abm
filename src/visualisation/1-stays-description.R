# Title     : Data description - stays vgr 2019
# Objective : Visualise the distributions of key statistics
# Created by: Yuan Liao
# Created on: 2022-03-27


library(dplyr)
library(ggplot2)
library(DBI)
library(RPostgres)
library(yaml)
library(ggsci)
library(ggpubr)
library(scales) # to access break formatting functions

# Load descriptive stats from the database
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
df <- dbGetQuery(con, 'SELECT * FROM description.vgr_stops_2019')
vars <- c("# of days", "# of stays", "# of stays per active day",
          "Total duration per active day, hours", "Share of isolated stays", "Duration per stay, min")
var.names <- names(df)
var.names <- var.names[! var.names == 'device_uid']
names(vars) <- var.names

plot.var.ccdf <- function(df, var, vars){
  # get the ecdf - Empirical Cumulative Distribution Function of v
  my_ecdf <- ecdf( df[,var] )
  # now put the ecdf and its complementary in a data.frame
  df2plot <- data.frame( x = sort(df[,var]), y = 1-my_ecdf(sort(df[,var])) )

  g <- ggplot(data=df2plot) +
    theme_minimal() +
    geom_point(aes(x=x, y=y), color = 'steelblue', size=0.1) +
    labs(x=paste(vars[[var]], "(x)"), y="P(X > x)") +
    scale_x_log10(breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", math_format(10^.x))) +
    scale_y_log10(breaks = trans_breaks("log10", function(x) 10^x),
                  labels = trans_format("log10", math_format(10^.x))) +
    annotation_logticks() +
    theme(plot.margin = margin(1,0,0,0, "cm"))
  return(g)
}
# Visualise the distributions
glist <- lapply(var.names, function(x){plot.var.ccdf(df, x, vars)})

G <- ggarrange(plotlist=glist, nrow = 2, ncol = 3, labels = c('(a)', '(b)', '(c)', '(d)', '(e)', '(f)'))

ggsave(filename = "figures/data_description.png", plot=G,
     width = 7.5, height = 4, unit = "in", dpi = 300)