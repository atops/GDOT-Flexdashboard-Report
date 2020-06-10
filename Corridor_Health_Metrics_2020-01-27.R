library(aws.s3)
library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(readr)


get_summary_data <- function(df, current_month = NULL) {
    #' Converts sub or cor data set to a single data frame for the current_month
    #' for use in get_subcorridor_summary_table function
    #'
    #' @param df cor or sub data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor
    # current_month <- months[order(months)][match(current_month, months.formatted)]
    data <- list(
        rename(df$mo$du, du = uptime, du.delta = delta),
        rename(df$mo$pau, pau = uptime, pau.delta = delta),
        rename(df$mo$pdc, pdc = Avg.Max.Ped.Delay, pdc.delta = delta),
        rename(df$mo$pdf, pdf = Avg.Max.Ped.Delay, pdf.delta = delta),
        rename(df$mo$cctv, cctv = uptime, cctv.delta = delta),
        rename(df$mo$ru, rsu = uptime, rsu.delta = delta),
        rename(df$mo$cu, cu = uptime, cu.delta = delta),
        rename(df$mo$tp, tp.delta = delta),
        rename(df$mo$aogd, aog.delta = delta),
        rename(df$mo$prd, pr.delta = delta),
        rename(df$mo$qsd, qs.delta = delta),
        rename(df$mo$sfd, sf.delta = delta),
        rename(df$mo$tti, tti.delta = delta),
        rename(df$mo$pti, pti.delta = delta)
    ) %>%
        reduce(full_join, by = c("Zone_Group", "Corridor", "Month")) %>%
        filter(
            as.character(Zone_Group) != as.character(Corridor)
        ) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            #-num,
            -starts_with("Description"),
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol"),
            -starts_with("num")
        )
    if ("mttr" %in% names(df$mo)) { # cor, not sub
        data <- data %>% 
            arrange(Month, Zone_Group, Corridor)
    } else {
        data <- data %>% 
            rename(
                Subcorridor = Corridor,
                Corridor = Zone_Group
            ) %>%
            arrange(Month, Corridor, Subcorridor)
    }
    data <- data[, c(2, 1, 3:length(data))] # reorder Subcorridor, Corridor
    if (!is.null(current_month)) {
        data <- data %>% filter(Month == ymd(current_month))
    }
    return(data)
}


source("Monthly_Report_Package_init.R")
options(dplyr.summarise.inform = FALSE)

# Ped delay - coordinated and free signals, csv from Anthony
pdc <- read_csv("health_ped_delay_data/ped_delay_summary_coordinated_Q12020_monthly.csv") %>% 
    mutate(
        SignalID = factor(SignalID),
        Month = ymd(glue("2020-{Month}-01")))
pdf <- read_csv("health_ped_delay_data/ped_delay_summary_free_Q12020_monthly.csv") %>% 
    mutate(
        SignalID = factor(SignalID),
        Month = ymd(glue("2020-{Month}-01")))

sub_mo_pdc <- get_cor_monthly_avg_by_day(pdc, subcorridors, "Avg.Max.Ped.Delay")
sub_mo_pdf <- get_cor_monthly_avg_by_day(pdf, subcorridors, "Avg.Max.Ped.Delay")

sub <- s3readRDS(bucket = "gdot-spm", object = "sub_ec2.rds")
cor <- s3readRDS(bucket = "gdot-spm", object = "cor_ec2.rds")

sub$mo$pdc <- sub_mo_pdc
sub$mo$pdf <- sub_mo_pdf

current_month <- ymd("2020-04-01")

csd <- get_summary_data(sub)
cmd <- get_summary_data(cor)






lapply(cor$mo, names)
lapply(cor$wk, names)
lapply(cor$dy, names)

lapply(sig$mo, names)
lapply(sig$wk, names)
lapply(sig$dy, names)

lapply(sub$mo, names)
lapply(sub$wk, names)
lapply(sub$dy, names)


                                    
write_csv(csd, "health_metrics_data_2020-06-03.csv")
