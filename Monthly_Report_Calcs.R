
# Monthly_Report_Calcs.R

setwd(file.path(dirname(path.expand("~")), "Code", "GDOT", "GDOT-Flexdashboard-Report"))

source("Monthly_Report_Functions.R")

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#
start_date <- "2017-07-01"
end_date <- "2018-06-30"
#-----------------------------------------------------------------------------#

sl_fn <- "../SIGNALS_LIST_all_RTOP.txt"
signals_list <- read.csv(sl_fn, col.names = "SignalID", colClasses = c("character"))$SignalID

#Signals to exclude in June 2018 (and months prior):
signals_to_exclude <- c("248", "1389", "3391", "3491",
                        "6329", "6330", "6331", "6347", "6350", "6656", "6657",
                        "7063", "7287", "7289", "7292", "7293", "7542",
                        "71000", "78296",
                        as.character(seq(1600, 1799)),
                        "7024", "7025")
signals_list <- setdiff(signals_list, signals_to_exclude)

#signals_df <- read.csv("../Signals_2018-05-16.csv") %>% as_tibble()
#signals_list <- signals_df$SignalID

#conn <- dbConnect(odbc::odbc(), dsn = "sqlodbc", uid = Sys.getenv("ATSPM_USERNAME"), pwd = Sys.getenv("ATSPM_PASSWORD"))
#dbWriteTable(conn, "signals_list", sldf, overwrite = TRUE)
#dbSendQuery(conn, "create clustered index signals_list_idx0 on signals_list(SignalID)")

get_dates_for_filenames <- function(start_date, end_date) {
    sd <- ymd(start_date)
    ed <- ymd(end_date)

    sy <- year(sd)
    sm <- month(sd)

    ey <- year(ed)
    em <- month(ed)

    s <- paste(sy, sprintf("%02d", sm), sep = "-")

    if (sm != em) {
        e <- paste(ey, sprintf("%02d", em), sep = "-")
        s <- paste(s, e, sep = "_")
    }
    s
}

dates <- seq(ymd(start_date), ymd(end_date), by = "1 month")
month_abbrs <- sapply(dates, function(x) { get_dates_for_filenames(x, x) })

# # GET CORRIDORS #############################################################

# corr_fn <- "c:/Users/V0010894/Code/GDOT/Master RTOP Counts File_2018-06-29.xlsx"
#
# corridors <- get_corridors(corr_fn)
# write_feather(corridors, "corridors.feather")

corridors <- feather::read_feather("corridors.feather")




# # ###########################################################################

# # GET RAW COUNTS FOR THROUGHPUT, VEHICLES/DAY, VEHICLES/HOUR ################
get_raw_15min_counts_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    start_dates <- start_dates[wday(start_dates) %in% c(TUE, WED, THU)]

    cl <- makeCluster(4)
    clusterExport(cl, c("get_15min_counts",
                        "get_counts",
                        "write_fst_",
                        "start_date",
                        "signals_list",
                        "end_date"))
    parLapply(cl, start_dates, function(start_date_) {
        library(DBI)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        library(fst)
        library(purrr)

        end_date_ <- start_date_

        counts <- get_15min_counts(start_date_, end_date_, signals_list)
        if (nrow(counts) > 0) {
            write_fst_(counts, paste0("counts_15min_TWR_", start_date_, ".fst"), append = TRUE)
        }

    })
    stopCluster(cl)
}

get_raw_15min_counts_date_range(start_date, end_date)


# # GET COUNTS FOR DETECTOR UPTIME ############################################
get_raw_1hr_counts_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    cl <- makeCluster(4)
    clusterExport(cl, c("get_1hr_counts",
                        "get_counts",
                        "write_fst_",
                        "start_date",
                        "end_date",
                        "signals_list"))
    counts_groups <- parLapply(cl, start_dates, function(start_date) {
        library(DBI)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        library(fst)
        library(purrr)

        end_date <- start_date

        counts <- get_1hr_counts(start_date, end_date, signals_list)
        write_fst_(counts, paste0("counts_1hr_", start_date, ".fst"), append = TRUE)

    })
    stopCluster(cl)

}
get_raw_1hr_counts_date_range(start_date, end_date)

# --- Everything up to here needs the ATSPM Database ---


# THIS IS NEEDED FOR ALL RTOP (AND SRTOP) INTERSECTIONS
# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages

lapply(month_abbrs, function(x) {
    month_pattern <- paste0("counts_15min_TWR_", x, "-\\d\\d?\\.fst")
    fns <- list.files(pattern = month_pattern)

    print(fns)

    raw_counts_15min <- bind_rows(lapply(fns, read_fst))

    # Filter and Adjust (interpolate) 15 min Counts
    filtered_counts_15min <- get_filtered_counts(raw_counts_15min, interval = "15 min")
    adjusted_counts_15min <- get_adjusted_counts(filtered_counts_15min)

    # Calculate and write Throughput
    throughput <- get_thruput(mutate(adjusted_counts_15min, Date = date(Timeperiod)))
    write_fst(throughput, paste0("tp_", x, ".fst"))
})

# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

lapply(month_abbrs, function(yyyy_mm) {
    month_pattern <- paste0("counts_1hr_", yyyy_mm, "-\\d\\d?\\.fst")
    fns <- list.files(pattern = month_pattern)

    print(fns)

    raw_counts_1hr <- bind_rows(lapply(fns, function(x) {
        read_fst(x) %>% filter(SignalID %in% signals_list)}))

    filtered_counts_1hr <- get_filtered_counts(raw_counts_1hr, interval = "1 hour")
    write_fst_(filtered_counts_1hr, paste0("filtered_counts_1hr_", yyyy_mm, ".fst"))
    rm(raw_counts_1hr)

    adjusted_counts_1hr <- get_adjusted_counts(filtered_counts_1hr)
    write_fst_(adjusted_counts_1hr, paste0("adjusted_counts_1hr_", yyyy_mm, ".fst"))
    rm(adjusted_counts_1hr)

    bad_detectors <- get_bad_detectors(filtered_counts_1hr)
    rm(filtered_counts_1hr)

    write_fst(bad_detectors, paste0("bad_detectors_", yyyy_mm, ".fst"))
})

# --- This needs the ATSPM database ---
conn <- dbConnect(odbc::odbc(),
                  dsn = "sqlodbc",
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
lapply(month_abbrs, function(yyyy_mm) {
    print(yyyy_mm)
    bad_detectors <- read_fst(paste0("bad_detectors_", yyyy_mm, ".fst"))
    # Need to be carefule with this to prevent duplicates
    dbWriteTable(conn, "BadDetectors", bad_detectors, append = TRUE)
})



# -- Run etl_dashboard (Python) to S3/Athena

# --- ----------------------------- ---




lapply(month_abbrs, function(yyyy_mm) {

    print(yyyy_mm)

    adjusted_counts_1hr <- read_fst(paste0("adjusted_counts_1hr_", yyyy_mm, ".fst"))

    # VPD
    vpd <- get_vpd(adjusted_counts_1hr) # calculate over current period
    write_fst(vpd, paste0("vpd_", yyyy_mm, ".fst"))

    # VPH
    vph <- get_vph(adjusted_counts_1hr)
    write_fst(vph, paste0("vph_", yyyy_mm, ".fst"))


    filtered_counts_1hr <- read_fst(paste0("filtered_counts_1hr_", yyyy_mm, ".fst"))

    # DAILY DETECTOR UPTIME
    daily_detector_uptime <- get_daily_detector_uptime(filtered_counts_1hr)
    ddu <- bind_rows(daily_detector_uptime)
    write_fst_(ddu, paste0("ddu_", yyyy_mm, ".fst"))

    # COMMUNICATIONS UPTIME
    comm_uptime <- get_comm_uptime(filtered_counts_1hr)
    write_fst(comm_uptime, paste0("cu_", yyyy_mm, ".fst"))

})



# # GET ARRIVALS ON GREEN #####################################################
get_aog_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 month") #by = "1 day")
    cl <- makeCluster(3)
    clusterExport(cl, c("get_cycle_data",
                        "get_spm_data",
                        "get_spm_data_aws",
                        "write_fst_",
                        "get_aog",
                        "signals_list",
                        "end_date"))
    parLapply(cl, start_dates, function(start_date) {
        library(DBI)
        library(RJDBC)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        library(fst)
        library(purrr)

        end_date <- as.character(ymd(start_date) + months(1) - days(1))

        cycle_data <- get_cycle_data(start_date, end_date, signals_list)
        if (nrow(collect(head(cycle_data))) > 0) {
            aog <- get_aog(cycle_data)
            write_fst_(aog, paste0("aog_", substr(ymd(start_date), 1, 7), ".fst"), append = FALSE)

        }
    })
    stopCluster(cl)
}


get_aog_date_range(start_date, end_date)

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 month")
    cl <- makeCluster(3)
    clusterExport(cl, c("get_detection_events",
                        "get_spm_data",
                        "get_spm_data_aws",
                        "write_fst_",
                        "get_qs",
                        "signals_list",
                        "end_date"))
    parLapply(cl, start_dates, function(start_date) {
        library(DBI)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        library(fst)

        #end_date <- start_date
        end_date <- as.character(ymd(start_date) + months(1) - days(1))

        detection_events <- get_detection_events(start_date, end_date, signals_list)
        if (nrow(collect(head(detection_events))) > 0) {
            qs <- get_qs(detection_events)
            write_fst_(qs, paste0("qs_", substr(ymd(start_date), 1, 7), ".fst"), append = FALSE)
        }
    })
    stopCluster(cl)
}

get_queue_spillback_date_range(start_date, end_date)



# # GET SPLIT FAILURES ########################################################

get_split_failures_date_range <- function(start_date, end_date) {

    # Run Python script to get split failures
    system(paste("python", "split_failures2.py", start_date, end_date, sl_fn))
}

get_split_failures_date_range(ymd(start_date), ymd(end_date))



# # TRAVEL TIME AND BUFFER TIME INDEXES #######################################

fs <- list.files(path = "Inrix/For_Monthly_Report", recursive = TRUE, full.names = TRUE)
fns <- fs[grepl("TWTh.csv", fs)]
tt <- get_tt_csv(fns)
tti <- tt$tti
pti <- tt$pti


write_fst(tti, "tti.fst")
write_fst(pti, "pti.fst")


# # BLUETOAD  UPTIME ##########################################################

# bt_data <- readr::read_csv("bt_all.csv") %>% select(-X1)
# bt_names <- readr::read_csv("btnames.csv")
#
# bt <- left_join(bt_data, bt_names)
#
# daily_bt_uptime <- bt %>%
#     group_by(Zone_Group, Zone, Corridor = Group, date(Timestamp)) %>%
#     summarize(uptime = sum(Up)/n(), num = n()) %>%
#     mutate(delta = uptime - lag(uptime))
#
# monthly_bt_uptime <- bt %>%
#     group_by(Zone_Group, Zone, Corridor = Group, Month = date(Timestamp) - days(day(Timestamp) - 1)) %>%
#     summarize(uptime = sum(Up)/n(), num = n()) %>%
#     mutate(delta = uptime - lag(uptime)) %>%
#     ungroup()
#
# cor_monthly_bt_uptime <- get_cor_monthly_avg_by_day(monthly_bt_uptime, corridors, "uptime", "num")
#
# saveRDS(daily_bt_uptime, "daily_bt_uptime.rds")
# saveRDS(monthly_bt_uptime, "monthly_bt_uptime.rds")





# # ###########################################################################
f <- function(prefix, month_abbrs) {
    purrr::map(paste0(prefix, month_abbrs, ".fst"), read_fst) %>%
        bind_rows() %>%
        mutate(SignalID = factor(SignalID)) %>%
        as_tibble()
}


# Code to put intersection into a SQLite DB, but it's too large for shinyapps.io
# db <- dbConnect(RSQLite::SQLite(), "counts.db")
#
# fns <- paste0("counts_1hr_", seq(ymd(start_date), ymd(end_date), by = "1 day"), ".fst")
# lapply(fns, function(fn) {
#     df <- read_fst(fn) %>%
#         transmute(Month = as.character(date(Timeperiod) - days(day(Timeperiod) - 1)),
#                   Timeperiod = as.character(Timeperiod),
#                   SignalID = as.integer(SignalID),
#                   Detector = as.integer(Detector),
#                   CallPhase = as.integer(CallPhase),
#                   vol = vol) %>%
#         arrange(SignalID, Detector, Timeperiod)
#     as_tibble()
#
#     dbWriteTable(db, "raw_counts", df, append = TRUE)
# })
# dbSendQuery(db, "CREATE UNIQUE INDEX IF NOT EXISTS rc_idx0 ON raw_counts (Month, Timeperiod ASC, SignalID, Detector)")
#
# rc_ <- tbl(db, "raw_counts")

# # DETECTOR UPTIME ###########################################################

ddu <- f("ddu_", month_abbrs)
daily_detector_uptime <- split(ddu, ddu$setback)

avg_daily_detector_uptime <- get_avg_daily_detector_uptime(daily_detector_uptime)
monthly_detector_uptime <- get_monthly_detector_uptime(avg_daily_detector_uptime)

cor_avg_daily_detector_uptime <- get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)
cor_monthly_detector_uptime <- get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)

saveRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds")
saveRDS(monthly_detector_uptime, "monthly_detector_uptime.rds")

saveRDS(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds")
saveRDS(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds")

rm(ddu)

# GET COMMUNICATIONS UPTIME ###################################################

cu <- f("cu_", month_abbrs)
daily_comm_uptime <- get_daily_avg(cu, "uptime")
cor_daily_comm_uptime <- get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")

weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime")
cor_weekly_comm_uptime <- get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")

monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime")
cor_monthly_comm_uptime <- get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")


saveRDS(daily_comm_uptime, "daily_comm_uptime.rds")
saveRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds")

saveRDS(weekly_comm_uptime, "weekly_comm_uptime.rds")
saveRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds")

saveRDS(monthly_comm_uptime, "monthly_comm_uptime.rds")
saveRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds")

rm(cu)

# DAILY VOLUMES ###############################################################

vpd <- f("vpd_", month_abbrs)
weekly_vpd <- get_weekly_vpd(vpd)

# Group into corridors --------------------------------------------------------
cor_weekly_vpd <- get_cor_weekly_vpd(weekly_vpd, corridors)

# Monthly volumes for bar charts and % change ---------------------------------
monthly_vpd <- get_monthly_vpd(vpd)

# Group into corridors
cor_monthly_vpd <- get_cor_monthly_vpd(monthly_vpd, corridors)

# Monthly % change from previous month by corridor ----------------------------
saveRDS(weekly_vpd, "weekly_vpd.rds")
saveRDS(monthly_vpd, "monthly_vpd.rds")
saveRDS(cor_weekly_vpd, "cor_weekly_vpd.rds")
saveRDS(cor_monthly_vpd, "cor_monthly_vpd.rds")

rm(vpd)

# HOURLY VOLUMES ##############################################################

vph <- f("vph_", month_abbrs)
weekly_vph <- get_weekly_vph(mutate(vph, CallPhase = 2)) # Hack because next function needs a CallPhase
weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)

# Group into corridors --------------------------------------------------------
cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)

monthly_vph <- get_monthly_vph(vph)
monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)

# Hourly volumes by Corridor --------------------------------------------------
cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)

saveRDS(weekly_vph, "weekly_vph.rds")
saveRDS(monthly_vph, "monthly_vph.rds")
saveRDS(cor_weekly_vph, "cor_weekly_vph.rds")
saveRDS(cor_monthly_vph, "cor_monthly_vph.rds")

saveRDS(weekly_vph_peak, "weekly_vph_peak.rds")
saveRDS(monthly_vph_peak, "monthly_vph_peak.rds")
saveRDS(cor_weekly_vph_peak, "cor_weekly_vph_peak.rds")
saveRDS(cor_monthly_vph_peak, "cor_monthly_vph_peak.rds")

rm(vph)

# DAILY THROUGHPUT ############################################################

throughput <- f("tp_", month_abbrs)
weekly_throughput <- get_weekly_thruput(throughput)

# Group into corridors --------------------------------------------------------
cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)

# Monthly throughput for bar charts and % change ---------------------------------
monthly_throughput <- get_monthly_thruput(throughput)

# Group into corridors
cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)

# Monthly % change from previous month by corridor ----------------------------
#cor_mo_pct_throughput <- get_cor_monthly_pct_change_thruput(cor_monthly_throughput)

saveRDS(weekly_throughput, "weekly_throughput.rds")
saveRDS(monthly_throughput, "monthly_throughput.rds")
saveRDS(cor_weekly_throughput, "cor_weekly_throughput.rds")
saveRDS(cor_monthly_throughput, "cor_monthly_throughput.rds")

rm(throughput)

# DAILY ARRIVALS ON GREEN #####################################################

aog <- f("aog_", month_abbrs)
daily_aog <- get_daily_aog(aog)

weekly_aog_by_day <- get_weekly_aog_by_day(aog)

cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)

monthly_aog_by_day <- get_monthly_aog_by_day(aog)

cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)

saveRDS(weekly_aog_by_day, "weekly_aog_by_day.rds")
saveRDS(monthly_aog_by_day, "monthly_aog_by_day.rds")
saveRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds")
saveRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds")

# HOURLY ARRIVALS ON GREEN ####################################################

aog_by_hr <- get_aog_by_hr(aog)
monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)

# Hourly volumes by Corridor --------------------------------------------------
cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)

cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)

saveRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds")
saveRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds")

rm(aog)

# DAILY SPLIT FAILURES #####################################################

sf_filenames <- list.files(pattern = "sf_201\\d-\\d{2}-\\d{2}\\.feather")
wds <- wday(sub(pattern = "sf_(.*)\\.feather", "\\1", sf_filenames))
twr <- sapply(wds, function(x) {x %in% c(TUE, WED, THU)})
sf_filenames <- sf_filenames[twr]
sf <- get_sf(bind_rows(lapply(sf_filenames, read_feather)))

sfs <- split(sf, sf$SignalID)

wsf <- lapply(sfs, get_weekly_sf_by_day) %>%
    bind_rows() %>% ungroup() %>%
    mutate(SignalID = factor(SignalID),
           Week = factor(Week))

cor_wsf <- get_cor_weekly_sf_by_day(wsf, corridors)


monthly_sfd <- lapply(sfs, get_monthly_sf_by_day) %>%
    bind_rows() %>% ungroup() %>%
    mutate(SignalID = factor(SignalID))

cor_monthly_sfd <- get_cor_monthly_sf_by_day(monthly_sfd, corridors)

saveRDS(wsf, "wsf.rds")
saveRDS(monthly_sfd, "monthly_sfd.rds")
saveRDS(cor_wsf, "cor_wsf.rds")
saveRDS(cor_monthly_sfd, "cor_monthly_sfd.rds")


# HOURLY SPLIT FAILURES #######################################################

sfh <- lapply(sfs, get_sf_by_hr) %>%
    bind_rows() %>% ungroup() %>% as_tibble() %>%
    mutate(SignalID = factor(SignalID),
           CallPhase = factor(CallPhase),
           Week = factor(Week),
           DOW = factor(DOW))

msfh <- get_monthly_sf_by_hr(sfh)

# Hourly volumes by Corridor --------------------------------------------------
cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)

saveRDS(msfh, "msfh.rds")
saveRDS(cor_msfh, "cor_msfh.rds")

rm(sf)

# DAILY QUEUE SPILLBACK #######################################################

qs <- f("qs_", month_abbrs)
wqs <- get_weekly_qs_by_day(qs)
cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)

monthly_qsd <- get_monthly_qs_by_day(qs)
cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)

saveRDS(wqs, "wqs.rds")
saveRDS(monthly_qsd, "monthly_qsd.rds")
saveRDS(cor_wqs, "cor_wqs.rds")
saveRDS(cor_monthly_qsd, "cor_monthly_qsd.rds")


# HOURLY QUEUE SPILLBACK ######################################################

qsh <- get_qs_by_hr(qs)
mqsh <- get_monthly_qs_by_hr(qsh)

# Hourly volumes by Corridor --------------------------------------------------
cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)

saveRDS(mqsh, "mqsh.rds")
saveRDS(cor_mqsh, "cor_mqsh.rds")

rm(qs)

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

tti <- read_fst("tti.fst")
pti <- read_fst("pti.fst")

cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, corridors)
cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, corridors)

cor_monthly_tti <- get_cor_monthly_tti(cor_monthly_tti_by_hr, corridors)
cor_monthly_pti <- get_cor_monthly_pti(cor_monthly_pti_by_hr, corridors)

saveRDS(cor_monthly_tti, "cor_monthly_tti.rds")
saveRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds")

saveRDS(cor_monthly_pti, "cor_monthly_pti.rds")
saveRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds")

rm(tti)
rm(pti)


# DETECTOR UPTIME AS REPORTED BY FIELD ENGINEERS ##############################

# # VEH, PED, CCTV UPTIME - AS REPORTED BY FIELD ENGINEERS via EXCEL

fns <- list.files(path = "Excel Monthly Reports/2017_12", recursive = TRUE, full.names = TRUE)
xl_uptime_fns <- c("January Vehicle and Ped Detector Info.xlsx",
                   "February Vehicle and Ped Detector Info.xlsx",
                   "March Vehicle and Ped Detector Info.xlsx",
                   "April Vehicle and Ped Detector Info.xlsx",
                   "May Vehicle and Ped Detector Info.xlsx")
xl_uptime_mos <- c("2018-01-01",
                   "2018-02-01",
                   "2018-03-01",
                   "2018-04-01",
                   "2018-05-01")

mrs_veh_xl <- get_veh_uptime_from_xl_monthly_reports(fns, corridors)
mrs_ped_xl <- get_ped_uptime_from_xl_monthly_reports(fns, corridors)

mrs_cctv_xl <- get_cctv_uptime_from_xl_monthly_reports(fns, corridors)

man_xl <- purrr::map2(xl_uptime_fns,
                      xl_uptime_mos,
                      get_det_uptime_from_manual_xl) %>%
    bind_rows() %>%
    mutate(Zone_Group = factor(Zone_Group))

man_veh_xl <- man_xl %>% filter(Type == "Vehicle") %>% select(-Type)
man_ped_xl <- man_xl %>% filter(Type == "Pedestrian") %>% select(-Type)

# Temporary until we can get manually entered January 2018 data
man_cctv_xl <- filter(mrs_cctv_xl, Month == ymd("2017-12-01")) %>% mutate(Month = Month + months(1))

man_cctv_xl_2018_01 <- read_excel("January Vehicle and Ped Detector Info.xlsx", sheet = "cor_cctv") %>%
    transmute(Zone_Group = Zone_Group,
              Corridor = Corridor,
              Month = ymd("2018-01-01"),
              up = as.integer(up),
              num = as.integer(num),
              uptime = uptime)
man_cctv_xl_2018_02 <- read_excel("February Vehicle and Ped Detector Info.xlsx", sheet = "cor_cctv") %>%
    transmute(Zone_Group = Zone_Group,
              Corridor = Corridor,
              Month = ymd("2018-02-01"),
              up = as.integer(up),
              num = as.integer(num),
              uptime = uptime)

cam_config <- read.csv("../camera_ids.csv") %>% as_tibble() %>%
    separate(col = CamID, into = c("CameraID", "Location"), sep = ": ")

num_cams <- cam_config %>% group_by(Corridor) %>% summarize(num = n())

cctv_511 <- read_feather("../parsed_cctv.feather") %>%
    filter(Date > "2018-02-01") %>%
    left_join(cam_config) %>%
    select(-Location) %>%
    group_by(Corridor, Date) %>%
    summarize(up = sum(Size != 0), num = n()) %>%
    mutate(uptime = up/num) %>%
    left_join(distinct(corridors, Corridor, Zone_Group))

cctv_uptime <- bind_rows(mrs_cctv_xl, man_cctv_xl_2018_01, man_cctv_xl_2018_02, cctv_511) #%>%

cctv_uptime$Date[is.na(cctv_uptime$Date)] <- cctv_uptime$Month[is.na(cctv_uptime$Date)]

monthly_cctv_uptime <- cctv_uptime %>%
    mutate(Month = Date - days(day(Date) - 1)) %>%
    select(-up, -num) %>%
    left_join(num_cams) %>%
    mutate(up = uptime * num) %>% group_by(Zone_Group, Corridor, Month) %>%
    summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
              up = sum(up, na.rm = TRUE),
              num = sum(num, na.rm = TRUE)) %>%
    ungroup()

saveRDS(monthly_cctv_uptime, "monthly_cctv_uptime.rds")

cor_monthly_xl_veh_uptime <- get_cor_monthly_xl_uptime(bind_rows(mrs_veh_xl, man_veh_xl))
cor_monthly_xl_ped_uptime <- get_cor_monthly_xl_uptime(bind_rows(mrs_ped_xl, man_ped_xl))
cor_monthly_xl_cctv_uptime <- get_cor_monthly_xl_uptime(monthly_cctv_uptime)
cor_daily_xl_cctv_uptime <- get_cor_monthly_xl_uptime(mutate(cctv_uptime, Month = Date)) %>%
    mutate(Date = Month,
           Zone_Group = factor(Zone_Group),
           Corridor = factor(Corridor)) %>% select(-Month)

saveRDS(cor_monthly_xl_veh_uptime, "cor_monthly_xl_veh_uptime.rds")
saveRDS(cor_monthly_xl_ped_uptime, "cor_monthly_xl_ped_uptime.rds")
saveRDS(cor_monthly_xl_cctv_uptime, "cor_monthly_xl_cctv_uptime.rds")
saveRDS(cor_daily_xl_cctv_uptime, "cor_daily_xl_cctv_uptime.rds")




# ACTIVITIES

csv_fns <- c("Teams Tasks Reported July-November_simpl.csv",
             "Teams Tasks Reported December.csv",
             "January TEAMS Data.csv",
             "February TEAMS Data.csv",
             "March Teams Data.csv",
             "April 2018 Teams Data2.csv",
             "May Teams Tasks.csv",
             "June RTOP (1-6) Reported Teams Tasks.csv")
             #"June Zone 7 Reported Teams Tasks.csv")
month_dates <- ymd(c("2017-11-01", "2017-12-01",
                     "2018-01-01", "2018-02-01", "2018-03-01", "2018-04-01", "2018-05-01", "2018-06-01")) #, "2018-06-01"))

teams_rtop_ <- purrr::map2(.x = csv_fns,
                           .y = month_dates,
                           ~ read_teams_csv(.x) %>% filter(`Date Reported` < .y + months(1))) %>%
    bind_rows() %>%
    filter(`Date Reported` >= ymd("2017-01-01")) %>%
    mutate(All = factor("all")) %>%
    as_tibble()


teams_rtop = bind_rows(
    mutate(teams_rtop_, Zone_Group = "All RTOP"),
    mutate(teams_rtop_, Zone_Group = "RTOP1"),
    mutate(teams_rtop_, Zone_Group = "RTOP2"))


csv_fns <- c("R1TSO_TEAMSReport_Feb2018.csv",

             "R1TSO_D1_TEAMSReport_Mar2018.csv",
             "R1TSO_D6_TEAMSReport_Mar2018.csv",

             "R1TSO_D1_TEAMSReport_Apr2018.csv",
             "R1TSO_D6_TEAMSReport_Apr2018.csv",

             "R1TSO_D1_TEAMSReport_May2018.csv",
             "R1TSO_D6_TEAMSReport_May2018.csv")

             #"R1TSO_D1_TEAMSReport_June2018.csv",
             #"R1TSO_D6_TEAMSReport_June2018.csv")

month_dates <- ymd(c("2018-02-01",
                     "2018-03-01", "2018-03-01",
                     "2018-04-01", "2018-04-01",
                     "2018-05-01", "2018-05-01"))
                     #"2018-06-01", "2018-06-01"))

teams_r16 <- purrr::map2(.x = csv_fns,
                         .y = month_dates,
                         ~ read_teams_csv(.x) %>%
                             filter(`Date Reported` < .y + months(1)) %>%
                             mutate(Zone_Group = Maintained_by))  %>%
    bind_rows() %>%
    filter(`Date Reported` >= ymd("2017-01-01")) %>%
    mutate(All = factor("all")) %>%
    as_tibble()


teams <- bind_rows(teams_rtop, teams_r16)

saveRDS(teams, "teams.rds")
#------------------------------------------------------------------------------
teams <- readRDS("teams.rds")


type_table <- get_outstanding_events(teams, "Task_Type") %>%
    mutate(Task_Type = if_else(Task_Type == "", "Unknown", Task_Type)) %>%
    group_by(Zone_Group, Task_Type, Month) %>%
    summarize_all(sum)

subtype_table <- get_outstanding_events(teams, "Task_Subtype") %>%
    mutate(Task_Subtype = if_else(Task_Subtype == "", "Unknown", Task_Subtype)) %>%
    group_by(Zone_Group, Task_Subtype, Month) %>%
    summarize_all(sum)

source_table <- get_outstanding_events(teams, "Task_Source") %>%
    mutate(Task_Source = if_else(Task_Source == "", "Unknown", Task_Source)) %>%
    group_by(Zone_Group, Task_Source, Month) %>%
    summarize_all(sum)

priority_table <- get_outstanding_events(teams, "Priority") %>%
    group_by(Zone_Group, Priority, Month) %>%
    summarize_all(sum)

all_teams_table <- get_outstanding_events(teams, "All") %>%
    group_by(Zone_Group, All, Month) %>%
    summarize_all(sum)

teams_tables <- list("type" = type_table,
                     "subtype" = subtype_table,
                     "source" = source_table,
                     "priority" = priority_table,
                     "all" = all_teams_table)

saveRDS(teams_tables, "teams_tables.rds")


cor_monthly_events <- teams_tables$all %>%
    ungroup() %>%
    transmute(Corridor = Zone_Group,
              Zone_Group = Zone_Group,
              Month = Month,
              Reported = Rep,
              Resolved = Res,
              Outstanding = outstanding) %>%
    arrange(Corridor, Zone_Group, Month) %>%
    group_by(Corridor, Zone_Group) %>%
    mutate(delta.rep = (Reported - lag(Reported))/lag(Reported),
           delta.res = (Resolved - lag(Resolved))/lag(Resolved),
           delta.out = (Outstanding - lag(Outstanding))/lag(Outstanding))

saveRDS(cor_monthly_events, "cor_monthly_events.rds")



# Package up for Flexdashboard

sigify <- function(df, cor_df, corridors) {

    df_ <- df %>% left_join(distinct(corridors, SignalID, Corridor, Name)) %>%
        rename(Zone_Group = Corridor, Corridor = SignalID) %>%
        ungroup() %>%
        mutate(Corridor = factor(Corridor))

    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Zone_Group)) %>%
        mutate(Zone_Group = Corridor)

    br <- bind_rows(df_, cor_df_)

    if ("Month" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Hour)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Date)
    }
}



cor <- list()
cor$dy <- list("du" = readRDS("cor_avg_daily_detector_uptime.rds"),
               "cu" = readRDS("cor_daily_comm_uptime.rds"),
               "cctv" = readRDS("cor_daily_xl_cctv_uptime.rds"))
cor$wk <- list("vpd" = readRDS("cor_weekly_vpd.rds"),
               "vph" = readRDS("cor_weekly_vph.rds"),
               "vphp" = readRDS("cor_weekly_vph_peak.rds"),
               "tp" = readRDS("cor_weekly_throughput.rds"),
               "aog" = readRDS("cor_weekly_aog_by_day.rds"),
               "qs" = readRDS("cor_wqs.rds"),
               "sf" = readRDS("cor_wsf.rds"),
               "cu" = readRDS("cor_weekly_comm_uptime.rds"))
cor$mo <- list("vpd" = readRDS("cor_monthly_vpd.rds"),
               "vph" = readRDS("cor_monthly_vph.rds"),
               "vphp" = readRDS("cor_monthly_vph_peak.rds"),
               "tp" = readRDS("cor_monthly_throughput.rds"),
               "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
               "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
               "qsd" = readRDS("cor_monthly_qsd.rds"),
               "qsh" = readRDS("cor_mqsh.rds"),
               "sfd" = readRDS("cor_monthly_sfd.rds"),
               "sfh" = readRDS("cor_msfh.rds"),
               "tti" = readRDS("cor_monthly_tti.rds"),
               "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
               "pti" = readRDS("cor_monthly_pti.rds"),
               "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
               "du" = readRDS("cor_monthly_detector_uptime.rds"),
               "cu" = readRDS("cor_monthly_comm_uptime.rds"),
               "veh" = readRDS("cor_monthly_xl_veh_uptime.rds"),
               "ped" = readRDS("cor_monthly_xl_ped_uptime.rds"),
               "cctv" = readRDS("cor_monthly_xl_cctv_uptime.rds"),
               "events" = readRDS("cor_monthly_events.rds"))
cor$qu <- list("vpd" = get_quarterly(cor$mo$vpd, "vpd"),
               "vph" = data.frame(), #get_quarterly(cor$mo$vph, "vph"),
               "vphpa" = get_quarterly(cor$mo$vphp$am, "vph"),
               "vphpp" = get_quarterly(cor$mo$vphp$pm, "vph"),
               "tp" = get_quarterly(cor$mo$tp, "vph"),
               "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
               "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
               "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
               "tti" = get_quarterly(cor$mo$tti, "tti"),
               "pti" = get_quarterly(cor$mo$pti, "pti"),
               "du" = get_quarterly(cor$mo$du, "uptime.all"),
               "cu" = get_quarterly(cor$mo$cu, "uptime"),
               "veh" = get_quarterly(cor$mo$veh, "uptime", "num"),
               "ped" = get_quarterly(cor$mo$ped, "uptime", "num"),
               "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
               "reported" = get_quarterly(cor$mo$events, "Reported"),
               "resolved" =  get_quarterly(cor$mo$events, "Resolved"),
               "outstanding" = get_quarterly(cor$mo$events, "Outstanding", operation = "sum"))

sig <- list()
sig$dy <- list("du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors),
               "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors))
sig$wk <- list("vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors),
               "vph" = sigify(readRDS("weekly_vph.rds"), cor$wk$vph, corridors),
               "vphp" = purrr::map2(readRDS("weekly_vph_peak.rds"), cor$wk$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors),
               "aog" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aog, corridors),
               "qs" = sigify(readRDS("wqs.rds"), cor$wk$qs, corridors),
               "sf" = sigify(readRDS("wsf.rds"), cor$wk$sf, corridors),
               "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors))
sig$mo <- list("vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors),
               "vph" = sigify(readRDS("monthly_vph.rds"), cor$mo$vph, corridors),
               "vphp" = purrr::map2(readRDS("monthly_vph_peak.rds"), cor$mo$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors),
               "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors),
               "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors),
               "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors),
               "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors),
               "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors),
               "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors),
               "tti" = data.frame(),
               "pti" = data.frame(),
               "du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$mo$du, corridors),
               "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors),
               "cctv" = readRDS("monthly_cctv_uptime.rds"))

saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")


# Bring April data back from April Report.
#  Because the database went wonky.
# ---- Loop through all filenames and cor/sig objects from April
cor4 <- readRDS("2018-04 Report Files/cor.rds")
sig4 <- readRDS("2018-04 Report Files/sig.rds")

cor$dy$du <- patch_april(cor$dy$du, cor4$dy$du)
cor$dy$cu <- patch_april(cor$dy$cu, cor4$dy$cu)
cor$wk$vpd <- patch_april(cor$wk$vpd, cor4$wk$vpd)
cor$wk$vph <- patch_april(cor$wk$vph, cor4$wk$vph)
cor$wk$vphp <- patch_april(cor$wk$vphp, cor4$wk$vphp)
cor$wk$tp <- patch_april(cor$wk$tp, cor4$wk$tp)
cor$wk$aog <- patch_april(cor$wk$aog, cor4$wk$aog)
cor$wk$qs <- patch_april(cor$wk$qs, cor4$wk$qs)
cor$wk$sf <- patch_april(cor$wk$sf, cor4$wk$sf)
cor$wk$cu <- patch_april(cor$wk$cu, cor4$wk$cu)
sig$dy$du <- patch_april(sig$dy$du, sig4$dy$du)
sig$dy$cu <- patch_april(sig$dy$cu, sig4$dy$cu)
sig$wk$vpd <- patch_april(sig$wk$vpd, sig4$wk$vpd)
sig$wk$vph <- patch_april(sig$wk$vph, sig4$wk$vph)
sig$wk$vphp <- patch_april(sig$wk$vphp, sig4$wk$vphp)
sig$wk$tp <- patch_april(sig$wk$tp, sig4$wk$tp)
sig$wk$aog <- patch_april(sig$wk$aog, sig4$wk$aog)
sig$wk$qs <- patch_april(sig$wk$qs, sig4$wk$qs)
sig$wk$sf <- patch_april(sig$wk$sf, sig4$wk$sf)
sig$wk$cu <- patch_april(sig$wk$cu, sig4$wk$cu)
cor$mo$vpd <- patch_april(cor$mo$vpd, cor4$mo$vpd)
cor$mo$vph <- patch_april(cor$mo$vph, cor4$mo$vph)
cor$mo$vphp <- patch_april(cor$mo$vphp, cor4$mo$vphp)
cor$mo$tp <- patch_april(cor$mo$tp, cor4$mo$tp)
cor$mo$aogd <- patch_april(cor$mo$aogd, cor4$mo$aogd)
cor$mo$aogh <- patch_april(cor$mo$aogh, cor4$mo$aogh)
cor$mo$qsd <- patch_april(cor$mo$qsd, cor4$mo$qsd)
cor$mo$qsh <- patch_april(cor$mo$qsh, cor4$mo$qsh)
cor$mo$sfd <- patch_april(cor$mo$sfd, cor4$mo$sfd)
cor$mo$sfh <- patch_april(cor$mo$sfh, cor4$mo$sfh)
cor$mo$du <- patch_april(cor$mo$du, cor4$mo$du)
cor$mo$cu <- patch_april(cor$mo$cu, cor4$mo$cu)
sig$mo$vpd <- patch_april(sig$mo$vpd, sig4$mo$vpd)
sig$mo$vph <- patch_april(sig$mo$vph, sig4$mo$vph)
sig$mo$vphp <- patch_april(sig$mo$vphp, sig4$mo$vphp)
sig$mo$tp <- patch_april(sig$mo$tp, sig4$mo$tp)
sig$mo$aogd <- patch_april(sig$mo$aogd, sig4$mo$aogd)
sig$mo$aogh <- patch_april(sig$mo$aogh, sig4$mo$aogh)
sig$mo$qsd <- patch_april(sig$mo$qsd, sig4$mo$qsd)
sig$mo$qsh <- patch_april(sig$mo$qsh, sig4$mo$qsh)
sig$mo$sfd <- patch_april(sig$mo$sfd, sig4$mo$sfd)
sig$mo$sfh <- patch_april(sig$mo$sfh, sig4$mo$sfh)
sig$mo$du <- patch_april(sig$mo$du, sig4$mo$du)
sig$mo$cu <- patch_april(sig$mo$cu, sig4$mo$cu)


saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")
