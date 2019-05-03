
# Monthly_Report_Package.R

library(yaml)
library(glue)

print(glue("{Sys.time()} Starting Package Script"))


if (Sys.info()["sysname"] == "Windows") {
    working_directory <- file.path(dirname(path.expand("~")), "Code", "GDOT", "GDOT-Flexdashboard-Report")
    
} else if (Sys.info()["sysname"] == "Linux") {
    working_directory <- file.path("~", "Code", "GDOT", "GDOT-Flexdashboard-Report")
    
} else {
    stop("Unknown operating system.")
}
setwd(working_directory)

source("Monthly_Report_Functions.R")
conf <- read_yaml("Monthly_Report.yaml")

# startsWith(Sys.info()["nodename"], "ip-") # check for if on AWS EC2 instance

corridors <- feather::read_feather(conf$corridors_filename) 
signals_list <- corridors$SignalID[!is.na(corridors$SignalID)]
all_corridors <- read_feather("all_corridors.feather")

conn <- get_athena_connection()

#system("aws s3 sync s3://gdot-spm/mark MARK --exclude *counts_*")

# # ###########################################################################

#f3 <- pqlib$read_parquet

# f2 <- function(table_name, start_date, end_date, signals_list = NULL) {
#     df <- tbl(conn, sql(glue("SELECT * from gdot_spm.{table_name}"))) %>%
#         filter(between(date, start_date, end_date))
# 
#     if (!is.null(signals_list)) {
#         df <- df %>% filter(signalid %in% signals_list)
#     }
#     df <- collect(df)
#     cols <- names(df)
#     if ("signalid" %in% cols) { 
#         df <- mutate(df, SignalID = factor(signalid)) %>% select(-signalid)
#     }
#     if ("callphase" %in% cols) { 
#         df <- mutate(df, CallPhase = factor(callphase)) %>% select(-callphase)
#     }
#     if ("date" %in% cols) { 
#         df <- mutate(df, Date = date(date)) %>% select(-date)
#     }
#     if ("date_hour" %in% cols) { 
#         df <- mutate(df, Date_Hour = ymd_hms(date_hour)) %>% select(-date_hour)
#     }
#     if ("setback" %in% cols) { 
#         df <- mutate(df, setback = factor(setback)) }
#     if ("week" %in% cols) { 
#         df <- mutate(df, Week = as.integer(week)) %>% select(-week)
#     }
#     if ("dow" %in% cols) { 
#         df <- mutate(df, DOW = as.integer(dow)) %>% select(-dow)
#     }
#     if ("timefromstopbar" %in% cols) { 
#         df <- mutate(df, TimeFromStopBar = factor(timefromstopbar)) %>% select(-timefromstopbar)
#     }
#     df
# }
# 
# f <- function(prefix, month_abbrs, daily = FALSE, combine = TRUE, signals_list = NULL) {
#     
#     x <- mclapply(month_abbrs, function(month_abbr) {
#         if (daily == TRUE) {
#             month_abbr <- paste0(month_abbr, "-")
#         }
#         fns <- aws.s3::get_bucket_df("gdot-spm", prefix = glue("SAM/{prefix}{month_abbr}"))$Key
#         #print(fns)
#         
#         if (length(fns) > 0) {
#             if (!is.null(signals_list)) {
#                 xx <- lapply(fns, function(fn) { 
#                     aws.s3::save_object(fn, bucket = "gdot-spm", file = basename(fn))
#                     read_fst(basename(fn)) %>% filter(SignalID %in% signals_list)
#                 }) %>% bind_rows() %>% as_tibble()
#             } else {
#                 xx <- lapply(fns, function(fn) {
#                     aws.s3::save_object(fn, bucket = "gdot-spm", file = basename(fn))
#                     read_fst(basename(fn))
#                 }) %>% bind_rows() %>% as_tibble()
#             }
# 		} else {
# 		    xx <- data.frame()
# 		}
#         lapply(basename(fns), file.remove)
#         xx
#     })
# 
#     if (combine == TRUE) {
#         x <- bind_rows(x) %>% 
#             mutate(SignalID = factor(SignalID)) %>% 
#             as_tibble()
#         #x <- bind_rows_keep_factors(x)
#     }
#     x
# }

# # Package everything up for Monthly Report back 13 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_start_date <- conf$report_start_date
if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}
dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

report_start_date <- as.character(report_start_date)
report_end_date <- as.character(report_end_date)
print(month_abbrs)

date_range <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 day")
date_range_str <- paste0("{", paste0(as.character(date_range), collapse=","), "}")


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 20]"))

tryCatch({
    #ddu <- f("ddu_", month_abbrs, signals_list = signals_list)
    #ddu <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'detector_uptime_pd', report_start_date, report_end_date, signals_list)
    
    #keys <- glue("s3a://gdot-spm/mark/detector_uptime_pd/date={date_range_str}/*")
    #ddu <- spark_read_parquet(sc, name = "ddu", path = keys)
    
    avg_daily_detector_uptime <- lapply(month_abbrs, function(yyyy_mm) {
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(report_end_date))
        
        s3_read_parquet_parallel('detector_uptime_pd', as.character(sd), as.character(ed), signals_list, 'gdot-spm') %>% 
            get_avg_daily_detector_uptime() %>%
            mutate(Date = date(Date))
    }) %>% bind_rows()
    
    #avg_daily_detector_uptime <- get_avg_daily_detector_uptime(ddu) %>% mutate(Date = date(Date))
    cor_avg_daily_detector_uptime <- get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)
    
    monthly_detector_uptime <- get_monthly_detector_uptime(avg_daily_detector_uptime)
    cor_monthly_detector_uptime <- get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)
    
    saveRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds")
    saveRDS(monthly_detector_uptime, "monthly_detector_uptime.rds")
    
    saveRDS(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds")
    saveRDS(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds")
    
    #rm(ddu)
    #rm(daily_detector_uptime)
    rm(avg_daily_detector_uptime)
    rm(monthly_detector_uptime)
    rm(cor_avg_daily_detector_uptime)
    rm(cor_monthly_detector_uptime)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

# DAILY PEDESTRIAN DETECTOR UPTIME ###############################################

print(glue("{Sys.time()} Ped Detector Uptime [2 of 20]"))

tryCatch({
    #papd <- f("papd_", month_abbrs, signals_list = signals_list) %>% rename(papd = vpd)
    papd <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'ped_actuations_pd', report_start_date, report_end_date, signals_list) %>%
        replace_na((list(CallPhase = 0))) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date))
    
    pau <- get_pau(papd)
    
    # pau <- lapply(month_abbrs, function(yyyy_mm) {
    #     sd <- ymd(paste0(yyyy_mm, "-01"))
    #     ed <- sd + months(1) - days(1)
    #     ed <- min(ed, ymd(report_end_date))
    #     
    #     df <- s3_read_parquet_parallel('ped_actuations_pd', as.character(sd), as.character(ed), signals_list, 'gdot-spm')
    #     if (nrow(df) > 0) {
    #         df <- df %>%
    #             replace_na((list(CallPhase = 0))) %>%
    #             mutate(SignalID = factor(SignalID),
    #                    CallPhase = factor(CallPhase),
    #                    Date = date(Date)) %>% 
    #             get_pau()
    #     } else {
    #         df <- data.frame()
    #     }
    #     df
    #         
    # }) %>% bind_rows()
    
    daily_pa_uptime <- get_daily_avg(pau, "uptime", peak_only = FALSE)
    cor_daily_pa_uptime <- get_cor_weekly_avg_by_day(daily_pa_uptime, corridors, "uptime")
    
    weekly_pa_uptime <- get_weekly_avg_by_day(pau, "uptime", peak_only = FALSE)
    cor_weekly_pa_uptime <- get_cor_weekly_avg_by_day(weekly_pa_uptime, corridors, "uptime")
    
    monthly_pa_uptime <- get_monthly_avg_by_day(pau, "uptime", "all", peak_only = FALSE)
    cor_monthly_pa_uptime <- get_cor_monthly_avg_by_day(monthly_pa_uptime, corridors, "uptime")
    
    saveRDS(pau, "pa_uptime.rds")
    
    saveRDS(daily_pa_uptime, "daily_pa_uptime.rds")
    saveRDS(cor_daily_pa_uptime, "cor_daily_pa_uptime.rds")
    
    saveRDS(weekly_pa_uptime, "weekly_pa_uptime.rds")
    saveRDS(cor_weekly_pa_uptime, "cor_weekly_pa_uptime.rds")
    
    saveRDS(monthly_pa_uptime, "monthly_pa_uptime.rds")
    saveRDS(cor_monthly_pa_uptime, "cor_monthly_pa_uptime.rds")
    
    rm(papd)
    rm(pau)
    rm(daily_pa_uptime)
    rm(cor_daily_pa_uptime)
    rm(weekly_pa_uptime)
    rm(cor_weekly_pa_uptime)
    rm(monthly_pa_uptime)
    rm(cor_monthly_pa_uptime)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [3 of 20]"))

tryCatch({
    #cu <- f("cu_", month_abbrs) 
    cu <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'comm_uptime', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date))
    
    daily_comm_uptime <- get_daily_avg(cu, "uptime", peak_only = FALSE) 
    cor_daily_comm_uptime <- get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")
    
    weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime", peak_only = FALSE)
    cor_weekly_comm_uptime <- get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")
    
    monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime", peak_only = FALSE)
    cor_monthly_comm_uptime <- get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")
    
    
    saveRDS(daily_comm_uptime, "daily_comm_uptime.rds")
    saveRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds")
    
    saveRDS(weekly_comm_uptime, "weekly_comm_uptime.rds")
    saveRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds")
    
    saveRDS(monthly_comm_uptime, "monthly_comm_uptime.rds")
    saveRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds")
    
    rm(cu)
    rm(daily_comm_uptime)
    rm(cor_daily_comm_uptime)
    rm(weekly_comm_uptime)
    rm(cor_weekly_comm_uptime)
    rm(monthly_comm_uptime)
    rm(cor_monthly_comm_uptime)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [4 of 20]"))

tryCatch({
    #vpd <- f("vpd_", month_abbrs)
    vpd <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'vehicles_pd', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date))
    
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
    rm(weekly_vpd)
    rm(monthly_vpd)
    rm(cor_weekly_vpd)
    rm(cor_monthly_vpd)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [5 of 20]"))

tryCatch({
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [6 of 20]"))

tryCatch({
    #papd <- f("papd_", month_abbrs)
    papd <- s3_read_parquet(bucket = 'gdot-spm', 'ped_actuations_pd', report_start_date, report_end_date, signals_list) %>%
        replace_na((list(CallPhase = 0))) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date))
    
    weekly_papd <- get_weekly_papd(papd)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)
    
    # Monthly volumes for bar charts and % change ---------------------------------
    monthly_papd <- get_monthly_papd(papd)
    
    # Group into corridors
    cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)
    
    # Monthly % change from previous month by corridor ----------------------------
    saveRDS(weekly_papd, "weekly_papd.rds")
    saveRDS(monthly_papd, "monthly_papd.rds")
    saveRDS(cor_weekly_papd, "cor_weekly_papd.rds")
    saveRDS(cor_monthly_papd, "cor_monthly_papd.rds")
    
    rm(papd)
    rm(weekly_papd)
    rm(monthly_papd)
    rm(cor_weekly_papd)
    rm(cor_monthly_papd)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [7 of 20]"))

tryCatch({
    #paph <- f("paph_", month_abbrs)
    paph <- s3_read_parquet(bucket = 'gdot-spm', 'ped_actuations_ph', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               Date = date(Date))
    
    
    weekly_paph <- get_weekly_paph(mutate(paph, CallPhase = 2)) # Hack because next function needs a CallPhase
    #weekly_paph_peak <- get_weekly_paph_peak(weekly_paph)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_paph <- get_cor_weekly_paph(weekly_paph, corridors)
    #cor_weekly_paph_peak <- get_cor_weekly_vph_peak(cor_weekly_paph)
    
    monthly_paph <- get_monthly_paph(paph)
    #monthly_paph_peak <- get_monthly_vph_peak(monthly_paph)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_paph <- get_cor_monthly_paph(monthly_paph, corridors)
    #cor_monthly_paph_peak <- get_cor_monthly_vph_peak(cor_monthly_paph)
    
    saveRDS(weekly_paph, "weekly_paph.rds")
    saveRDS(monthly_paph, "monthly_paph.rds")
    saveRDS(cor_weekly_paph, "cor_weekly_paph.rds")
    saveRDS(cor_monthly_paph, "cor_monthly_paph.rds")
    
    #saveRDS(weekly_paph_peak, "weekly_paph_peak.rds")
    #saveRDS(monthly_paph_peak, "monthly_paph_peak.rds")
    #saveRDS(cor_weekly_paph_peak, "cor_weekly_paph_peak.rds")
    #saveRDS(cor_monthly_paph_peak, "cor_monthly_paph_peak.rds")
    
    rm(paph)
    rm(weekly_paph)
    rm(monthly_paph)
    rm(cor_weekly_paph)
    rm(cor_monthly_paph)
    #rm(weekly_paph_peak)
    #rm(monthly_paph_peak)
    #rm(cor_weekly_paph_peak)
    #rm(cor_monthly_paph_peak)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [8 of 20]"))

tryCatch({
    #throughput <- f("tp_", month_abbrs)
    
    throughput <- s3_read_parquet(bucket = 'gdot-spm', 'throughput', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(as.integer(CallPhase)),
               Date = date(Date))

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
    rm(weekly_throughput)
    rm(cor_weekly_throughput)
    rm(monthly_throughput)
    rm(cor_monthly_throughput)
    #gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [9 of 20]"))

tryCatch({
    #aog <- f("aog_", month_abbrs, combine = TRUE)
    aog <- s3_read_parquet(bucket = 'gdot-spm', 'arrivals_on_green', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date))
    
    daily_aog <- get_daily_aog(aog)
    
    weekly_aog_by_day <- get_weekly_aog_by_day(aog)
    
    cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
    
    monthly_aog_by_day <- get_monthly_aog_by_day(aog)
    
    cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)
    
    saveRDS(weekly_aog_by_day, "weekly_aog_by_day.rds")
    saveRDS(monthly_aog_by_day, "monthly_aog_by_day.rds")
    saveRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds")
    saveRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds")
    
    rm(daily_aog)
    rm(weekly_aog_by_day)
    rm(monthly_aog_by_day)
    rm(cor_weekly_aog_by_day)
    rm(cor_monthly_aog_by_day)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [10 of 20]"))

tryCatch({
    aog_by_hr <- get_aog_by_hr(aog)
    monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)
    
    cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)
    
    saveRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds")
    saveRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds")
    
    rm(aog)
    rm(aog_by_hr)
    rm(cor_monthly_aog_peak)
    rm(monthly_aog_by_hr)
    rm(cor_monthly_aog_by_hr)
    gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY SPLIT FAILURES #####################################################

tryCatch({
    print(glue("{Sys.time()} Daily Split Failures [11 of 20]"))
    
    #sf <- f("sf_", month_abbrs)
    # sf <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'split_failures', report_start_date, report_end_date, signals_list) %>%
    #   transmute(SignalID = factor(SignalID),
    #             CallPhase = factor(CallPhase),
    #             Date = date(Date),
    #             Date_Hour = Date_Hour,
    #             DOW = wday(Date),
    #             Week = week(Date),
    #             sf = sf,
    #             cycles = cycles,
    #             sf_freq = sf_freq)
    #usable_cores <- get_usable_cores()
    #doParallel::registerDoParallel(cores = usable_cores)
     
    sfs <- lapply(month_abbrs, function(yyyy_mm) {
    #sfs <- foreach(yyyy_mm = month_abbrs) %dopar% {
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(report_end_date))
        
        sf <- s3_read_parquet_parallel('split_failures', as.character(sd), as.character(ed), signals_list, 'gdot-spm') %>% 
            transmute(SignalID = factor(SignalID),
                      CallPhase = factor(CallPhase),
                      Date = date(Date),
                      Date_Hour = Date_Hour,
                      DOW = wday(Date),
                      Week = week(Date),
                      sf = sf,
                      cycles = cycles,
                      sf_freq = sf_freq)
        
        wsf <- get_weekly_sf_by_day(sf) %>% ungroup()
        msf <- get_monthly_sf_by_day(sf) %>% ungroup() %>% filter(!is.na(Month))
        sfh <- get_sf_by_hr(sf)
        
        list(wsf = wsf, msf = msf, sfh = sfh)
    })
    
    wsf <- lapply(sfs, function(x) { x$wsf %>% ungroup() }) %>% 
        bind_rows() %>% 
        arrange(SignalID, Date) %>% 
        mutate(SignalID = factor(SignalID)) %>%
        group_by(SignalID) %>%
        mutate(delta = (sf_freq - lag(sf_freq))/lag(sf_freq)) %>%
        ungroup()
    
    monthly_sfd <- lapply(sfs, function(x) { x$msf %>% ungroup() }) %>% 
        bind_rows() %>% 
        arrange(SignalID, Month) %>% 
        mutate(SignalID = factor(SignalID)) %>%
        group_by(SignalID) %>%
        mutate(delta = (sf_freq - lag(sf_freq))/lag(sf_freq)) %>%
        ungroup()
    
    sfh <- lapply(sfs, function(x) { x$sfh %>% ungroup() }) %>% 
        bind_rows() %>% 
        arrange(SignalID, CallPhase) %>% 
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(delta = (sf_freq - lag(sf_freq))/lag(sf_freq)) %>%
        ungroup()
    
    
    #wsf <- get_weekly_sf_by_day(sf)
    cor_wsf <- get_cor_weekly_sf_by_day(wsf, corridors)
    
    #monthly_sfd <- get_monthly_sf_by_day(sf)
    cor_monthly_sfd <- get_cor_monthly_sf_by_day(monthly_sfd, corridors)
    
    saveRDS(wsf, "wsf.rds")
    saveRDS(monthly_sfd, "monthly_sfd.rds")
    saveRDS(cor_wsf, "cor_wsf.rds")
    saveRDS(cor_monthly_sfd, "cor_monthly_sfd.rds")
    
    
    rm(wsf)
    rm(monthly_sfd)
    rm(cor_wsf)
    rm(cor_monthly_sfd)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [12 of 20]"))

tryCatch({
    #sfh <- get_sf_by_hr(sf) #--this is really slow.
    
    msfh <- get_monthly_sf_by_hr(sfh)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)
    
    saveRDS(msfh, "msfh.rds")
    saveRDS(cor_msfh, "cor_msfh.rds")
    
    #rm(sf)
    rm(sfh)
    rm(msfh)
    rm(cor_msfh)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [13 of 20]"))

tryCatch({
    #qs <- f("qs_", month_abbrs)
    qs <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'queue_spillback', report_start_date, report_end_date, signals_list) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date = date(Date)) #%>%
    #replace_na(list(TimeFromStopBar = 0))
    
    wqs <- get_weekly_qs_by_day(qs)
    cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)
    
    monthly_qsd <- get_monthly_qs_by_day(qs)
    cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)
    
    saveRDS(wqs, "wqs.rds")
    saveRDS(monthly_qsd, "monthly_qsd.rds")
    saveRDS(cor_wqs, "cor_wqs.rds")
    saveRDS(cor_monthly_qsd, "cor_monthly_qsd.rds")
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [14 of 20]"))

tryCatch({
    qsh <- get_qs_by_hr(qs)
    mqsh <- get_monthly_qs_by_hr(qsh)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)
    
    saveRDS(mqsh, "mqsh.rds")
    saveRDS(cor_mqsh, "cor_mqsh.rds")
    
    rm(qs)
    rm(wqs)
    rm(monthly_qsd)
    rm(cor_wqs)
    rm(cor_monthly_qsd)
    rm(qsh)
    rm(mqsh)
    rm(cor_mqsh)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [15 of 20]"))

tryCatch({
    # fns <- list.files(path = "Inrix/For_Monthly_Report",
    #                   pattern = "tt_.*_summary.csv",
    #                   recursive = FALSE,
    #                   full.names = TRUE)
    # 
    # tt <- lapply(fns, read_csv) %>% 
    #     bind_rows() %>% 
    #     mutate(Corridor = factor(Corridor)) %>%
    #     arrange(Corridor, Hour)
    
    tt <- s3_read_parquet_parallel('travel_time_metrics', report_start_date, report_end_date, bucket = 'gdot-spm') %>% 
        mutate(Corridor = factor(Corridor))
    
    # sc <- get_spark_context()
    # table_name <- "travel_time_metrics"
    # keys <- glue("s3a://gdot-spm/mark/{table_name}/date={date_range_str}/*")
    # tt <- spark_read_parquet(sc, name = "tt", path = keys) %>% dplyr::select(-starts_with("__"))
    
    tti <- tt %>% 
        dplyr::select(-pti) %>% collect() %>% mutate(Corridor = factor(Corridor))
    
    
    pti <- tt %>% 
        dplyr::select(-tti) %>% collect() %>% mutate(Corridor = factor(Corridor))
    
    cor_monthly_vph <- readRDS("cor_monthly_vph.rds")
    
    cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, all_corridors)
    cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, all_corridors)
    
    cor_monthly_tti <- get_cor_monthly_ti_by_day(tti, cor_monthly_vph, all_corridors)
    cor_monthly_pti <- get_cor_monthly_ti_by_day(pti, cor_monthly_vph, all_corridors)
    
    write_fst(tti, "tti.fst")
    write_fst(pti, "pti.fst")
    
    saveRDS(cor_monthly_tti, "cor_monthly_tti.rds")
    saveRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds")
    
    saveRDS(cor_monthly_pti, "cor_monthly_pti.rds")
    saveRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds")
    
    rm(tt)
    rm(tti)
    rm(pti)
    rm(cor_monthly_tti)
    rm(cor_monthly_tti_by_hr)
    rm(cor_monthly_pti)
    rm(cor_monthly_pti_by_hr)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DETECTOR UPTIME AS REPORTED BY FIELD ENGINEERS ##############################

print(glue("{Sys.time()} Uptimes [16 of 20]"))

tryCatch({
    # # VEH, PED, CCTV UPTIME - AS REPORTED BY FIELD ENGINEERS via EXCEL
    
    #xl_uptime_fns <- file.path(conf$xl_uptime$path, conf$xl_uptime$filenames)
    xl_uptime_fns <- conf$xl_uptime$filenames
    xl_uptime_mos <- conf$xl_uptime$months
    
    
    
    man_xl <- purrr::map2(xl_uptime_fns,
                          xl_uptime_mos,
                          get_det_uptime_from_manual_xl) %>%
        bind_rows() %>%
        mutate(Zone_Group = factor(Zone_Group)) %>% 
        filter(Month >= report_start_date)
    
    man_veh_xl <- man_xl %>% filter(Type == "Vehicle")
    man_ped_xl <- man_xl %>% filter(Type == "Pedestrian")
    
    
    cor_monthly_xl_veh_uptime <- man_veh_xl %>% #bind_rows(mrs_veh_xl, man_veh_xl) %>%
        dplyr::select(-c(Zone_Group, Type)) %>%
        get_cor_monthly_avg_by_day(all_corridors, "uptime", "num")
    
    
    cor_monthly_xl_ped_uptime <- man_ped_xl %>% #bind_rows(mrs_ped_xl, man_ped_xl) %>%
        dplyr::select(-c(Zone_Group, Type)) %>%
        get_cor_monthly_avg_by_day(all_corridors, "uptime", "num")
    
    cam_config <- aws.s3::get_object(conf$cctv_config_filename, bucket = 'gdot-spm') %>%
        rawToChar() %>%
        read_csv() %>%
        separate(col = CamID, into = c("CameraID", "Location"), sep = ": ")
    
    if (class(cam_config$As_of_Date) != "character") {
        cam_config <- cam_config %>% 
            mutate(As_of_Date = if_else(grepl('\\d{4}-\\d{2}-\\d{2}', As_of_Date),
                                        ymd(As_of_Date),
                                        mdy(As_of_Date)))
    }
        
        #mutate(As_of_Date = mdy(As_of_Date))
        #mutate(As_of_Date = if_else(mdy(As_of_Date))
    
    # cam_config <- read.csv(conf$cctv_config_filename) %>% as_tibble() %>% #"../camera_ids_2018-10-22.csv"
    #     separate(col = CamID, into = c("CameraID", "Location"), sep = ": ") %>%
    #     mutate(As_of_Date = ymd(As_of_Date))
    
    daily_cctv_uptime <- dbGetQuery(conn, sql("select cameraid, date, size from gdot_spm.cctv_uptime")) %>% 
        transmute(CameraID = factor(cameraid),
                  Date = date(date),
                  Size = size) %>%
        as_tibble() %>%
        
        # CCTV image size variance by CameraID and Date
        #  -> reduce to 1 for Size > 0, 0 otherwise
        
        # Expanded out to include all available cameras on all days
        #  up/uptime is 0 if no data
        #daily_cctv_uptime <- read_feather(conf$cctv_parsed_filename) %>% #"parsed_cctv.feather"
        filter(Date > "2018-02-02" & Size > 0) %>%
        mutate(up = 1, num = 1) %>%
        dplyr::select(-Size) %>%
        distinct() %>%
        
        # Expanded out to include all available cameras on all days
        complete(CameraID, Date = full_seq(Date, 1), fill = list(up = 0, num = 1)) %>%
        
        mutate(uptime = up/num) %>%
        
        left_join(dplyr::select(cam_config, -Location)) %>% 
        filter(Date >= As_of_Date & Corridor != "") %>%
        dplyr::select(-As_of_Date) %>%
        mutate(CameraID = factor(CameraID),
               Corridor = factor(Corridor))
    
    
    # Find the days where uptime across the board is very low (close to 0)
    #  This is symptomatic of a problem with the acquisition rather than the camreras themselves
    bad_days <- daily_cctv_uptime %>% 
        group_by(Date) %>% 
        summarize(sup = sum(up),
                  snum = sum(num),
                  suptime = sum(up)/sum(num)) %>% 
        filter(suptime < 0.2)
    
    
    
    cor_daily_cctv_uptime <- get_cor_weekly_avg_by_day(
        daily_cctv_uptime, #filter(daily_cctv_uptime, !(Date >= "2018-11-01" & Date <= "2019-01-15")), 
        all_corridors, "uptime", "num")
    
    weekly_cctv_uptime <- get_weekly_avg_by_day_cctv(daily_cctv_uptime)
    
    cor_weekly_cctv_uptime <- get_cor_weekly_avg_by_day(weekly_cctv_uptime, all_corridors, "uptime", "num")
    
    cor_monthly_cctv_uptime <- get_cor_monthly_avg_by_day(mutate(daily_cctv_uptime, Month = Date - days(day(Date) -1)),
                                                          all_corridors, "uptime", "num")
    
    
    saveRDS(cor_monthly_xl_veh_uptime, "cor_monthly_xl_veh_uptime.rds")
    saveRDS(cor_monthly_xl_ped_uptime, "cor_monthly_xl_ped_uptime.rds")
    
    
    saveRDS(daily_cctv_uptime, "daily_cctv_uptime.rds")
    
    saveRDS(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.rds")
    saveRDS(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.rds")
    saveRDS(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.rds")
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})













# ACTIVITIES ##############################

print(glue("{Sys.time()} TEAMS [17 of 20]"))

tryCatch({
    # Teams Locations
    if (!file.exists("teams_locations.feather")) {
        aws.s3::save_object('mark/teams/teams_locations.feather', bucket = 'gdot-spm')
    }
    teams_locations <- read_feather("teams_locations.feather")
    
    
    # New version from API result
    teams <- aws.s3::get_object('mark/teams/tasks.csv', bucket = 'gdot-spm') %>%
        #rawToChar() %>%
        #read_csv() %>%
        get_teams_tasks() %>%
        
        
        #teams <- get_teams_tasks(tasks_fn = conf$teams_tasks_filename, locations = teams_locations) %>%
        
        filter(`Date Reported` >= ymd(report_start_date) &
                   `Date Reported` <= ymd(report_end_date))
    
    
    #saveRDS(teams, "teams.rds")
    #------------------------------------------------------------------------------
    
    
    type_table <- get_outstanding_events(teams, "Task_Type") %>%
        mutate(Task_Type = if_else(Task_Type == "", "Unknown", Task_Type)) %>%
        group_by(Zone_Group, Task_Type, Month) %>%
        summarize_all(sum) %>%
        ungroup() %>%
        mutate(Task_Type = factor(Task_Type))
    
    subtype_table <- get_outstanding_events(teams, "Task_Subtype") %>%
        mutate(Task_Subtype = if_else(Task_Subtype == "", "Unknown", Task_Subtype)) %>%
        group_by(Zone_Group, Task_Subtype, Month) %>%
        summarize_all(sum) %>%
        ungroup() %>%
        mutate(Task_Subtype = factor(Task_Subtype))
    
    source_table <- get_outstanding_events(teams, "Task_Source") %>%
        mutate(Task_Source = if_else(Task_Source == "", "Unknown", Task_Source)) %>%
        group_by(Zone_Group, Task_Source, Month) %>%
        summarize_all(sum) %>%
        ungroup() %>%
        mutate(Task_Source = factor(Task_Source))
    
    priority_table <- get_outstanding_events(teams, "Priority") %>%
        group_by(Zone_Group, Priority, Month) %>%
        summarize_all(sum) %>%
        ungroup() %>%
        mutate(Priority = factor(Priority))
    
    all_teams_table <- get_outstanding_events(teams, "All") %>%
        group_by(Zone_Group, All, Month) %>%
        summarize_all(sum) %>%
        ungroup() %>%
        mutate(All = factor(All))
    
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
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# # WATCHDOG ###########################################################

print(glue("{Sys.time()} watchdog alerts [18 of 20]"))

tryCatch({
    # -- Alerts: detector downtime --
    
    bad_det <- dbGetQuery(conn, sql("select * from gdot_spm.bad_detectors")) %>% 
        transmute(SignalID = factor(signalid), 
                  Detector = factor(detector), 
                  Date = date(date)) %>%
        filter(Date > today() - months(9)) %>%
        as_tibble() %>%
        left_join(dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name), by = c("SignalID")) %>%
        transmute(Zone_Group, Zone, Corridor, 
                  SignalID = factor(SignalID), CallPhase = factor(0), DetectorID = Detector, 
                  Date, Alert = factor("Bad Vehicle Detection"), Name = factor(Name))
    
    #Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
    
    
    bad_det_filename <- "bad_detectors.fst"
    write_fst(bad_det, bad_det_filename)
    aws.s3::put_object(bad_det_filename, 
                       object = "mark/watchdog/bad_detectors.fst", 
                       bucket = "gdot-spm")
    file.remove(bad_det_filename)
    
    # -- Alerts: pedestrian detector downtime --
    
    bad_ped <- readRDS("pa_uptime.rds") %>%
        filter(uptime == 0) %>%
        left_join(corridors, by = c("SignalID")) %>%
        transmute(Zone_Group, 
                  Zone, 
                  Corridor = factor(Corridor), 
                  SignalID = factor(SignalID), 
                  CallPhase = factor(CallPhase), 
                  DetectorID = factor(CallPhase),
                  Date, 
                  Alert = factor("Bad Ped Detection"), 
                  Name = factor(Name))
    
    bad_det_filename <- "bad_ped_detectors.fst"
    write_fst(bad_ped, bad_det_filename)
    aws.s3::put_object(bad_det_filename, 
                       object = "mark/watchdog/bad_ped_detectors.fst", 
                       bucket = "gdot-spm")
    file.remove(bad_det_filename)
    
    
    # -- Alerts: CCTV downtime --
    
    bad_cam <- tbl(conn, sql("select * from gdot_spm.cctv_uptime")) %>% 
        dplyr::select(-starts_with("__")) %>%
        filter(size == 0) %>% collect() %>% 
        transmute(CameraID = factor(cameraid), 
                  Date = date(date)) %>%
        filter(Date > today() - months(9)) %>%
        left_join(cam_config, by = c("CameraID")) %>%
        filter(Date > As_of_Date) %>%
        left_join(distinct(corridors, Zone_Group, Zone, Corridor), by = c("Corridor")) %>%
        transmute(Zone_Group, Zone, Corridor = factor(Corridor), 
                  SignalID = factor(CameraID), CallPhase = factor(0), DetectorID = factor(0), 
                  Date, Alert = factor("No Camera Image"), Name = factor(Location))
    
    bad_cam_filename <- "bad_cameras.fst"
    write_fst(bad_cam, bad_cam_filename)
    aws.s3::put_object(bad_cam_filename, 
                       object = "mark/watchdog/bad_cameras.fst", 
                       bucket = "gdot-spm")
    file.remove(bad_cam_filename)
    
    # -- Watchdog Alerts --
    
    # Nothing to do here
    
    
    # -- --------------- --
    
    #Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})






# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [19 of 20]"))

sigify <- function(df, cor_df, corridors) {
    
    df_ <- df %>% left_join(distinct(corridors, SignalID, Corridor, Name), by = c("SignalID")) %>%
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

tryCatch({
    cor <- list()
    cor$dy <- list("du" = readRDS("cor_avg_daily_detector_uptime.rds"),
                   "cu" = readRDS("cor_daily_comm_uptime.rds"),
                   "pau" = readRDS("cor_daily_pa_uptime.rds"),
                   "cctv" = readRDS("cor_daily_cctv_uptime.rds"))
    cor$wk <- list("vpd" = readRDS("cor_weekly_vpd.rds"),
                   "vph" = readRDS("cor_weekly_vph.rds"),
                   "vphp" = readRDS("cor_weekly_vph_peak.rds"),
                   "papd" = readRDS("cor_weekly_papd.rds"),
                   "paph" = readRDS( "cor_weekly_paph.rds"),
                   "tp" = readRDS("cor_weekly_throughput.rds"),
                   "aog" = readRDS("cor_weekly_aog_by_day.rds"),
                   "qs" = readRDS("cor_wqs.rds"),
                   "sf" = readRDS("cor_wsf.rds"),
                   "cu" = readRDS("cor_weekly_comm_uptime.rds"),
                   "pau" = readRDS("cor_weekly_pa_uptime.rds"),
                   "cctv" =  readRDS("cor_weekly_cctv_uptime.rds"))
    cor$mo <- list("vpd" = readRDS("cor_monthly_vpd.rds"),
                   "vph" = readRDS("cor_monthly_vph.rds"),
                   "vphp" = readRDS("cor_monthly_vph_peak.rds"),
                   "papd" = readRDS("cor_monthly_papd.rds"),
                   "paph" = readRDS("cor_monthly_paph.rds"),
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
                   "pau" = readRDS("cor_monthly_pa_uptime.rds"),
                   "veh" = readRDS("cor_monthly_xl_veh_uptime.rds"),
                   "ped" = readRDS("cor_monthly_xl_ped_uptime.rds"),
                   "cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
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
                   "pau" = get_quarterly(cor$mo$pau, "uptime"),
                   "veh" = get_quarterly(cor$mo$veh, "uptime", "num"),
                   "ped" = get_quarterly(cor$mo$ped, "uptime", "num"),
                   "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
                   "reported" = get_quarterly(cor$mo$events, "Reported"),
                   "resolved" =  get_quarterly(cor$mo$events, "Resolved"),
                   "outstanding" = get_quarterly(cor$mo$events, "Outstanding", operation = "latest"))
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})







tryCatch({
    sig <- list()
    sig$dy <- list("du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors),
                   "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors),
                   "pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors),
                   "cctv" = readRDS("daily_cctv_uptime.rds"))
    sig$wk <- list("vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors),
                   "vph" = sigify(readRDS("weekly_vph.rds"), cor$wk$vph, corridors),
                   "vphp" = purrr::map2(readRDS("weekly_vph_peak.rds"), cor$wk$vphp,
                                        function(x, y) { sigify(x, y, corridors) }),
                   "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors),
                   "paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
                   "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors),
                   "aog" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aog, corridors),
                   "qs" = sigify(readRDS("wqs.rds"), cor$wk$qs, corridors),
                   "sf" = sigify(readRDS("wsf.rds"), cor$wk$sf, corridors),
                   "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors),
                   "pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors),
                   "cctv" = cor$wk$cctv) #readRDS("weekly_cctv_uptime.rds"))
    sig$mo <- list("vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors),
                   "vph" = sigify(readRDS("monthly_vph.rds"), cor$mo$vph, corridors),
                   "vphp" = purrr::map2(readRDS("monthly_vph_peak.rds"), cor$mo$vphp,
                                        function(x, y) { sigify(x, y, corridors) }),
                   "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors),
                   "paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors),
                   "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors),
                   "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors),
                   "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors),
                   "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors),
                   "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors),
                   "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors),
                   "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors),
                   "tti" = data.frame(),
                   "pti" = data.frame(),
                   "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors),
                   "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors),
                   "pau" = sigify(readRDS("monthly_pa_uptime.rds"), cor$mo$pau, corridors),
                   "cctv" = cor$mo$cctv)
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

#saveRDS(cor, "cor.rds")
#saveRDS(sig, "sig.rds")




saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")

print(glue("{Sys.time()} Upload to AWS [20 of 20]"))

aws.s3::put_object(file = "cor.rds",
                   object = "cor_ec2.rds",
                   bucket = "gdot-spm")
aws.s3::put_object(file = "sig.rds",
                   object = "sig_ec2.rds",
                   bucket = "gdot-spm")
aws.s3::put_object(file = "teams_tables.rds",
                   object = "teams_tables_ec2.rds",
                   bucket = "gdot-spm")

#print(glue("{Sys.time()} Build Signal Dashboards [20 of 20]"))

#db_build_data_for_signal_dashboard_ec2(month_abbrs = month_abbrs[length(month_abbrs)],
#                                       corridors = corridors,
#                                       pth = 'Signal_Dashboards',
#                                       upload_to_s3 = TRUE)
