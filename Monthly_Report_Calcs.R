
# Monthly_Report_Calcs.R

library(yaml)

setwd(file.path(dirname(path.expand("~")), "Code", "GDOT", "GDOT-Flexdashboard-Report"))
source("Monthly_Report_Functions.R")
conf <- read_yaml("Monthly_Report_calcs.yaml")

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#
start_date <- ifelse(conf$start_date == "yesterday", 
                     today() - days(1), 
                     conf$start_date)
end_date <- ifelse(conf$end_date == "yesterday",
                   today() - days(1),
                   conf$end_date)
month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#


#sl_fn <- "../SIGNALS_LIST_all_RTOP.txt"
#signals_list <- read.csv(sl_fn, col.names = "SignalID", colClasses = c("character"))$SignalID

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

# corridors <- get_corridors(conf$corridors_xlsx_filename)
# write_feather(corridors, "corridors.feather")

# conn <- dbConnect(odbc::odbc(),
#                  dsn = "sqlodbc",
#                  uid = Sys.getenv("ATSPM_USERNAME"),
#                  pwd = Sys.getenv("ATSPM_PASSWORD"))
# dbWriteTable(conn, "Corridors", corridors, overwrite = TRUE)
# dbSendQuery(conn, "CREATE CLUSTERED INDEX Corridors_Idx0 on Corridors(SignalID)")

# -- ----------------------------------------------------

corridors <- feather::read_feather(conf$corridors_filename) 
signals_list <- corridors$SignalID[!is.na(corridors$SignalID)]

# -- This could be used. Some differences between factors and chars that would 
#    need to be tested.

#corridors <- dbReadTable(conn, "Corridors")
#signals_list <- corridors$SignalID

# -- If we want to run calcs on all signals in ATSPM database

#sig_df <- dbReadTable(conn, "Signals") %>% as_tibble() 
#signals_list <- sig_df$SignalID



#Signals to exclude in June 2018 (and months prior):
#signals_to_exclude <- c("248", "1389", "3391", "3491",
#                        "6329", "6330", "6331", "6347", "6350", "6656", "6657",
#                        "7063", "7287", "7289", "7292", "7293", "7542",
#                        "71000", "78296",
#                        as.character(seq(1600, 1799)),
#                        "7024", "7025")
#signals_list <- setdiff(signals_list, signals_to_exclude)


# # ###########################################################################

# # # GET RAW COUNTS FOR THROUGHPUT, VEHICLES/DAY, VEHICLES/HOUR ################
# get_raw_15min_counts_date_range <- function(start_date, end_date) {
# 
#     start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
#     start_dates <- start_dates[wday(start_dates) %in% c(TUE, WED, THU)]
# 
#     cl <- makeCluster(4)
#     clusterExport(cl, c("get_15min_counts",
#                         "get_counts",
#                         "write_fst_",
#                         "start_date",
#                         "signals_list",
#                         "end_date"))
#     parLapply(cl, start_dates, function(start_date_) {
#         library(DBI)
#         library(dplyr)
#         library(tidyr)
#         library(lubridate)
#         library(fst)
#         library(purrr)
# 
#         end_date_ <- start_date_
# 
#         counts <- get_15min_counts(start_date_, end_date_, signals_list)
#         if (nrow(counts) > 0) {
#             write_fst_(counts, paste0("counts_15min_TWR_", start_date_, ".fst"), append = TRUE)
#         }
# 
#     })
#     stopCluster(cl)
# }
# get_raw_15min_counts_date_range(start_date, end_date)
# 
# 
# # # GET COUNTS FOR DETECTOR UPTIME ############################################
# get_raw_1hr_counts_date_range <- function(start_date, end_date) {
# 
#     start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
#     cl <- makeCluster(4)
#     clusterExport(cl, c("get_1hr_counts",
#                         "get_counts",
#                         "write_fst_",
#                         "start_date",
#                         "end_date",
#                         "signals_list"))
#     counts_groups <- parLapply(cl, start_dates, function(start_date) {
#         library(DBI)
#         library(dplyr)
#         library(tidyr)
#         library(lubridate)
#         library(fst)
#         library(purrr)
# 
#         end_date <- start_date
# 
#         counts <- get_1hr_counts(start_date, end_date, signals_list)
#         write_fst_(counts, paste0("counts_1hr_", start_date, ".fst"), append = TRUE)
# 
#     })
#     stopCluster(cl)
# 
# }
# get_raw_1hr_counts_date_range(start_date, end_date)
# 
# 
# get_comms_date_range <- function(start_date, end_date) {
#     
#     start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
#     cl <- makeCluster(4)
#     clusterExport(cl, c("get_comm_uptime2",
#                         "signals_list"))
#     
#     parLapply(cl, start_dates, function(start_date) {
#         library(DBI)
#         library(dplyr)
#         library(tidyr)
#         library(lubridate)
#         library(fst)
#         
#         library(glue)
#         
#         comm_uptime <- get_comm_uptime2(start_date, signals_list)
#         write_fst(comm_uptime, paste0("cu_", start_date, ".fst"))
#     })
#     stopCluster(cl)
# }
# get_comms_date_range(start_date, end_date)



# This replaces the above. Much simpler. And one pass through the database.
get_counts2_date_range <- function(start_date, end_date) {
    
    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    
    lapply(start_dates, function(x) get_counts2(x, uptime = TRUE, counts = FALSE)) # Temporary. Change to TRUE, TRUE
}
get_counts2_date_range(start_date, end_date)



# --- Everything up to here needs the ATSPM Database ---


# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages

get_daily_throughput <- function(month_abbrs) {
    lapply(month_abbrs, function(x) {
        month_pattern <- paste0("counts_15min_TWR_", x, "-\\d\\d?\\.fst")
        fns <- list.files(pattern = month_pattern)
        
        print(fns)
        print("raw counts")
        raw_counts_15min <- bind_rows(lapply(fns, read_fst))
        
        # Filter and Adjust (interpolate) 15 min Counts
        print("filtered counts")
        filtered_counts_15min <- get_filtered_counts(raw_counts_15min, interval = "15 min")
        rm(raw_counts_15min)
        
        print("adjusted counts")
        adjusted_counts_15min <- get_adjusted_counts(filtered_counts_15min)
        rm(filtered_counts_15min)
        
        # Calculate and write Throughput
        print("throughput")
        throughput <- get_thruput(mutate(adjusted_counts_15min, Date = date(Timeperiod)))
        rm(adjusted_counts_15min)
        
        write_fst(throughput, paste0("tp_", x, ".fst"))
    })
}
get_daily_throughput(month_abbrs)

# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

get_filtered_adjusted_bad_detectors <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        month_pattern <- paste0("counts_1hr_", yyyy_mm, "-\\d\\d?\\.fst")
        fns <- list.files(pattern = month_pattern)
        
        print(fns)
        print("raw counts")
        raw_counts_1hr <- bind_rows(lapply(fns, function(x) {
            read_fst(x) %>% filter(SignalID %in% signals_list)}))
        
        print("filtered counts")
        filtered_counts_1hr <- get_filtered_counts(raw_counts_1hr, interval = "1 hour")
        write_fst_(filtered_counts_1hr, paste0("filtered_counts_1hr_", yyyy_mm, ".fst"))
        rm(raw_counts_1hr)
        
        print("adjusted counts")
        adjusted_counts_1hr <- get_adjusted_counts(filtered_counts_1hr)
        write_fst_(adjusted_counts_1hr, paste0("adjusted_counts_1hr_", yyyy_mm, ".fst"))
        rm(adjusted_counts_1hr)
        
        print("bad detectors")
        bad_detectors <- get_bad_detectors(filtered_counts_1hr)
        rm(filtered_counts_1hr)
        
        write_fst(bad_detectors, paste0("bad_detectors_", yyyy_mm, ".fst"))
    })
}
get_filtered_adjusted_bad_detectors(month_abbrs)

# --- This needs the ATSPM database ---
upload_bad_detectors_to_db <- function(month_abbrs) {
    conn <- dbConnect(odbc::odbc(),
                      dsn = "sqlodbc",
                      uid = Sys.getenv("ATSPM_USERNAME"),
                      pwd = Sys.getenv("ATSPM_PASSWORD"))
    lapply(month_abbrs, function(yyyy_mm) {
        print(yyyy_mm)
        bad_detectors <- read_fst(paste0("bad_detectors_", yyyy_mm, ".fst"))
        # Need to be carefule with this to prevent duplicates
        lapply(strsplit(month_abbrs, "-"), 
               function(x) { 
                   dbSendQuery(conn, paste("delete from BadDetectors where year(Date) =", x[1], "and month(Date) =", x[2]))
               }
        )
        dbWriteTable(conn, "BadDetectors", bad_detectors, append = TRUE)
    })
}
upload_bad_detectors_to_db(month_abbrs)


get_vpd_vph_ddu_cu <- function(month_abbrs) {
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
        #comm_uptime <- get_comm_uptime(filtered_counts_1hr)
        #write_fst(comm_uptime, paste0("cu_", yyyy_mm, ".fst"))
        
    })
}
get_vpd_vph_ddu_cu(month_abbrs)


# -- Run etl_dashboard (Python): cycledata, detectionevents to S3/Athena --
import_from_path("spm_events")
py_run_file("etl_dashboard.py") # python script

# --- ----------------------------- -----------


# # GET ARRIVALS ON GREEN #####################################################
get_aog_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 month") #by = "1 day")
    cl <- makeCluster(4)
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
        library(glue)
        library(purrr)
        library(fst)

        #start_date <- as.character(ymd(start_date) - days(day(ymd(start_date)) - 1))
        #end_date <- as.character(ymd(start_date) + months(1) - days(1))
        
        start_date <- floor_date(start_date, "months")
        end_date <- start_date + months(1) - days(1)

        cycle_data <- get_cycle_data(start_date, end_date, signals_list)
        if (nrow(collect(head(cycle_data))) > 0) {
            aog <- get_aog(cycle_data)
            yyyy_mm <- format(ymd(start_date), "%Y-%m")
            write_fst_(aog, glue("aog_{yyyy_mm}.fst"), append = FALSE)

        }
    })
    stopCluster(cl)
}
get_aog_date_range(start_date, end_date)

# aog_files <- list.files(pattern = "aog_201.*.fst")
# lapply(aog_files, function(x) {
#     read_fst(x) %>%
#         mutate(Week = week(Date),
#                DOW = as.integer(DOW)) %>%
#         write_fst(x)
# })

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {

    start_dates <- seq(ymd(start_date), ymd(end_date), by = "1 month")
    cl <- makeCluster(4)
    clusterExport(cl, c("get_detection_events",
                        "get_spm_data",
                        "get_spm_data_aws",
                        "write_fst_",
                        "get_qs",
                        "signals_list",
                        "end_date"))
    parLapply(cl, start_dates, function(start_date) {
        library(DBI)
        library(RJDBC)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        library(fst)

        #end_date <- start_date
        start_date <- as.character(ymd(start_date) - days(day(ymd(start_date)) - 1))
        
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

py_run_file("split_failures2.py") # python script

# # GET CAMERA UPTIMES ########################################################

py_run_file("parse_cctvlog.py") # Run python script








