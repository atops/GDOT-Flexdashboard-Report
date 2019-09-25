
# Monthly_Report_Calcs.R

library(yaml)
library(glue)

print(glue("{Sys.time()} Starting Calcs Script"))

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

usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#
start_date <- ifelse(conf$start_date == "yesterday",
    format(today() - days(1), "%Y-%m-%d"),
    conf$start_date
)
end_date <- ifelse(conf$end_date == "yesterday",
    format(today() - days(1), "%Y-%m-%d"),
    conf$end_date
)

# Manual overrides
# start_date <- "2019-06-28"
# end_date <- "2019-06-30"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

install_corridors_file <- function() {
    corridors <- s3read_using(
        get_corridors,
        object = "Corridors_Latest.xlsx",
        bucket = "gdot-spm"
    )
    write_feather(corridors, conf$corridors_filename)
    aws.s3::put_object(conf$corridors_filename,
        object = conf$corridors_filename,
        bucket = "gdot-spm"
    )

    all_corridors <- s3read_using(
        function(x) get_corridors(x, filter_signals = FALSE),
        object = "Corridors_Latest.xlsx",
        bucket = "gdot-spm"
    )
    write_feather(all_corridors, glue("all_{conf$corridors_filename}"))
    aws.s3::put_object(glue("all_{conf$corridors_filename}"),
        object = glue("all_{conf$corridors_filename}"),
        bucket = "gdot-spm"
    )
}

# install_corridors_file()


# -- ----------------------------------------------------

corridors <- read_feather(conf$corridors_filename)
signals_list <- unique(corridors$SignalID)
all_corridors <- read_feather(glue("all_{conf$corridors_filename}"))

subcorridors <- corridors %>%
    mutate(
        Subcorridor = as.character(Subcorridor),
        Subcorridor = if_else(!is.na(Subcorridor), Subcorridor, as.character(Corridor))
    ) %>%
    mutate(Corridor = factor(Subcorridor)) %>%
    select(-Subcorridor)

# -- TMC Codes for Corridors
# tmc_routes <- get_tmc_routes()
# write_feather(tmc_routes, "tmc_routes.feather")
# aws.s3::put_object("tmc_routes.feather", object = "tmc_routes.feather", bucket = "gdot-spm")


# -- Teams Locations
# Warning: Need to clean up ,="", and convert to utf-8 on Notepad++
# to TEAMS Location Report after export
#
# teams_locations_report_raw_output <- "TEAMS_Reports/TEAMS_Locations_Report_2019-08-12.csv"
# teams_locations <- get_teams_locations(teams_locations_report_raw_output)
# st_geometry(teams_locations) <- NULL
# write_feather(teams_locations, "teams_locations.feather")
# put_object(file = "teams_locations.feather",
#            object = "teams_locations.feather",
#            bucket = "gdot-spm")




print(Sys.time())

# # GET CAMERA UPTIMES ########################################################

print(glue("{Sys.time()} parse cctv logs [1 of 10]"))

if (conf$run$cctv == TRUE) {
    system("python parse_cctvlog.py", wait = FALSE) # Run python script asynchronously
    system("python parse_cctvlog_encoders.py", wait = FALSE) # Run python script asynchronously
}

# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [2 of 10]"))

if (conf$run$travel_times == TRUE) {
    system("python get_travel_times.py", wait = FALSE) # Run python script asynchronously
}

# # COUNTS ####################################################################

print(glue("{Sys.time()} counts [3 of 10]"))

if (conf$run$counts == TRUE) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    if (length(date_range) == 1) {
        lapply(date_range, function(date_) {
            get_counts2(date_, uptime = TRUE, counts = TRUE)
        })
    } else {
        foreach(date_ = date_range) %dopar% {
            get_counts2(date_, uptime = TRUE, counts = TRUE)
        }
        #registerDoSEQ()
        #gc()
    }
}
print("\n---------------------- Finished counts ---------------------------\n")

print(glue("{Sys.time()} monthly cu [4 of 10]"))


# --- Everything up to here needs the ATSPM Database ---

signals_list <- as.integer(as.character(corridors$SignalID))
signals_list <- unique(as.character(signals_list[signals_list > 0]))

# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages


# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

print(glue("{Sys.time()} counts-based measures [5 of 10]"))

get_counts_based_measures <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        gc()

        #-----------------------------------------------
        # 1-hour counts, filtered, adjusted, bad detectors

        # start and end days of the month
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(end_date))
        date_range <- seq(sd, ed, by = "1 day")


        print("adjusted counts")
        s3_read_parquet_parallel("filtered_counts_1hr",
            as.character(sd),
            as.character(ed),
            bucket = "gdot-spm"
        ) %>%
            mutate(
                Date = date(Date),
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector)
            ) %>%
            get_adjusted_counts() %>%
            s3_upload_parquet_date_split(
                prefix = "adjusted_counts_1hr",
                table_name = "adjusted_counts_1hr"
            )
        gc()

        # foreach(date_ = date_range) %dopar% {
        lapply(date_range, function(date_) {
            if (between(date_, start_date, end_date)) {
                print(glue("filtered_counts_1hr: {date_}"))
                filtered_counts_1hr <- s3_read_parquet_parallel(
                    "filtered_counts_1hr",
                    as.character(date_),
                    as.character(date_),
                    bucket = "gdot-spm"
                ) %>%
                    mutate(
                        Date = date(Date),
                        SignalID = factor(SignalID),
                        CallPhase = factor(CallPhase),
                        Detector = factor(Detector)
                    )

                # BAD DETECTORS
                print(glue("detectors: {date_}"))
                bad_detectors <- get_bad_detectors(filtered_counts_1hr)
                s3_upload_parquet_date_split(bad_detectors, prefix = "bad_detectors", table_name = "bad_detectors")

                # DAILY DETECTOR UPTIME
                print(glue("ddu: {date_}"))
                daily_detector_uptime <- get_daily_detector_uptime(filtered_counts_1hr) %>% bind_rows()
                s3_upload_parquet_date_split(daily_detector_uptime, prefix = "ddu", table_name = "detector_uptime_pd")

                rm(filtered_counts_1hr)
                gc()
            }

            print(glue("reading adjusted_counts_1hr: {date_}"))
            adjusted_counts_1hr <- s3_read_parquet_parallel(
                "adjusted_counts_1hr",
                as.character(date_),
                as.character(date_),
                bucket = "gdot-spm"
            )

            if (nrow(adjusted_counts_1hr) > 0) {
                adjusted_counts_1hr <- adjusted_counts_1hr %>%
                    mutate(
                        Date = date(Date),
                        SignalID = factor(SignalID),
                        CallPhase = factor(CallPhase),
                        Detector = factor(Detector)
                    )

                # VPD
                print(glue("vpd: {date_}"))
                vpd <- get_vpd(adjusted_counts_1hr) # calculate over current period
                s3_upload_parquet_date_split(vpd, prefix = "vpd", table_name = "vehicles_pd")

                # VPH
                print(glue("vph: {date_}"))
                vph <- get_vph(adjusted_counts_1hr)
                s3_upload_parquet_date_split(vph, prefix = "vph", table_name = "vehicles_ph")
            }
        })
        registerDoSEQ()
        gc()

        #-----------------------------------------------
        # 15-minute counts and throughput
        print("15-minute counts and throughput")

        doParallel::registerDoParallel(cores = usable_cores)

        date_range_twr <- date_range[lubridate::wday(date_range, label = TRUE) %in% c("Tue", "Wed", "Thu")]

        filtered_counts_15min <- foreach(date_ = date_range_twr) %dopar% {
            if (between(date_, start_date, end_date)) {
                date_ <- as.character(date_)
                print(date_)
                s3_read_parquet("filtered_counts_15min", date_, date_, bucket = "gdot-spm") %>%
                    transmute(
                        SignalID = factor(SignalID),
                        CallPhase = factor(CallPhase),
                        Detector = factor(Detector),
                        # CountPriority = CountPriority,
                        Date = date(Date),
                        Timeperiod = Timeperiod,
                        Month_Hour = Month_Hour,
                        Hour = Hour,
                        vol = vol,
                        Good = Good,
                        Good_Day = Good_Day,
                        delta_vol = delta_vol,
                        mean_abs_delta = mean_abs_delta
                    )
            }
        } %>% bind_rows()

        if (length(filtered_counts_15min) > 0) {
            print("adjusted counts")
            adjusted_counts_15min <- get_adjusted_counts(filtered_counts_15min) %>%
                mutate(Date = date(Timeperiod))
            rm(filtered_counts_15min)

            # Calculate and write Throughput
            throughput <- get_thruput(adjusted_counts_15min)
            # throughput <- get_thruput(filtered_counts_15min)

            s3_upload_parquet_date_split(throughput, prefix = "tp", table_name = "throughput")
        }

        registerDoSEQ()
        gc()



        #-----------------------------------------------
        # 1-hour pedestrian activation counts
        print("1-hour pedestrian activation counts")

        conn <- get_athena_connection()


        counts_ped_1hr <- s3_read_parquet_parallel(
            "counts_ped_1hr",
            as.character(start_date),
            as.character(end_date),
            bucket = "gdot-spm"
        )

        # PAPD - pedestrian activations per day
        print("papd")
        papd <- get_vpd(counts_ped_1hr, mainline_only = FALSE) %>%
            ungroup() %>%
            rename(papd = vpd)
        # s3_upload_parquet(papd, sd, glue("papd_{yyyy_mm}"), "ped_actuations_pd")
        # write_fst(papd, paste0("papd_", yyyy_mm, ".fst"))
        s3_upload_parquet_date_split(papd, prefix = "papd", table_name = "ped_actuations_pd")


        # PAPH - pedestrian activations per hour
        print("paph")
        paph <- get_vph(counts_ped_1hr, mainline_only = FALSE) %>%
            rename(paph = vph)
        # s3_upload_parquet(paph, sd, glue("paph_{yyyy_mm}"), "ped_actuations_ph")
        # write_fst(paph, paste0("paph_", yyyy_mm, ".fst"))
        s3_upload_parquet_date_split(paph, prefix = "paph", table_name = "ped_actuations_ph")
    })
}
if (conf$run$counts_based_measures == TRUE) {
    get_counts_based_measures(month_abbrs)
}

# print(glue("{Sys.time()} bad detectors [4 of 10]"))

print("--- Finished counts-based measures ---")



# -- Run etl_dashboard (Python): cycledata, detectionevents to S3/Athena --
print(glue("{Sys.time()} etl [7 of 10]"))

if (conf$run$etl == TRUE) {
    library(reticulate)

    python_path <- file.path("~", "miniconda3", "bin", "python")
    use_python(python_path)

    etl <- reticulate::import_from_path("etl_dashboard", path = "~/Code/GDOT/GDOT-Flexdashboard-Report/")

    etl$main(start_date, end_date)

    # run python script and wait for completion
    # system2("./etl_dashboard.sh", args = c(start_date, end_date))


    # date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    # foreach(date_ = date_range) %dopar% {
    #     # run python script and wait for completion
    #     system(glue("python etl_dashboard.py {date_} {date_}"), wait = TRUE)
    # }
}

# --- ----------------------------- -----------

# # GET ARRIVALS ON GREEN #####################################################
get_aog_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        # foreach(date_ = date_range) %dopar% {
        print(date_)

        cycle_data <- get_cycle_data(date_, date_, signals_list)
        if (nrow(collect(head(cycle_data))) > 0) {
            aog <- get_aog(cycle_data)
            s3_upload_parquet_date_split(aog, prefix = "aog", table_name = "arrivals_on_green")
        }
    })
}
print(glue("{Sys.time()} aog [8 of 10]"))

if (conf$run$arrivals_on_green == TRUE) {
    get_aog_date_range(start_date, end_date)
}
gc()

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        # foreach(date_ = date_range) %dopar% {
        print(date_)

        detection_events <- get_detection_events(date_, date_, signals_list)
        if (nrow(collect(head(detection_events))) > 0) {
            qs <- get_qs(detection_events)
            s3_upload_parquet_date_split(qs, prefix = "qs", table_name = "queue_spillback")
        }
    })
}
print(glue("{Sys.time()} queue spillback [9 of 10]"))

if (conf$run$queue_spillback == TRUE) {
    get_queue_spillback_date_range(start_date, end_date)
}



# # GET SPLIT FAILURES ########################################################

print(glue("{Sys.time()} split failures [10 of 10]"))

get_sf_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    #lapply(date_range, function(date_) {
    foreach(date_ = date_range) %dopar% {
        print(date_)
        cycle_data <- get_cycle_data(date_, date_, signals_list)
        detection_events <- get_detection_events(date_, date_, signals_list)
        if (nrow(collect(head(cycle_data))) > 0 & nrow(collect(head(cycle_data))) > 0) {
            sf <- get_sf_utah(cycle_data, detection_events)
            s3_upload_parquet_date_split(sf, prefix = "sf", table_name = "split_failures")
        }
    }#)
    registerDoSEQ()
    gc()
}

if (conf$run$split_failures == TRUE) {
    get_sf_date_range(start_date, end_date) # Utah method, based on green, start-of-red occupancies
}



# # GET TEAMS TASKS ###########################################################

# Download all TEAMS tasks via API. Shell command. Windows only.
# if (Sys.info()["sysname"] == "Windows") {
#     system('"DocumentClient.exe" TEAMS_Reports/tasks.csv')
# }


print("\n--------------------- End Monthly Report calcs -----------------------\n")
