
# Monthly_Report_Calcs_1.R

source("Monthly_Report_Calcs_init.R")


# # GET CAMERA UPTIMES ########################################################

print(glue("{Sys.time()} parse cctv logs [1 of 11]"))

if (conf$run$cctv == TRUE) {
    # Run python scripts asynchronously
    system("~/miniconda3/bin/python parse_cctvlog.py", wait = FALSE)
    system("~/miniconda3/bin/python parse_cctvlog_encoders.py", wait = FALSE)
}

# # GET RSU UPTIMES ###########################################################

print(glue("{Sys.time()} parse rsu logs [2 of 11]"))

if (conf$run$rsus == TRUE) {
    # Run python script asynchronously
    system("~/miniconda3/bin/python parse_rsus.py", wait = FALSE)
}

# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [3 of 11]"))

if (conf$run$travel_times == TRUE) {
    # Run python script asynchronously
    system("~/miniconda3/bin/python get_travel_times.py travel_times_1hr.yaml", wait = FALSE)
    system("~/miniconda3/bin/python get_travel_times.py travel_times_15min.yaml", wait = FALSE)
    system("~/miniconda3/bin/python get_travel_times_1min.py", wait = FALSE)
}

# # COUNTS ####################################################################

print(glue("{Sys.time()} counts [4 of 11]"))

if (conf$run$counts == TRUE) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    if (length(date_range) == 1) {
        get_counts2(
            date_range[1],
            bucket = conf$bucket,
            conf_athena = conf$athena,
            uptime = TRUE,
            counts = TRUE
        )
    } else {
        foreach(date_ = date_range, .errorhandling = "pass") %dopar% {
            keep_trying(get_counts2, n_tries = 2, 
                date_,
                bucket = conf$bucket,
                conf_athena = conf$athena,
                uptime = TRUE,
                counts = TRUE
            )
        }
    }
}


print("\n---------------------- Finished counts ---------------------------\n")

print(glue("{Sys.time()} monthly cu [5 of 11]"))

signals_list <- as.integer(as.character(corridors$SignalID))
signals_list <- unique(as.character(signals_list[signals_list > 0]))

# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages


# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

print(glue("{Sys.time()} counts-based measures [6 of 11]"))

get_counts_based_measures <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        # yyyy_mm <- month_abbrs # for debugging
        invisible(gc())

        #-----------------------------------------------
        # 1-hour counts, filtered, adjusted, bad detectors

        # start and end days of the month
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(end_date))
        date_range <- seq(sd, ed, by = "1 day")
        
        
        print("1-hour adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_1hr", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_1hr", "adjusted_counts_1hr", conf)
        
        fc_ds <- arrow::open_dataset(sources = "filtered_counts_1hr/")
        ac_ds <- arrow::open_dataset(sources = "adjusted_counts_1hr/")
        
        lapply(date_range, function(date_) {
            # print(date_)
            adjusted_counts_1hr <- ac_ds %>% 
                filter(Date == date_) %>% 
                select(-c(Date, date)) %>% 
                collect()
            s3_upload_parquet_date_split(
                adjusted_counts_1hr,
                bucket = conf$bucket,
                prefix = "adjusted_counts_1hr",
                table_name = "adjusted_counts_1hr",
                conf_athena = conf$athena, parallel = FALSE
            )
            rm(adjusted_counts_1hr)
        })
        
        mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(x) {
            write_signal_details(x, conf$athena, signals_list)
        })


        mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(date_) {
            date_str <- format(date_, "%F")
            if (between(date_, start_date, end_date)) {
                print(glue("filtered_counts_1hr: {date_str}"))
                filtered_counts_1hr <- fc_ds %>%
                    filter(date == date_str) %>%
                    select(-date) %>%
                    collect()

                if (!is.null(filtered_counts_1hr) && nrow(filtered_counts_1hr)) {
                    filtered_counts_1hr <- filtered_counts_1hr %>%
                        mutate(
                            Date = date(Date),
                            SignalID = factor(SignalID),
                            CallPhase = factor(CallPhase),
                            Detector = factor(Detector)
                        )

                    # BAD DETECTORS
                    print(glue("detectors: {date_}"))
                    bad_detectors <- get_bad_detectors(filtered_counts_1hr)
                    s3_upload_parquet_date_split(
                        bad_detectors,
                        bucket = conf$bucket,
                        prefix = "bad_detectors",
                        table_name = "bad_detectors",
                        conf_athena = conf$athena
                    )
                    rm(bad_detectors)

                    # # DAILY DETECTOR UPTIME
                    print(glue("ddu: {date_}"))
                    daily_detector_uptime <- get_daily_detector_uptime(filtered_counts_1hr) %>%
                        bind_rows()
                    s3_upload_parquet_date_split(
                        daily_detector_uptime,
                        bucket = conf$bucket,
                        prefix = "ddu",
                        table_name = "detector_uptime_pd",
                        conf_athena = conf$athena
                    )
                    rm(daily_detector_uptime)
                    rm(filtered_counts_1hr)
                }
            }

            print(glue("reading adjusted_counts_1hr: {date_str}"))
            adjusted_counts_1hr <- ac_ds %>% 
                filter(date == date_str) %>% 
                select(-date) %>% 
                collect()
            
            if (!is.null(adjusted_counts_1hr) && nrow(adjusted_counts_1hr)) {
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
                s3_upload_parquet_date_split(
                    vpd,
                    bucket = conf$bucket,
                    prefix = "vpd",
                    table_name = "vehicles_pd",
                    conf_athena = conf$athena
                )
                rm(vpd)
                # VPH
                print(glue("vph: {date_}"))
                vph <- get_vph(adjusted_counts_1hr, interval = "1 hour")
                s3_upload_parquet_date_split(
                    vph,
                    bucket = conf$bucket,
                    prefix = "vph",
                    table_name = "vehicles_ph",
                    conf_athena = conf$athena
                )
                rm(vph)
                rm(adjusted_counts_1hr)
            }
        })
        if (dir.exists("filtered_counts_1hr")) {
            unlink("filtered_counts_1hr", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_1hr")) {
            unlink("adjusted_counts_1hr", recursive = TRUE)
        }
        rm(fc_ds)
        rm(ac_ds)


        #-----------------------------------------------
        # 15-minute counts and throughput
        # FOR EVERY TUE, WED, THU OVER THE WHOLE MONTH
        print("15-minute counts and throughput")

        print("Prep 15-minute adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_15min", conf, date_range)
        print("Calculate 15-minute adjusted counts")
        get_adjusted_counts_arrow("filtered_counts_15min", "adjusted_counts_15min", conf)

        cat('adjusted counts created. Opening dataset.\n')
        ac_ds <- arrow::open_dataset(sources = "adjusted_counts_15min/")
        cat('dataset read.\n') 

        lapply(date_range, function(date_) {
            print(date_)
            adjusted_counts_15min <- ac_ds %>% 
                filter(Date == date_) %>% 
                select(-c(Date, date)) %>% 
                collect()
            s3_upload_parquet_date_split(
                adjusted_counts_15min,
                bucket = conf$bucket,
                prefix = "adjusted_counts_15min",
                table_name = "adjusted_counts_15min",
                conf_athena = conf$athena, parallel = FALSE
            )
            rm(ac_ds)

            throughput <- get_thruput(adjusted_counts_15min)
            s3_upload_parquet_date_split(
                throughput,
                bucket = conf$bucket,
                prefix = "tp",
                table_name = "throughput",
                conf_athena = conf$athena, parallel = FALSE
            )
            rm(throughput)
            
            # Vehicles per 15-minute timeperiod
            print(glue("vp15: {date_}"))
            vp15 <- get_vph(adjusted_counts_15min, interval = "15 min")
            s3_upload_parquet_date_split(
                vp15,
                bucket = conf$bucket,
                prefix = "vp15",
                table_name = "vehicles_15min",
                conf_athena = conf$athena
            )
            rm(vp15)
            rm(adjusted_counts_15min)
        })
        
        if (dir.exists("filtered_counts_15min")) {
            unlink("filtered_counts_15min", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_15min")) {
            unlink("adjusted_counts_15min", recursive = TRUE)
        }

        
        
        #-----------------------------------------------
        # 1-hour pedestrian activation counts
        print("1-hour pedestrian activation counts")

        counts_ped_1hr <- s3_read_parquet_parallel(
            "counts_ped_1hr",
            as.character(sd),
            as.character(ed),
            bucket = conf$bucket
        )

        if (!is.null(counts_ped_1hr) && nrow(counts_ped_1hr)) {

            # PAPD - pedestrian activations per day
            print("papd")
            papd <- get_vpd(counts_ped_1hr, mainline_only = FALSE) %>%
                ungroup() %>%
                rename(papd = vpd)
            s3_upload_parquet_date_split(
                papd,
                bucket = conf$bucket,
                prefix = "papd",
                table_name = "ped_actuations_pd",
                conf_athena = conf$athena
            )
            rm(papd)

            # PAPH - pedestrian activations per hour
            print("paph")
            paph <- get_vph(counts_ped_1hr, interval = "1 hour", mainline_only = FALSE) %>%
                rename(paph = vph)
            s3_upload_parquet_date_split(
                paph,
                bucket = conf$bucket,
                prefix = "paph",
                table_name = "ped_actuations_ph",
                conf_athena = conf$athena
            )
            rm(paph)
            rm(counts_ped_1hr)
	}

        #-----------------------------------------------
        # 15-min pedestrian activation counts
        print("15-minute pedestrian activation counts")

        counts_ped_15min <- s3_read_parquet_parallel(
            "counts_ped_15min",
            as.character(sd),
            as.character(ed),
            bucket = conf$bucket
        )

        if (!is.null(counts_ped_15min) && nrow(counts_ped_15min)) {
            # PA15 - pedestrian activations per 15min
            print("pa15")
            pa15 <- get_vph(counts_ped_15min, interval = "15 min", mainline_only = FALSE) %>%
                rename(pa15 = vph)
            s3_upload_parquet_date_split(
                pa15,
                bucket = conf$bucket,
                prefix = "pa15",
                table_name = "ped_actuations_15min",
                conf_athena = conf$athena
            )
            rm(pa15)
            rm(counts_ped_15min)
        }
    })
}
if (conf$run$counts_based_measures == TRUE) {
    get_counts_based_measures(month_abbrs)
}


print("--- Finished counts-based measures ---")
