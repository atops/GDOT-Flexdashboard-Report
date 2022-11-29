
# Monthly_Report_Package.R -- For 15-min Data

source("Monthly_Report_Package_init.R")

# For 15min counts (no monthly or weekly), go one extra day base MR_calcs, maximum of 7 days
# to limit the amount of data to process and upload.
calcs_start_date <- today(tzone = "America/New_York") - days(7)
calcs_start_date <- max(calcs_start_date, as_date(get_date_from_string(conf$start_date)) - days(1))
# Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
rds_start_date <- calcs_start_date - days(1)


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29(4)]"))

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ##########################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29(4)]"))

# # WATCHDOG ##################################################################

print(glue("{Sys.time()} watchdog alerts [3 of 29(4)]"))

# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29(4)]"))

# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} 15-Minute Pedestrian Activations [5 of 29(4)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_15min",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            parallel = FALSE
        ) %>%
            filter(!is.na(CallPhase)) %>%   # Added 1/14/20 to perhaps exclude non-programmed ped pushbuttons
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date),
                vol = as.numeric(vol)
            )
        
        bad_ped_detectors <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "bad_ped_detectors",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            parallel = FALSE
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector))
        
        # Filter out bad days
        paph <- paph %>%
            select(SignalID, Timeperiod, CallPhase, Detector, vol) %>%
            anti_join(bad_ped_detectors)
        
        pa_15min <- get_period_sum(paph, "vol", "Timeperiod") %>%
            complete(SignalID, Timeperiod = full_seq(Timeperiod, 900), fill = list(vol = 0))
        cor_15min_pa <- get_cor_monthly_avg_by_period(pa_15min, corridors, "vol", "Timeperiod")
        sub_15min_pa <- get_cor_monthly_avg_by_period(pa_15min, subcorridors, "vol", "Timeperiod")
        
        
        addtoRDS(
            pa_15min, "pa_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pa, "cor_15min_pa.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_pa, "sub_15min_pa.rds", "vol", rds_start_date, calcs_start_date
        )
        
        rm(paph)
        rm(bad_ped_detectors)
        rm(pa_15min)
        rm(cor_15min_pa)
        rm(sub_15min_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# GET PEDESTRIAN DELAY ########################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29(4)]"))

# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29(4)]"))

# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29(4)]"))

# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} 15-Minute Volumes [9 of 29(4)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_15min",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            ) %>%
            rename(vol = vph)
        
        vol_15min <- get_period_sum(vph, "vol", "Timeperiod")
        cor_15min_vol <- get_cor_monthly_avg_by_period(vol_15min, corridors, "vol", "Timeperiod")
        sub_15min_vol <- get_cor_monthly_avg_by_period(vol_15min, subcorridors, "vol", "Timeperiod")
        
        
        addtoRDS(
            vol_15min, "vol_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_vol, "cor_15min_vol.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_vol, "sub_15min_vol.rds", "vol", rds_start_date, calcs_start_date
        )
        
        rm(vph)
        rm(vol_15min)
        rm(cor_15min_vol)
        rm(sub_15min_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29(4)]"))

# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29(4)]"))

# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} 15-Minute AOG [12 of 29(4)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green_15min",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date)
            )
        
        aog <- aog %>% 
            rename(Timeperiod = Date_Period) %>%
            select(SignalID, CallPhase, Timeperiod, aog, pr, vol)
        
        aog_15min <- get_period_avg(aog, "aog", "Timeperiod", "vol") %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(SignalID, Timeperiod = full_seq(Timeperiod, 900))
        cor_15min_aog <- get_cor_monthly_avg_by_period(aog_15min, corridors, "aog", "Timeperiod")
        sub_15min_aog <- get_cor_monthly_avg_by_period(aog_15min, subcorridors, "aog", "Timeperiod")
        
        
        
        addtoRDS(
            aog_15min, "aog_15min.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_aog, "cor_15min_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_aog, "sub_15min_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        
        rm(aog_15min)
        rm(cor_15min_aog)
        rm(sub_15min_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29(4)]"))

# HOURLY PROGESSION RATIO #####################################################

print(glue("{Sys.time()} 15-Minute Progression Ratio [14 of 29(4)]"))

tryCatch(
    {
        pr_15min <- get_period_avg(aog, "pr", "Timeperiod", "vol")
        cor_15min_pr <- get_cor_monthly_avg_by_period(pr_15min, corridors, "pr", "Timeperiod")
        sub_15min_pr <- get_cor_monthly_avg_by_period(pr_15min, subcorridors, "pr", "Timeperiod")
        
        
        
        addtoRDS(
            pr_15min, "pr_15min.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pr, "cor_15min_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_pr, "sub_15min_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        
        rm(aog)
        rm(pr_15min)
        rm(cor_15min_pr)
        rm(sub_15min_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY SPLIT FAILURES ########################################################

print(glue("{Sys.time()} Daily Split Failures [15 of 29(4)]"))

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} 15-Minute Split Failures [16 of 29(4)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures_15min",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            callback = function(x) filter(x, CallPhase == 0)
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )
        
        sf <- sf %>% 
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, sf_freq)

        sf_15min <- get_period_avg(sf, "sf_freq", "Timeperiod") %>%
            # Fill in gaps with 0
            complete(SignalID, Timeperiod = full_seq(Timeperiod, 900), fill = list(sf_freq = 0))
        cor_15min_sf <- get_cor_monthly_avg_by_period(sf_15min, corridors, "sf_freq", "Timeperiod")
        sub_15min_sf <- get_cor_monthly_avg_by_period(sf_15min, subcorridors, "sf_freq", "Timeperiod")
        
        
        
        addtoRDS(
            sf_15min, "sf_15min.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_sf, "cor_15min_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_sf, "sub_15min_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        
        rm(sf)
        rm(sf_15min)
        rm(cor_15min_sf)
        rm(sub_15min_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29(4)]"))

# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} 15-Minute Queue Spillback [18 of 29(4)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback_15min",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        # Fill in gaps with 0
        qs <- qs %>% 
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, qs_freq)
        
        qs_15min <- get_period_avg(qs, "qs_freq", "Timeperiod") %>%
            # Fill in gaps with 0
            complete(SignalID, Timeperiod = full_seq(Timeperiod, 900), fill = list(qs_freq = 0))
        cor_15min_qs <- get_cor_monthly_avg_by_period(qs_15min, corridors, "qs_freq", "Timeperiod")
        sub_15min_qs <- get_cor_monthly_avg_by_period(qs_15min, subcorridors, "qs_freq", "Timeperiod")
        
        
        
        addtoRDS(
            qs_15min, "qs_15min.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_qs, "cor_15min_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_qs, "sub_15min_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        
        rm(qs)
        rm(qs_15min)
        rm(cor_15min_qs)
        rm(sub_15min_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29(4)]"))

# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29(4)]"))

# ACTIVITIES ##################################################################

print(glue("{Sys.time()} TEAMS [21 of 29(4)]"))

# USER DELAY COSTS   ##########################################################

print(glue("{Sys.time()} User Delay Costs [22 of 29(4)]"))

# Flash Events ################################################################

print(glue("{Sys.time()} Flash Events [23 of 29(4)]"))

# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29(4)]"))

# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29(4)]"))

# CRASH INDICES ###############################################################

print(glue("{Sys.time()} Crash Indices [26 of 29(4)]"))





# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [27 of 29(4)]"))

sigify <- function(df, cor_df, corridors, identifier = "SignalID") {
    if (identifier == "SignalID") {
        df_ <- df %>%
            left_join(distinct(corridors, SignalID, Corridor, Name), by = c("SignalID")) %>%
            rename(Zone_Group = Corridor, Corridor = SignalID) %>%
            ungroup() %>%
            mutate(Corridor = factor(Corridor))
    } else if (identifier == "CameraID") {
        corridors <- rename(corridors, Name = Location)
        df_ <- df %>%
            select(
                -matches("Subcorridor"),
                -matches("Zone_Group")
            ) %>%
            left_join(distinct(corridors, CameraID, Corridor, Name), by = c("Corridor", "CameraID")) %>%
            rename(
                Zone_Group = Corridor,
                Corridor = CameraID
            ) %>%
            ungroup() %>%
            mutate(Corridor = factor(Corridor))
    } else {
        stop("bad identifier. Must be SignalID (default) or CameraID")
    }
    
    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Zone_Group)) %>%
        mutate(Zone_Group = Corridor) %>%
        select(-matches("Subcorridor"))
    
    br <- bind_rows(df_, cor_df_) %>%
        mutate(Corridor = factor(Corridor))
    
    if ("Zone_Group" %in% names(br)) {
        br <- br %>%
            mutate(Zone_Group = factor(Zone_Group))
    }
    
    if ("Month" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Hour)
    } else if ("Timeperiod" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Timeperiod)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Date)
    }
}



tryCatch(
    {
        cor2 <- list()
        cor2$qhr <- list(
            "vph" = readRDS("cor_15min_vol.rds"),
            "paph" = readRDS("cor_15min_pa.rds"),
            "aogh" = readRDS("cor_15min_aog.rds"),
            "prh" = readRDS("cor_15min_pr.rds"),
            "sfh" = readRDS("cor_15min_sf.rds"),
            "qsh" = readRDS("cor_15min_qs.rds")
        )
    }, 
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


tryCatch(
    {
        sub2 <- list()
        sub2$qhr <- list(
            "vph" = readRDS("sub_15min_vol.rds"),
            "paph" = readRDS("sub_15min_pa.rds"),
            "aogh" = readRDS("sub_15min_aog.rds"),
            "prh" = readRDS("sub_15min_pr.rds"),
            "sfh" = readRDS("sub_15min_sf.rds"),
            "qsh" = readRDS("sub_15min_qs.rds")
        )
    }, 
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



tryCatch(
    {
        sig2 <- list()
        sig2$qhr <- list(
            "vph" = sigify(readRDS("vol_15min.rds"), cor2$qhr$vph, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta),
            "paph" = sigify(readRDS("pa_15min.rds"), cor2$qhr$paph, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta),
            "aogh" = sigify(readRDS("aog_15min.rds"), cor2$qhr$aogh, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, aog, delta),
            "prh" = sigify(readRDS("pr_15min.rds"), cor2$qhr$prh, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, pr, delta),
            "sfh" = sigify(readRDS("sf_15min.rds"), cor2$qhr$sfh, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, sf_freq, delta),
            "qsh" = sigify(readRDS("qs_15min.rds"), cor2$qhr$qsh, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, qs_freq, delta)
        )
    }, 
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)









if (FALSE) { # Extra column we don't need. Save space in large tables.
# Assign Descriptions for hover text
print(glue("{Sys.time()} Assigning descriptions to tables"))

descs <- corridors %>%
    select(SignalID, Corridor, Description) %>%
    group_by(SignalID, Corridor) %>%
    filter(Description == first(Description)) %>%
    ungroup()

for (tab in names(cor2$qhr)) {
    sig2$qhr[[tab]] <- sig2$qhr[[tab]] %>%
        left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
        mutate(
            Description = coalesce(Description, Corridor),
            Corridor = factor(Corridor),
            Description = factor(Description)
        )
    if (tab %in% names(sub2$qhr)) {
        sub2$qhr[[tab]] <- sub2$qhr[[tab]] %>% mutate(Description = Corridor)
    }
    if (tab %in% names(cor2$qhr)) {
        cor2$qhr[[tab]] <- cor2$qhr[[tab]] %>% mutate(Description = Corridor)
    }
}
}


print(glue("{Sys.time()} Upload to AWS [28 of 29(4)]"))


print(glue("{Sys.time()} Write to Database [29 of 29(4)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
aurora <- keep_trying(func = get_aurora_connection, f = RMySQL::dbConnect, driver = RMySQL::MySQL(), n_tries = 5)
# recreate_database(conn)

append_to_database(
    aurora, cor2, "cor", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sub2, "sub", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sig2, "sig", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)

dbDisconnect(aurora)



