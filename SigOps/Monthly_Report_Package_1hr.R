
# Monthly_Report_Package.R -- For Hourly Data

source("Monthly_Report_Package_init.R")

# For hourly counts (no monthly or weekly), only go back 7 days
# to limit the amount of data to process and upload.
calcs_start_date <- today(tzone = "America/New_York") - days(7)
# Need to keep some data in rds to calculate accurate deltas
rds_start_date <- calcs_start_date - days(7)


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 29(3)]"))

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ##########################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 29(3)]"))

# # WATCHDOG ##################################################################

print(glue("{Sys.time()} watchdog alerts [3 of 29(3)]"))

# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 29(3)]"))

# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 29(3)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_1hr",
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
            rename(Hour = Timeperiod, paph = vol) %>%
            select(SignalID, Hour, CallPhase, Detector, paph) %>%
            anti_join(bad_ped_detectors)
        
        hourly_pa <- get_period_sum(paph, "paph", "Hour") %>%
            complete(SignalID, Hour = full_seq(Hour, 3600), fill = list(paph = 0))
        cor_hourly_pa <- get_cor_monthly_avg_by_period(hourly_pa, corridors, "paph", "Hour")
        sub_hourly_pa <- get_cor_monthly_avg_by_period(hourly_pa, subcorridors, "paph", "Hour")
        
        
        addtoRDS(
            hourly_pa, "hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_pa, "cor_hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_pa, "sub_hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )
        
        rm(paph)
        rm(bad_ped_detectors)
        rm(hourly_pa)
        rm(cor_hourly_pa)
        rm(sub_hourly_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# GET PEDESTRIAN DELAY ########################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 29(3)]"))

# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 29(3)]"))

# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 29(3)]"))

# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 29(3)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_ph",
            start_date = calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            )

        hourly_vol <- get_period_sum(vph, "vph", "Hour")
        cor_hourly_vol <- get_cor_monthly_avg_by_period(hourly_vol, corridors, "vph", "Hour")
        sub_hourly_vol <- get_cor_monthly_avg_by_period(hourly_vol, subcorridors, "vph", "Hour")
        
        
        addtoRDS(
            hourly_vol, "hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_vol, "cor_hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_vol, "sub_hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )
        
        rm(vph)
        rm(hourly_vol)
        rm(cor_hourly_vol)
        rm(sub_hourly_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 29(3)]"))

# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 29(3)]"))

# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [12 of 29(3)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green",
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
        
        # Don't fill in gaps (leave as NA)
        # since no volume means no value for aog or pr (it's not 0)
        aog <- aog %>% 
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, aog, pr, vol)
        
        hourly_aog <- get_period_avg(aog, "aog", "Hour", "vol") %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(SignalID, Hour = full_seq(Hour, 3600))
        cor_hourly_aog <- get_cor_monthly_avg_by_period(hourly_aog, corridors, "aog", "Hour")
        sub_hourly_aog <- get_cor_monthly_avg_by_period(hourly_aog, subcorridors, "aog", "Hour")
        
        
        
        addtoRDS(
            hourly_aog, "hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_aog, "cor_hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_aog, "sub_hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        
        rm(hourly_aog)
        rm(cor_hourly_aog)
        rm(sub_hourly_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 29(3)]"))

# HOURLY PROGESSION RATIO #####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 29(3)]"))

tryCatch(
    {
        hourly_pr <- get_period_avg(aog, "pr", "Hour", "vol")
        cor_hourly_pr <- get_cor_monthly_avg_by_period(hourly_pr, corridors, "pr", "Hour")
        sub_hourly_pr <- get_cor_monthly_avg_by_period(hourly_pr, subcorridors, "pr", "Hour")
        
        
        
        addtoRDS(
            hourly_pr, "hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_pr, "cor_hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_pr, "sub_hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        
        rm(aog)
        rm(hourly_pr)
        rm(cor_hourly_pr)
        rm(sub_hourly_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY SPLIT FAILURES ########################################################

print(glue("{Sys.time()} Daily Split Failures [15 of 29(3)]"))

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 29(3)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures",
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
        
        # Fill in gaps with 0
        sf <- sf %>% 
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, sf_freq)

        hourly_sf <- get_period_avg(sf, "sf_freq", "Hour") %>%
            # Fill in gaps with 0
            complete(SignalID, Hour = full_seq(Hour, 3600), fill = list(sf_freq = 0))
        cor_hourly_sf <- get_cor_monthly_avg_by_period(hourly_sf, corridors, "sf_freq", "Hour")
        sub_hourly_sf <- get_cor_monthly_avg_by_period(hourly_sf, subcorridors, "sf_freq", "Hour")
        
        
        
        addtoRDS(
            hourly_sf, "hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_sf, "cor_hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_sf, "sub_hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        
        rm(sf)
        rm(hourly_sf)
        rm(cor_hourly_sf)
        rm(sub_hourly_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 29(3)]"))

# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 29(3)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback",
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
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, qs_freq)
        
        hourly_qs <- get_period_avg(qs, "qs_freq", "Hour") %>%
            # Fill in gaps with 0
            complete(SignalID, Hour = full_seq(Hour, 3600), fill = list(qs_freq = 0))
        cor_hourly_qs <- get_cor_monthly_avg_by_period(hourly_qs, corridors, "qs_freq", "Hour")
        sub_hourly_qs <- get_cor_monthly_avg_by_period(hourly_qs, subcorridors, "qs_freq", "Hour")
        
        
        
        addtoRDS(
            hourly_qs, "hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_qs, "cor_hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_qs, "sub_hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        
        rm(qs)
        rm(hourly_qs)
        rm(cor_hourly_qs)
        rm(sub_hourly_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 29(3)]"))

# CCTV UPTIME From 511 and Encoders

print(glue("{Sys.time()} CCTV Uptimes [20 of 29(3)]"))

# ACTIVITIES ##################################################################

print(glue("{Sys.time()} TEAMS [21 of 29(3)]"))

# USER DELAY COSTS   ##########################################################

print(glue("{Sys.time()} User Delay Costs [22 of 29(3)]"))

# Flash Events ################################################################

print(glue("{Sys.time()} Flash Events [23 of 29(3)]"))

# BIKE/PED SAFETY INDEX #######################################################

print(glue("{Sys.time()} Bike/Ped Safety Index [24 of 29(3)]"))

# RELATIVE SPEED INDEX ########################################################

print(glue("{Sys.time()} Relative Speed Index [25 of 29(3)]"))

# CRASH INDICES ###############################################################

print(glue("{Sys.time()} Crash Indices [26 of 29(3)]"))





# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [27 of 29(3)]"))

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



# All of the hourlies and 15-min bins. Ugh.

tryCatch(
    {
        cor2 <- list()
        cor2$hr <- list(
            "vph" = readRDS("cor_hourly_vol.rds"),
            "paph" = readRDS("cor_hourly_pa.rds"),
            "aogh" = readRDS("cor_hourly_aog.rds"),
            "prh" = readRDS("cor_hourly_pr.rds"),
            "sfh" = readRDS("cor_hourly_sf.rds"),
            "qsh" = readRDS("cor_hourly_qs.rds")
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
        sub2$hr <- list(
            "vph" = readRDS("sub_hourly_vol.rds"),
            "paph" = readRDS("sub_hourly_pa.rds"),
            "aogh" = readRDS("sub_hourly_aog.rds"),
            "prh" = readRDS("sub_hourly_pr.rds"),
            "sfh" = readRDS("sub_hourly_sf.rds"),
            "qsh" = readRDS("sub_hourly_qs.rds")
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
        sig2$hr <- list(
            "vph" = sigify(readRDS("hourly_vol.rds"), cor2$hr$vph, corridors) %>%
                select(Zone_Group, Corridor, Hour, vph, delta),
            "paph" = sigify(readRDS("hourly_pa.rds"), cor2$hr$paph, corridors) %>%
                select(Zone_Group, Corridor, Hour, paph, delta),
            "aogh" = sigify(readRDS("hourly_aog.rds"), cor2$hr$aogh, corridors) %>%
                select(Zone_Group, Corridor, Hour, aog, delta),
            "prh" = sigify(readRDS("hourly_pr.rds"), cor2$hr$prh, corridors) %>%
                select(Zone_Group, Corridor, Hour, pr, delta),
            "sfh" = sigify(readRDS("hourly_sf.rds"), cor2$hr$sfh, corridors) %>%
                select(Zone_Group, Corridor, Hour, sf_freq, delta),
            "qsh" = sigify(readRDS("hourly_qs.rds"), cor2$hr$qsh, corridors) %>%
                select(Zone_Group, Corridor, Hour, qs_freq, delta)
        )
    }, 
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)










# Assign Descriptions for hover text
print(glue("{Sys.time()} Assigning descriptions to tables"))

descs <- corridors %>%
    select(SignalID, Corridor, Description) %>%
    group_by(SignalID, Corridor) %>%
    filter(Description == first(Description)) %>%
    ungroup()

for (tab in names(cor2$hr)) {
    sig2$hr[[tab]] <- sig2$hr[[tab]] %>%
        left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
        mutate(
            Description = coalesce(Description, Corridor),
            Corridor = factor(Corridor),
            Description = factor(Description)
        )
    if (tab %in% names(sub2$hr)) {
        sub2$hr[[tab]] <- sub2$hr[[tab]] %>% mutate(Description = Corridor)
    }
    if (tab %in% names(cor2$hr)) {
        cor2$hr[[tab]] <- cor2$hr[[tab]] %>% mutate(Description = Corridor)
    }
}



print(glue("{Sys.time()} Upload to AWS [28 of 29(3)]"))


print(glue("{Sys.time()} Write to Database [29 of 29(3)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
aurora <- get_aurora_connection()

# Uncomment this block to recreate (refresh) database. Rare event. Development only.
# recreate_database(aurora, cor2, "cor")
# recreate_database(aurora, sub2, "sub")
# recreate_database(aurora, sig2, "sig")

# append_to_database(aurora, cor2, "cor", calcs_start_date, report_start_date, report_end_date)
# append_to_database(aurora, sub2, "sub", calcs_start_date, report_start_date, report_end_date)
# append_to_database(aurora, sig2, "sig", calcs_start_date, report_start_date, report_end_date)


append_to_database(
    aurora, cor2, "cor", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sub2, "sub", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sig2, "sig", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)

dbDisconnect(aurora)



