
get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {
    
    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {
        
        df <- df %>%
            filter(eventcode == event_code)
        
        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>% 
                group_by(timeperiod = date_trunc('hour', timestamp),
                         signalid,
                         eventparam)
            
            # Group by 15 minute interval using Athena/Presto SQL
        } else if (units == "15min") {
            df <- df %>% 
                mutate(timeperiod = date_trunc('minute', timestamp)) %>%
                group_by(timeperiod = date_add('second',
                                               as.integer(-1 * mod(to_unixtime(timeperiod), 15*60)),
                                               timeperiod),
                         signalid,
                         eventparam)
        }
        
        df <- df %>%
            count() %>%
            ungroup() %>%
            collect() %>%
            transmute(Timeperiod = ymd_hms(timeperiod),
                      SignalID = factor(signalid),
                      Detector = factor(eventparam),
                      vol = as.integer(n)) %>%
            left_join(det_config, by = c("SignalID", "Detector")) %>%
            
            dplyr::select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
        df
    } else {
        data.frame()
    }
}


get_counts2 <- function(date_, bucket, conf_athena, uptime = TRUE, counts = TRUE) {
    
    atspm_query <- sql(glue(paste(
        "select distinct timestamp, signalid, eventcode, eventparam", 
        "from {conf_athena$database}.{conf_athena$atspm_table}", 
        "where date = '{start_date}'")))
    
    #conn <- get_athena_connection(conf_athena)
    
    start_date <- date_
    end_time <- format(date(date_) + days(1) - seconds(0.1), "%Y-%m-%d %H:%M:%S.9")
    
    if (counts == TRUE) {
        det_config <- get_det_config(start_date) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase))
        
        ped_config <- get_ped_config(start_date) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase))
    }
    

    print(paste("-- Get Counts for:", start_date, "-----------"))
    
    if (uptime == TRUE) {
        
        
        # Uptime is updated as of July 2020
        # Takes comm codes from MaxView EventLog table (500,501,502,503,504)
        # These codes show comm attempts, failures and percent failures every 5 minutes. 
        # If failure rate is less than 100% we say we have comm at that time period
        # We average up the periods with comms to get the uptime for the day. 
        # Typically (# periods with some comm)/(total number of periods) = % uptime
        #
        # When we start pulling from the ATSPM database for events, we'll have
        # to pull these codes separately since they only appear in MaxView's EventLog
        #
        conn <- get_athena_connection(conf_athena)
        
        print(glue("Communications uptime {date_}"))
        comm_uptime <- tbl(conn, atspm_query) %>% 
            filter(eventcode %in% c(502,503)) %>%
            collect() %>% 
            group_by(signalid) %>% 
            summarize(
                success_rate = sum(eventparam[eventcode==502]<100, na.rm=TRUE),
                denom = sum(eventparam[eventcode==502]>-1, na.rm=TRUE),
                response_ms = mean(eventparam[eventcode==503], na.rm=TRUE), 
                .groups = "drop") %>%
            mutate(SignalID = factor(signalid),
                   CallPhase = factor(0),
                   uptime = success_rate/denom,
                   Date_Hour = ymd_hms(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date), 
                   Week = week(start_date)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime, response_ms)
            
        dbDisconnect(conn)
        
        s3_upload_parquet(comm_uptime, date_, 
                          fn = glue("cu_{date_}"), 
                          bucket = bucket,
                          table_name = "comm_uptime",
                          conf_athena = conf_athena)
    }
    
    if (counts == TRUE) {

        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_TWR_{date_}")
        
        filtered_counts_1hr_fn <- glue("filtered_counts_1hr_{date_}")
        filtered_counts_15min_fn <- glue("filtered_counts_15min_{date_}")
        
        conn <- get_athena_connection(conf_athena)
        
        # get 1hr counts
        print("1-hour counts")
        counts_1hr <- get_counts(
            tbl(conn, atspm_query), 
            det_config, 
            "hours", 
            date_, 
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)

        dbDisconnect(conn)
        
        s3_upload_parquet(counts_1hr, date_, 
                          fn = counts_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_1hr", 
                          conf_athena = conf_athena)

        conn <- get_athena_connection(conf_athena)
        
        print("1-hr filtered counts")
        if (nrow(counts_1hr) > 0) {
            filtered_counts_1hr <- get_filtered_counts_3stream(
                counts_1hr, 
                interval = "1 hour")
            
            filtered_counts_1hr <- filtered_counts_hack(filtered_counts_1hr)
            
            s3_upload_parquet(filtered_counts_1hr, date_, 
                              fn = filtered_counts_1hr_fn, 
                              bucket = bucket,
                              table_name = "filtered_counts_1hr", 
                              conf_athena = conf_athena)
            rm(counts_1hr)
            
        }
        
        
        
        conn <- get_athena_connection(conf_athena)
        
        # get 1hr ped counts
        print("1-hour pedestrian counts")
        counts_ped_1hr <- get_counts(
            tbl(conn, atspm_query), 
            ped_config, 
            "hours", 
            date_, 
            event_code = 90, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        dbDisconnect(conn)

        s3_upload_parquet(counts_ped_1hr, date_, 
                          fn = counts_ped_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_ped_1hr", 
                          conf_athena = conf_athena)
        rm(counts_ped_1hr)
        
        
        conn <- get_athena_connection(conf_athena)
        
        # get 15min counts
        print("15-minute counts")
        counts_15min <- get_counts(
            tbl(conn, atspm_query), 
            det_config, 
            "15min", 
            date_,
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        dbDisconnect(conn)
        
        s3_upload_parquet(counts_15min, date_, 
                          fn = counts_15min_fn, 
                          bucket = bucket,
                          table_name = "counts_15min", 
                          conf_athena = conf_athena)
        
        # get 15min filtered counts
        print("15-minute filtered counts")
        if (nrow(counts_15min) > 0) {
            get_filtered_counts_3stream(
                counts_15min, 
                interval = "15 min") %>%
                s3_upload_parquet(date_,   # filtered_counts_15min, 
                                  fn = filtered_counts_15min_fn, 
                                  bucket = bucket,
                                  table_name = "filtered_counts_15min", 
                                  conf_athena = conf_athena)
        }
    }
    #dbDisconnect(conn)
}



# New version of get_filtered_counts from 4/2/2020.
#  Considers three factors separately and fails a detector if any are flagged:
#  Streak of 5 "flatlined" hours,
#  Five hours exceeding max volume,
#  Mean Absolute Deviation greater than a threshold
get_filtered_counts_3stream <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    if (interval == "1 hour") {
        max_volume <- 1200  # 1000 - increased on 3/19/2020 (down to 2000 on 3/31) to accommodate mainline ramp meters
        max_volume_mainline <- 3000 # New on 5/21/2020
        max_delta <- 500
        max_abs_delta <- 200  # 200 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 500 # New on 5/21/2020
        max_flat <- 5
        hi_vol_pers <- 5
    } else if (interval == "15 min") {
        max_volume <- 300  #250 - increased on 3/19/2020 (down to 500 on 3/31) to accommodate mainline ramp meter detectors
        max_volume_mainline <- 750 # New on 5/21/2020
        max_delta <- 125
        max_abs_delta <- 50  # 50 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 125
        max_flat <- 20
        hi_vol_pers <- 20
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))
    
    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase, ApproachDesc), ## ApproachDesc is new
                   Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline = grepl("Mainline", ApproachDesc)) ## New
    
    expanded_counts <- full_join(
        det_config, 
        counts, 
        by = c("SignalID", "Timeperiod", "Detector", "CallPhase")
    ) %>%
        transmute(SignalID = factor(SignalID),
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline,
                  vol = as.double(vol)) %>%
        replace_na(list(vol = 0)) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%
        
        group_by(SignalID, Date, CallPhase, Detector) %>%
        mutate(delta_vol = vol - lag(vol),
               mean_abs_delta = as.integer(ceiling(mean(abs(delta_vol), na.rm = TRUE))), 
               vol_streak = if_else(hour(Timeperiod) < 5, -1, vol),
               flatlined = streak_run(vol_streak),
               flatlined = if_else(hour(Timeperiod) < 5, 0, as.double(flatlined)),
               flat_flag = max(flatlined, na.rm = TRUE) > max_flat,
               maxvol_flag = if_else(Mainline, 
                                     sum(vol > max_volume_mainline) > hi_vol_pers, 
                                     sum(vol > max_volume) > hi_vol_pers),
               mad_flag = if_else(Mainline,
                                  mean_abs_delta > max_abs_delta_mainline,
                                  mean_abs_delta > max_abs_delta)) %>%
        
        ungroup() %>%
        select(-Mainline, -vol_streak) 
    
    # bad day = any of the following:
    #    flatlined for at least 5 hours (starting at 5am hour)
    #    vol exceeds maximum allowed over 5 different hours
    #    mean absolute delta exceeds 500
    #  - or - 
    #    flatlined for at least 20 15-min periods (starting at 5am)
    #    vol exceeds maximum over 20 different 15-min periods
    #    mean absolute delta exeeds 125
    
    expanded_counts %>%
        group_by(
            SignalID, Date, Detector, CallPhase) %>% 
        mutate(
            flat_strk = as.integer(max(flatlined)), 
            max_vol = as.integer(max(vol, na.rm = TRUE)), 
            flat_flag = max(flat_flag), 
            maxvol_flag = max(maxvol_flag), 
            mad_flag = max(mad_flag)) %>%
        ungroup() %>%
        mutate(Good_Day = if_else(
            flat_flag > 0 | maxvol_flag > 0 | mad_flag > 0, 
            as.integer(0), 
            as.integer(1))) %>%
        select(
            SignalID:flatlined, flat_strk, flat_flag, maxvol_flag, mad_flag, Good_Day) %>%
        mutate(
            Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
            Hour = Month_Hour - months(month(Month_Hour) - 1),
            vol = if_else(Good_Day==1, vol, as.double(NA)))
}


filtered_counts_hack <- function(filtered_counts) {
    
    # This is an unabashed hack to temporarily deal with one problematic phase
    
    # 21 - max(vol) > 1000 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 21)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))
    
    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day
    
    # 22 - max(vol) > 1000 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 22)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))
    
    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day
    
    # 23 - max(vol) > 500 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 23)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 500, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))
    
    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day
    
    filtered_counts
}



get_adjusted_counts <- function(filtered_counts, ends_with = NULL) {
    
    usable_cores <- get_usable_cores()
    
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    if (!is.null(ends_with)) {
        det_config <- det_config %>% 
            filter(endsWith(as.character(SignalID), ends_with))
    }
    
    filtered_counts %>%
        left_join(det_config, by = c("SignalID", "CallPhase", "Detector", "Date")) %>%
        filter(!is.na(CountPriority)) %>%
        
        split(.$SignalID) %>% mclapply(function(fc) {
            fc <- fc %>% 
                mutate(DOW = wday(Timeperiod),
                       vol = as.double(vol))
            
            ## Phase Contribution - The fraction of volume within each phase on each detector
            ## For instance, for a three-lane phase with equal volumes on each lane, 
            ## ph_contr would be 0.33, 0.33, 0.33.
            ph_contr <- fc %>%
                group_by(SignalID, CallPhase, Timeperiod) %>% 
                mutate(na.vol = sum(is.na(vol))) %>%
                ungroup() %>% 
                filter(na.vol == 0) %>% 
                dplyr::select(-na.vol) %>% 
                
                # phase contribution factors--fraction of phase volume a detector contributes
                group_by(SignalID, Timeperiod, CallPhase) %>% 
                mutate(share = vol/sum(vol)) %>% 
                filter(share > 0.1, share < 0.9) %>% 
                group_by(SignalID, CallPhase, Detector) %>% 
                summarize(Ph_Contr = mean(share, na.rm = TRUE), .groups = "drop")
            
            ## Get the expected volume for the phase based on the volumes and phc
            ## for the detectors with volumes (not NA)
            fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
                group_by(SignalID, Timeperiod, CallPhase) %>%
                mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
            
            ## Fill in the NA detector volumes with the expected volume for the phase
            ## and the phc for the missing detectors within the same timeperiod
            fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
            
            ## Calculate median hourly volumes over the month by DOW 
            ## to fill in missing data for all detectors in a phase
            mo_dow_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
                summarize(dow_hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
            
            ## Calculate median hourly volumes over the month (for all days, not by DOW)
            ## to fill in missing data for all detectors in a phase
            mo_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, Month_Hour) %>% 
                summarize(hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            
            fc_phc %>% 
                # fill in missing detectors by hour and day of week volume in the month
                left_join(mo_dow_hrly_vols, 
                          by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(dow_hrly_vol), as.integer(vol))) %>%
                
                # fill in remaining missing detectors by hourly volume in the month
                left_join(mo_hrly_vols,
                          by = (c("SignalID", "CallPhase", "Detector", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(hrly_vol), as.integer(vol))) %>%
                
                dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
                
                filter(!is.na(vol))
        }, mc.cores = usable_cores) %>% bind_rows() #ceiling(parallel::detectCores()*1/3)
}
## -- --- End of Adds CountPriority from detector config file -------------- -- ##


get_adjusted_counts_split10 <- function(filtered_counts, callback = function(x) {x}) {
    lapply(as.character(seq_len(9)), function(i) {
        file.remove(glue("fc{i}.fst"))
    })
    lapply(as.character(seq_len(9)), function(i) {
        filtered_counts %>% 
            filter(endsWith(as.character(SignalID), i)) %>%
            write_fst(glue("fc{i}.fst"))
    })
    rm(filtered_counts)
    
    usable_cores <- get_usable_cores()
    
    ac <- lapply(as.character(seq_len(9)), function(i) {
        read_fst(glue("fc{i}.fst")) %>% 
            get_adjusted_counts(ends_with = i) %>%
            callback()
    }) %>% bind_rows() %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase)) %>% 
        arrange(SignalID, CallPhase, Date)
    
    lapply(as.character(seq_len(9)), function(i) {
        file.remove(glue("fc{i}.fst"))
    })
    ac
}



# Variant that splits signals into equally sized chunks
# may be a template for other memory-intenstive functions.
get_adjusted_counts_split <- function(filtered_counts) {
    
    print("Getting detector config...")
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    # Define temporary directory and file names
    temp_dir <- tempdir()
    if (!dir.exists(temp_dir)) {
        dir.create(temp_dir)
    }
    temp_file_root <- stringi::stri_rand_strings(1,8)
    temp_path_root <- file.path(temp_dir, temp_file_root)
    print(temp_path_root)
    
    # Join with det_config
    print("Joining with detector configuration...")
    filtered_counts <- filtered_counts %>%
        mutate(
            SignalID = factor(SignalID), 
            CallPhase = factor(CallPhase),
            Detector = factor(Detector)) %>%
        left_join(det_config, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        filter(!is.na(CountPriority))
    
    print("Writing to temporary files by SignalID...")
    signalids <- as.character(unique(filtered_counts$SignalID))
    splits <- split(signalids, ceiling(seq_along(signalids)/100))
    lapply(
        names(splits),
        function(i) {
            #print(paste0(temp_file_root, "_", i, ".fst"))
            cat('.')
            filtered_counts %>%
                filter(SignalID %in% splits[[i]]) %>%
                write_fst(paste0(temp_path_root, "_", i, ".fst"))
        })
    cat('.', sep='\n')
    
    file_names <- paste0(temp_path_root, "_", names(splits), ".fst")
    
    # Read in each temporary file and run adjusted counts in parallel. Afterward, clean up.
    print("getting adjusted counts for each SignalID...")
    df <- mclapply(file_names, mc.cores = usable_cores, FUN = function(fn) {
        cat('.')
        fc <- read_fst(fn) %>% 
            mutate(DOW = wday(Timeperiod),
                   vol = as.double(vol))
        
        ph_contr <- fc %>%
            group_by(SignalID, CallPhase, Timeperiod) %>% 
            mutate(na.vol = sum(is.na(vol))) %>%
            ungroup() %>% 
            filter(na.vol == 0) %>% 
            dplyr::select(-na.vol) %>% 
            
            # phase contribution factors--fraction of phase volume a detector contributes
            group_by(SignalID, CallPhase, Detector) %>% 
            summarize(vol = sum(vol, na.rm = TRUE),
                      .groups = "drop_last") %>% 
            mutate(sum_vol = sum(vol, na.rm = TRUE),
                   Ph_Contr = vol/sum_vol) %>% 
            ungroup() %>% 
            dplyr::select(-vol, -sum_vol)
        
        # fill in missing detectors from other detectors on that phase
        fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
            # fill in missing detectors from other detectors on that phase
            group_by(SignalID, Timeperiod, CallPhase) %>%
            mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
        
        fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
        
        #hourly volumes over the month to fill in missing data for all detectors in a phase
        mo_hrly_vols <- fc_phc %>%
            group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
            summarize(Hourly_Volume = median(vol, na.rm = TRUE), .groups = "drop")

        # fill in missing detectors by hour and day of week volume in the month
        left_join(fc_phc, 
                  mo_hrly_vols, 
                  by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>% 
            ungroup() %>%
            mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
            
            dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
            
            filter(!is.na(vol))
    }) %>% bind_rows()
    cat('.', sep='\n')
    
    mclapply(file_names, mc.cores = usable_cores, FUN = file.remove)

    df
}
