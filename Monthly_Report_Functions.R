
# Monthly_Report_Functions.R
Sys.unsetenv("JAVA_HOME")
library(DBI)
library(RJDBC)			  
library(readxl)
library(readr)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)
library(data.table)
library(feather)
library(fst)
library(parallel)
library(pool)

library(plotly)
library(crosstalk)


# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = c(6,7,8,9); PM_PEAK_HOURS = c(15,16,17,18)

write_fst_ <- function(df, fn, append = FALSE) {
    if (append == TRUE & file.exists(fn)) {
        
        factors <- unique(unlist(
            map(list(df), ~ select_if(df, is.factor) %>% names())
        ))
            
        df_ <- read_fst(fn)
        df_ <- bind_rows(df, df_)  %>% 
            mutate_at(vars(one_of(factors)), factor)
    } else {
        df_ <- df
    }
    write_fst(distinct(df_), fn)
}

get_corridors <- function(corr_fn) {
    readxl::read_xlsx(corr_fn) %>% 
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID=factor(`GDOT MaxView Device ID`), 
                  Zone = as.factor(Zone), 
                  Zone_Group = District,
                  Corridor = as.factor(Group),
                  Milepost = as.numeric(Milepost),
                  Name = Name) %>% 
        #filter(!is.na(SignalID)) %>% 
        mutate(Description = paste(SignalID, Name, sep = ": "))
}
get_corridor_name <- function(string) {
    dplyr::case_when(
        
        # From Inrix, Excel Monthly Report Files
        
        grepl(pattern = "Z1.*( |-)13", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)141", string) ~ "SR 141S",
        grepl(pattern = "Z1.*( |-)155", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)237", string) ~ "SR 237",
        grepl(pattern = "Z1.*( |-)42", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)9", string) ~ "SR 9-Atlanta",
        
        grepl(pattern = "Z2.*( |-)120", string) ~ "SR 120W",
        grepl(pattern = "Z2.*( |-)140", string) ~ "SR 140-Roswell",
        grepl(pattern = "Z2.*( |-)280", string) ~ "SR 280",
        grepl(pattern = "Z2.*( |-)360", string) ~ "SR 360",
        grepl(pattern = "Z2.*( |-)3", string) ~ "SR 3N",
        grepl(pattern = "Z2.*( |-)92", string) ~ "SR 92",
        grepl(pattern = "Z2.*( |-)9", string) ~ "SR 9N",
        
        grepl(pattern = "Z3.*( |-)138E", string) ~ "SR 138E",
        grepl(pattern = "Z3.*( |-)138S", string) ~ "SR 138S",
        grepl(pattern = "Z3.*( |-)3S", string) ~ "SR 3S",
        grepl(pattern = "Z3.*( |-)85", string) ~ "SR 85",
        
        grepl(pattern = "Z4.*( |-)278", string) ~ "US 278",
        grepl(pattern = "Z4.*( |-)3", string) ~ "SR 3",
        grepl(pattern = "Z4.*( |-)5", string) ~ "SR 5",
        grepl(pattern = "Z4.*( |-)6", string) ~ "SR 6",
        
        grepl(pattern = "Z5.*SR( |-)10", string) ~ "SR 10",
        grepl(pattern = "Z5.*SR( |-)12", string) ~ "SR 12",
        grepl(pattern = "Z5.*( |-)154", string) ~ "SR 154",
        grepl(pattern = "Z5.*( |-)155", string) ~ "SR 155S",
        grepl(pattern = "Z5.*( |-)42", string) ~ "SR 42",
        grepl(pattern = "Z5.*( |-)8W( |-)DeKalb", string) ~ "SR 8W-DeKalb",
        grepl(pattern = "Z5.*( |-)8W( |-)", string) ~ "SR 8W-Ponce",
        
        grepl(pattern = "Z6.*( |-)120", string) ~ "SR 120E",
        grepl(pattern = "Z6.*( |-)140", string) ~ "SR 140-Gwinnett",
        grepl(pattern = "Z6.*( |-)141", string) ~ "SR 141N",
        grepl(pattern = "Z6.*( |-)20( |-)", string) ~ "SR 20",
        grepl(pattern = "Z6.*( |-)8.*( |-)DeKalb", string) ~ "SR 8E-DeKalb",
        grepl(pattern = "Z6.*( |-)8.*( |-)Gwinnett", string) ~ "SR 8E-Gwinnett",
        grepl(pattern = "Z6.*( |-)9", string) ~ "SR 9-Alpharetta",
        
        grepl(pattern = "North(.*)Ave", string) ~ "North Ave",
        
        # From TEAMS Report
        
        # Zone 1
        startsWith(string, "SR 13") ~ "SR 13/42/155",
        startsWith(string, "SR 141-Fulton") ~ "SR 141S",
        startsWith(string, "SR 141-Dekalb") ~ "SR 141S",
        startsWith(string, "SR 155N") ~ "SR 13/42/155",
        startsWith(string, "SR 237") ~ "SR 237",
        startsWith(string, "SR 42") ~ "SR 13/42/155",
        startsWith(string, "SR 9-Fulton") ~ "SR 9-Atlanta", # Some of this is SR9-Alpharetta (Z6)
        # Zone 2
        startsWith(string, "SR 120-Cobb") ~ "SR 120W",
        startsWith(string, "SR 140-Fulton") ~ "SR 140-Roswell",
        startsWith(string, "SR 280") ~ "SR 280",
        startsWith(string, "SR 360") ~ "SR 360",
        startsWith(string, "SR 3-Cobb") ~ "SR 3N",
        startsWith(string, "SR 92") ~ "SR 92",
        # Zone 3
        startsWith(string, "SR 138E") ~ "SR 138E",
        startsWith(string, "SR 138S") ~ "SR 138S",
        startsWith(string, "SR 3-Clayton") ~ "SR 3S",
        startsWith(string, "SR 3-Henry") ~ "SR 3S",
        startsWith(string, "SR 314-Fayette") ~ "SR 3S",
        startsWith(string, "SR 85") ~ "SR 85",
        startsWith(string, "SR 331-Clayton") ~ "SR 85",
        # Zone 4
        startsWith(string, "US 278") ~ "US 278",
        startsWith(string, "SR 3-Fulton") ~ "SR 3",
        startsWith(string, "SR 5") ~ "SR 5",
        startsWith(string, "SR 6") ~ "SR 6",
        # Zone 5
        startsWith(string, "SR 10") ~ "SR 10",
        startsWith(string, "SR 12") ~ "SR 12",
        startsWith(string, "SR 154") ~ "SR 154",
        startsWith(string, "SR 155S") ~ "SR 155S",
        startsWith(string, "SR 8-Dekalb") ~ "SR 8W-DeKalb", # Some of this is 8E-Dekalb (Z6)
        startsWith(string, "SR 8-Fulton") ~ "SR 8W-Ponce",
        # Zone 6
        startsWith(string, "SR 120-Fulton") ~ "SR 120E",
        startsWith(string, "SR 140-Gwinnett") ~ "SR 140-Gwinnett",
        startsWith(string, "SR 141-Forsyth") ~ "SR 141N",
        startsWith(string, "SR 141-Gwinnett") ~ "SR 141N",
        startsWith(string, "SR 20") ~ "SR 20",
        startsWith(string, "SR 8-Gwinnett") ~ "SR 8E-Gwinnett",
        
        startsWith(string, "-") ~ "",

        # Catchall. Return itself.
        TRUE ~ string)
}
get_15min_counts_older <- function(start_date, end_date, signals_list) {
    
    conn <- dbConnect(odbc::odbc(), 
                      dsn="sqlodbc", 
                      uid=Sys.getenv("ATSPM_USERNAME"), 
                      pwd=Sys.getenv("ATSPM_PASSWORD"))
    
    cel_query <- paste("SELECT *, DATEADD(minute, (DATEDIFF(minute, 0, TimeStamp)/15) * 15, 0) AS Timeperiod",
                       "FROM Controller_Event_Log",
                       "WHERE DATEPART(dw, TimeStamp) in (3,4,5)")
    
    cel <- tbl(conn, sql(cel_query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    counts <- dplyr::filter(cel, Timeperiod >= start_date & Timeperiod < end_date1 &
                                EventCode==82 & 
                                SignalID %in% signals_list) %>%
        rename(Detector = EventParam) %>% 
        group_by(SignalID, Timeperiod, Detector) %>%
        summarize(vol = n())
    
    det <- tbl(conn, "DetectorConfig") %>%
        mutate(SignalID = as.character(SignalID))
    
    left_join(counts, select(det, Detector, CallPhase, SignalID)) %>% 
        dplyr::filter(CallPhase %in% c(2,6)) %>% 
        collect()
    
    # SignalID | CallPhase | Timeperiod | Detector | vol 
}

# counts base query
get_counts <- function(start_date, end_date, signals_list, 
                       interval = "1 hour", event_code = 82L, TWR_only = FALSE, mainline_only = FALSE) {
    
    conn <- dbConnect(odbc::odbc(), 
                      dsn="sqlodbc", 
                      uid=Sys.getenv("ATSPM_USERNAME"), 
                      pwd=Sys.getenv("ATSPM_PASSWORD"))
    
    if (interval=="1 hour") {
        query_select <- "SELECT DISTINCT SignalID, Timestamp, EventCode, EventParam, DATEADD(hour, DATEDIFF(hour, 0, Timestamp), 0) AS Timeperiod"
    } else if (interval=="15 min") {
        query_select <- "SELECT DISTINCT SignalID, Timestamp, EventCode, EventParam, DATEADD(minute, (DATEDIFF(minute, 0, Timestamp)/15) * 15, 0) AS Timeperiod"
    } else {
        print("no interval provided. defaulting to an hour")
        query_select <- "SELECT DISTINCT SignalID, Timestamp, EventCode, EventParam, DATEADD(minute, DATEDIFF(minute, 0, Timestamp), 0) AS Timeperiod"
    }
    
    if (TWR_only==TRUE) {
        query_where <- "WHERE DATEPART(dw, Timestamp) in (3,4,5)"
    } else {
        query_where <- ""
    }
    
    cel_query <- paste(query_select,
                       "FROM Controller_Event_Log",
                       query_where)
    
    print(cel_query)
    
    cel <- tbl(conn, sql(cel_query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    counts <- dplyr::filter(cel, Timestamp >= start_date & Timestamp < end_date1 &
                                EventCode==event_code) %>% 
        rename(Detector = EventParam) %>% 
        group_by(SignalID, Timeperiod, Detector) %>%
        summarize(vol = n())
    
    if (event_code==82L) {
        det <- tbl(conn, "DetectorConfig2")
    } else if (event_code==90L) {
        det <- tbl(conn, "PedestrianConfig")
    } else {
        print("no EventCode provided. assuming 82 (vehicles)")
        det <- tbl(conn, "DetectorConfig2")
    }

    
    ret_df <- left_join(counts, det)
    
    if (mainline_only == TRUE) {
        ret_df <- ret_df %>% dplyr::filter(CallPhase %in% c(2,6))
    } 
    
    ret_df %>% 
        select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
        arrange(SignalID, Detector, Timeperiod) %>%
        collect() %>% 
        ungroup() %>%
        mutate(SignalID = factor(SignalID), 
               Detector = factor(Detector), 
               CallPhase = factor(CallPhase))

    # SignalID | Timeperiod | Detector | CallPhase | vol 
}
# 15-minute counts for volumes and performance measures. TWR only, mainline phases (2,6) only
get_15min_counts <- function(start_date, end_date, signals_list) {
    get_counts(start_date, end_date, signals_list, 
               interval = "15 min", TWR_only = TRUE, mainline_only = TRUE)
}
# 1-hour counts for equipment uptime calcs. All days, all phases
get_1hr_counts <- function(start_date, end_date, signals_list) {
    get_counts(start_date, end_date, signals_list, 
               interval = "1 hour", TWR_only = FALSE, mainline_only = FALSE)
}
# 15-min counts over all dates, phases for QC plots
get_qc_counts <- function(start_date, end_date, signals_list) {
    get_counts(start_date, end_date, signals_list, 
               interval = "15 min", TWR_only = FALSE, mainline_only = FALSE)
}
# Query SPM Database - base query
get_filtered_counts <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase))
    
    # define included detectors
    all_periods <- seq(min(counts$Timeperiod), max(counts$Timeperiod), by = interval)
    
    num_days <- length(unique(date(counts$Timeperiod)))
    all_periods <- all_periods[wday(all_periods) %in% unique(wday(counts$Timeperiod))]
    
    included_detectors <- counts %>% 
        filter(!is.na(CallPhase)) %>%
        group_by(SignalID, CallPhase, Detector) %>% 
        summarize(Total_Volume = sum(vol, na.rm = TRUE)) %>% 
        dplyr::filter(Total_Volume > (100 * num_days)) %>%
        select(-Total_Volume)
    
    dtlevels <- as.character(sort(as.integer(levels(included_detectors$Detector))))
    cplevels <- as.character(sort(as.integer(levels(included_detectors$CallPhase))))
    
    #  expand to all detectors and time periods
    e <- expand.grid(temp = unique(mutate(included_detectors, 
                                          new = paste(SignalID, CallPhase, Detector, sep = '|'))$new),
                     Timeperiod = all_periods) %>%
        separate(temp, c("SignalID","CallPhase","Detector")) %>%
        mutate(SignalID = factor(SignalID), 
               Detector = factor(Detector, levels = dtlevels), 
               CallPhase = factor(CallPhase, levels = cplevels))
    
    # Bad hour = any of the following:
    #    missing data (NA)
    #    volume > 1000
    #    absolute change in volume from previous hour > 500
    #    change in volume from previous hour = 0
    expanded_counts <- left_join(e, counts) %>% 
        mutate(SignalID = factor(SignalID), 
               Detector = factor(Detector), 
               CallPhase = factor(CallPhase),
               vol0 = ifelse(is.na(vol), 0, vol)) %>%
        group_by(SignalID, CallPhase, Detector) %>% 
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>% 
        mutate(delta_vol = vol0 - lag(vol0),
               Good = ifelse(is.na(vol) | 
                                 vol > 1000 | 
                                 is.na(delta_vol) | 
                                 abs(delta_vol) > 500 | 
                                 abs(delta_vol) == 0,
                             0, 1)) %>%
        select(-vol0)

    # bad day = any of the following:
    #    too many bad hours (60%) based on the above criteria
    #    mean absolute change in hourly volume > 200 
    bad_days <- expanded_counts %>% 
        filter(hour(Timeperiod) > 5) %>%
        group_by(SignalID, CallPhase, Detector, Date = date(Timeperiod)) %>% 
        summarize(Good = sum(Good, na.rm = TRUE), 
                  All = n(), 
                  Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
                  mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE)) %>% 
        mutate(Good_Day = as.integer(ifelse(Pct_Good >= 60 & mean_abs_delta < 200, 1, 0))) # manually calibrated
    
    # counts with the bad days taken out
    filtered_counts <- left_join(mutate(expanded_counts, Date=date(Timeperiod)), 
                                 select(bad_days, SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)) %>%
        mutate(vol = ifelse(Good_Day==1, vol, NA),
               Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
               Hour = Month_Hour - months(month(Month_Hour) - 1))
    
    filtered_counts

    # -------------------------------------------------------------------

}
get_adjusted_counts <- function(filtered_counts) {
    
    filtered_counts <- mutate(filtered_counts, DOW = wday(Timeperiod))
    
    # signal|callphase|timeperiod where there is at least one na in vol
    na_vol_counts <- filtered_counts %>% 
        group_by(SignalID, CallPhase, Timeperiod) %>% 
        summarize(na.vol = sum(is.na(vol))) %>% 
        filter(na.vol > 0)
    
    # exclude above from ph_contr calc (below)
    fc <- anti_join(filtered_counts, na_vol_counts)
    
    # phase contribution factors--fraction of phase volume a detector contributes
    ph_contr <- fc %>% 
        group_by(SignalID, CallPhase, Detector) %>% 
        summarize(vol = sum(vol, na.rm = TRUE)) %>% 
        group_by(SignalID, CallPhase) %>% 
        mutate(Ph_Contr = vol/sum(vol)) %>% 
        select(-vol) %>% ungroup()
        
    # SignalID | CallPhase | Detector | Ph_Contr
    

    # fill in missing detectors from other detectors on that phase
    fc_phc <- left_join(filtered_counts, ph_contr) %>% 
        # fill in missing detectors from other detectors on that phase
        group_by(SignalID, Timeperiod, CallPhase) %>%
        mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()

    fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
    
    #hourly volumes over the month to fill in missing data for all detectors in a phase
    mo_hrly_vols <- fc_phc %>%
        group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
        summarize(Hourly_Volume = median(vol, na.rm = TRUE)) %>%
        ungroup()
    # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
        
    # fill in missing detectors by hour and day of week volume in the month
    left_join(fc_phc, mo_hrly_vols) %>%
        ungroup() %>%
        mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), vol)) %>%
        
        select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
        
        filter(!is.na(vol))
    
    # SignalID | CallPhase | Timeperiod | Detector | vol 
}

# Pivot Counts - for old monthly report
get_pivot_counts <- function(filtered_counts, output_filename = NULL) {
    
    spread_volumes <- filtered_counts %>%
        mutate(Timeperiod = floor_date(Timeperiod, unit = "1 hour")) %>%
        group_by(SignalID, Detector, Timeperiod) %>%
        summarize(Volume = sum(vol)) %>%
        ungroup() %>%
        
        select(SignalID, Detector, Timeperiod, Volume) %>%
        spread(Detector, Volume) %>%
        mutate_at(vars(-(SignalID:Timeperiod)), funs(replace(., is.na(.), 0))) %>%
        arrange(SignalID, Timeperiod)
    
    if (!is.null(output_filename)) {
        write.csv(spread_volumes, output_filename)
    }
    spread_volumes
}
get_1hr_ped_counts <- function(start_date, end_date, signals_list) {
    get_counts(start_date, end_date, signals_list,
               interval = "1 hour", event_code = 90, TWR = FALSE, mainline_only = FALSE)
}
get_spm_data <- function(start_date, end_date, signals_list, table, TWR_only=TRUE) {
    
    conn <- dbConnect(odbc::odbc(), 
                      dsn="sqlodbc", 
                      uid=Sys.getenv("ATSPM_USERNAME"), 
                      pwd=Sys.getenv("ATSPM_PASSWORD"))

    if (TWR_only==TRUE) {
        query_where <- "WHERE DATEPART(dw, CycleStart) in (3,4,5)"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", table, query_where)
    
    df <- tbl(conn, sql(query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    dplyr::filter(df, CycleStart >= start_date & CycleStart < end_date1 &
                      SignalID %in% signals_list)
}
get_spm_data_aws <- function(start_date, end_date, signals_list, table, TWR_only=TRUE) {
    
    #conn <- DBI::dbConnect(odbc::odbc(), dsn = "GDOT_SPM_Athena")#,
    #                        ProxyHost = "gdot-enterprise",
    #                        ProxyPort = "8080",
    #                        ProxyUid = Sys.getenv("GDOT_USERNAME"),
    #                        ProxyPwd = Sys.getenv("GDOT_PASSWORD"),
    #                        UseProxy = 1
    # )
    # 
    drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
                classPath = "../../AthenaJDBC42_2.0.2.jar",
                identifier.quote = "'")

    conn <- dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                      s3_staging_dir = 's3://gdot-spm-athena',
                      user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                      password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                      ProxyHost = "gdot-enterprise",
                      ProxyPort = "8080",
                      ProxyUID = Sys.getenv("GDOT_USERNAME"),
                      ProxyPWD = Sys.getenv("GDOT_PASSWORD"))

    
    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", paste0("gdot_spm.", tolower(table)), query_where)
													  
    df <- tbl(conn, sql(query))
    
    #start_date<- as.character(start_date)
    #end_date1 <- as.character(ymd(end_date) + days(1))
    end_date1 <- ymd(end_date) + days(1)
    
    signals_list <- as.integer(signals_list)
								 
    dplyr::filter(df, date >= start_date & date < end_date1) # &
                      #signalid %in% signals_list)
}
# Query Cycle Data
get_cycle_data <- function(start_date, end_date, signals_list) {
    get_spm_data_aws(start_date, end_date, signals_list, table="CycleData")
}
# Query Detection Events
get_detection_events <- function(start_date, end_date, signals_list) {
    get_spm_data_aws(start_date, end_date, signals_list, table="DetectionEvents")
}
get_detector_count <- function(signals_list) {
    conn <- dbConnect(odbc::odbc(), 
                      dsn="sqlodbc", 
                      uid=Sys.getenv("ATSPM_USERNAME"), 
                      pwd=Sys.getenv("ATSPM_PASSWORD"))
    det <- tbl(conn, "DetectorConfig") %>%
        dplyr::filter(SignalID %in% signals_list & CallPhase %in% c(2,6)) %>%
        mutate(SignalID = as.character(SignalID)) %>%
        collect() %>%
        group_by(SignalID, CallPhase) %>%
        summarize(Detectors = n())
}

get_bad_detectors <- function(filtered_counts_1hr) {
    filtered_counts_1hr %>% 
        ungroup() %>% 
        filter(Good_Day==0) %>% 
        distinct(Date, SignalID, Detector) %>% 
        arrange(Date, SignalID, Detector)
}

# Volume VPD
get_vpd <- function(counts) {
    
    counts %>%
        filter(CallPhase %in% c(2,6)) %>% # sum over Phases 2,6 # added 4/24/18
        mutate(DOW = wday(Timeperiod), 
               Week = week(Timeperiod),
               Date = date(Timeperiod)) %>% 
        group_by(SignalID, CallPhase, Week, DOW, Date) %>% 
        summarize(vpd = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Date | vpd
}

# SPM Throughput
get_thruput <- function(counts) {
    counts %>%
        mutate(DOW = wday(Date), 
               Week = week(Date)) %>%
        group_by(SignalID, Week, DOW, Date, Timeperiod) %>%
        summarize(vph = sum(vol)) %>%
        group_by(SignalID, Week, DOW, Date) %>%
        summarize(vph = quantile(vph, probs=c(0.95), na.rm = TRUE) * 4) %>%
        ungroup() %>%
        arrange(SignalID, Date) %>%
        mutate(CallPhase = 0) %>%
        select(SignalID, CallPhase, Date, Week, DOW, vph)
    
    # SignalID | CallPhase | Date | Week | DOW | vph
}

# SPM Arrivals on Green -- modified for use with dbplyr on AWS Athena
get_aog <- function(cycle_data) {
    
    df <- cycle_data %>% 
        filter(Phase %in% c(2,6)) %>%
        group_by(SignalID, Phase, CycleStart, EventCode) %>% 
        summarize(Volume=sum(Volume)) %>% 
        group_by(SignalID, Phase, CycleStart) %>% 
        mutate(Total_Volume = sum(Volume),
               Total_Volume = ifelse(Total_Volume==0, 1, Total_Volume),
               CallPhase = Phase) %>% 
        filter(EventCode==1) %>%

						 
        group_by(SignalID = SignalID, CallPhase, 
                 Hour = date_trunc('hour', CycleStart)) %>%
        summarize(vol = sum(Total_Volume),
                  aog = sum(Volume)/sum(Total_Volume)) %>%
        collect %>% ungroup() %>%
        mutate(SignalID = factor(SignalID),
               vol = as.integer(vol),
																  
				   
               CallPhase = factor(CallPhase),
               Date_Hour = lubridate::ymd_hms(Hour),
               Date = date(Date_Hour),
               DOW = factor(wday(Date)), 
               Week = factor(week(Date))) %>%
        ungroup() %>%
        select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, aog, vol)

    # SignalID | CallPhase | Date_Hour | Date | Hour | aog | vol
}
get_daily_aog <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(aog = weighted.mean(aog, vol), vol = sum(vol))
    
    # SignalID | CallPhase | Date | Week | DOW | aog | vol
}

# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                 CallPhase = factor(Phase),
                 Date_Hour = lubridate::ymd_hms(Hour),
                 Date = date(Date_Hour),
                 DOW = wday(Date), 
                 Week = week(Date)) %>%
    select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
    as_tibble()
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | sf | cycles | sf_freq
}

# SPM Queue Spillback
get_qs <- function(detection_events) {
    
    detection_events %>% 
        filter(Phase %in% c(2,6)) %>%
        
        group_by(SignalID,
                 CallPhase = Phase,
                 CycleStart) %>%
        summarize(occ = approx_percentile(DetDuration, 0.95)) %>%
        ungroup() %>%
        group_by(SignalID, 
                 CallPhase,
                 Hour = date_trunc('hour', CycleStart)) %>%
        summarize(cycles = n(), 
                  qs = count_if(occ > 3)) %>%
        collect() %>%
        ungroup() %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date_Hour = ymd_hms(Hour),
               Date = lubridate::floor_date(Date_Hour, unit="days"),
               DOW = factor(wday(Date_Hour)),
               Week = factor(week(Date_Hour)),
               qs = as.integer(qs),
               cycles = as.integer(cycles),
               qs_freq = as.double(qs)/as.double(cycles)) %>%
        select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, qs, cycles, qs_freq)
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
}

# SPM Travel Time Index, Buffer Time Index
get_tt_xl <- function(fns, rts) {
    
    dfs <- mapply(function(f,r) {
        df <- readxl::read_excel(f) %>% mutate(Corridor = r)
    }, fns, rts, SIMPLIFY = FALSE)
    
    rbindlist(dfs) %>% 
        mutate(miles = speed /3600 * travel_time_seconds, 
               ref_sec = miles/reference_speed * 3600) %>%
        group_by(Corridor = factor(Corridor), measurement_tstamp) %>%
        summarize(travel_time_seconds = sum(travel_time_seconds),
                  ref_sec = sum(ref_sec)) %>%
        ungroup() %>%
        mutate(tti = travel_time_seconds/ref_sec, 
               hour = measurement_tstamp - days(day(measurement_tstamp) - 1)) %>%
        group_by(Corridor, hour) %>%
        summarize(tti = mean(travel_time_seconds/ref_sec),
                  bti = quantile(travel_time_seconds, c(0.90))/mean(ref_sec)) %>% 
        tidyr::gather(idx, value, tti, bti) %>% 
        as_tibble()
    
    # Corridor | hour | idx | value
}
get_tt_csv_older <- function(fns) {
    
    dfs <- lapply(fns, function(f) {
        data.table::fread(f) %>% 
            mutate(measurement_tstamp = ymd_hms(measurement_tstamp),
                   travel_time_seconds = travel_time_minutes * 60,
                   Corridor = get_corridor_name(f)) %>%
            filter(wday(measurement_tstamp) %in% c(TUE,WED,THU)) %>%
            mutate(miles = speed /3600 * travel_time_seconds, 
                   ref_sec = miles/reference_speed * 3600) %>%
            group_by(Corridor = factor(Corridor), measurement_tstamp) %>%
            summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                      ref_sec = sum(ref_sec),
                      miles = sum(miles)) %>%
            ungroup() %>%
            mutate(tti = travel_time_seconds/ref_sec, 
                   date_hour = floor_date(measurement_tstamp, "hours"),
                   hour = date_hour - days(day(date_hour) - 1)) %>%
            group_by(Corridor, hour) %>%
            summarize(tti = mean(travel_time_seconds/ref_sec),
                      pti = quantile(travel_time_seconds, c(0.90))/mean(ref_sec)) %>%
            ungroup()
    })
    df <- bind_rows(dfs) %>% mutate(Corridor = factor(Corridor))

    tti <- select(df, Corridor, Hour = hour, tti) %>% as_tibble()
    pti <- select(df, Corridor, Hour = hour, pti) %>% as_tibble()
    
    list("tti" = tti, "pti" = pti)
    
    # Corridor | hour | idx | value
}
get_tt_csv <- function(fns) {
    
    dfs <- lapply(fns, function(f) {
        data.table::fread(f) %>% 
            mutate(measurement_tstamp = ymd_hms(measurement_tstamp),
                   travel_time_seconds = travel_time_minutes * 60,
                   Corridor = get_corridor_name(f)) %>%
            filter(wday(measurement_tstamp) %in% c(TUE,WED,THU)) %>%
            mutate(miles = speed /3600 * travel_time_seconds, 
                   ref_sec = miles/reference_speed * 3600) %>%
            group_by(Corridor = factor(Corridor), measurement_tstamp) %>%
            summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                      ref_sec = sum(ref_sec),
                      miles = sum(miles)) %>%
            ungroup()
    })
    df <- bind_rows(dfs) %>% 
        group_by(Corridor = factor(Corridor),
                 measurement_tstamp) %>%
        summarize(travel_time_seconds = sum(travel_time_seconds),
                  ref_sec = sum(ref_sec),
                  miles = sum(miles)) %>%
        ungroup() %>%
        mutate(tti = travel_time_seconds/ref_sec, 
               date_hour = floor_date(measurement_tstamp, "hours"),
               hour = date_hour - days(day(date_hour) - 1)) %>%
        group_by(Corridor, hour) %>%
        summarize(tti = mean(travel_time_seconds/ref_sec),
                  pti = quantile(travel_time_seconds, c(0.90))/mean(ref_sec)) %>%
        ungroup()
    
    tti <- select(df, Corridor, Hour = hour, tti) %>% as_tibble()
    pti <- select(df, Corridor, Hour = hour, pti) %>% as_tibble()
    
    list("tti" = tti, "pti" = pti)
    
    # Corridor | hour | idx | value
}

# Comm Uptime from detector data. Needs all phases, all days
get_comm_uptime <- function(filtered_counts) {
    filtered_counts %>%
        group_by(SignalID, Timeperiod) %>% 
        summarize(total_comm = n(),
                  bad_comm = sum(is.na(vol))) %>%
        ungroup() %>%
        mutate(uptime = ifelse(bad_comm != total_comm, TRUE, FALSE),
               SignalID = factor(SignalID),
               CallPhase = factor(0),
               Date_Hour = lubridate::ymd_hms(Timeperiod),
               Date = date(Date_Hour),
               DOW = wday(Date), 
               Week = week(Date)) %>%
        select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime)
}

# -- Generic Aggregation Functions
get_Tuesdays_ <- function(df) {
    df %>% 
        ungroup() %>% 
        dplyr::filter(DOW == 3) %>% 
        select(Week, Date) %>% 
        unique() %>% 
        arrange(Week)
}
weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_=NULL) {
    
    per_ <- as.name(per_)
    
    gdf <- left_join(df, corridors) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone, Corridor, Zone_Group, !!per_) 
    
    if (is.null(wt_)) {
        gdf %>% 
            summarize(!!var_ := mean(!!var_), na.rm = TRUE) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            ungroup() %>%
            select(Zone, Corridor, Zone_Group, !!per_, !!var_, delta)
    } else {
        gdf %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            ungroup() %>%
            select(Zone, Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta)
    }
}
group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                  !!wt_ := sum(!!wt_)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        mutate(Zone_Group = corr_grp) %>% 
        select(Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta) # addition of Zone_Group is new
}
group_corridor_by_sum_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := sum(!!var_)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        mutate(Zone_Group = corr_grp) %>% # this is new and untested
        select(Corridor, Zone_Group, !!per_, !!var_, delta) # addition of Zone_Group is new
}

group_corridor_by_date <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Date"), var_, wt_, corr_grp)
}
group_corridor_by_month <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Month"), var_, wt_, corr_grp)
}
group_corridor_by_hour <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Hour"), var_, wt_, corr_grp)
}

group_corridors_ <- function(df, per_, var_, wt_, gr_ = group_corridor_by_) {
    
    per_ <- as.name(per_)
    
    zgs <- lapply(c("RTOP1", "RTOP2", "D1", "D2", "D3", "D4", "D5", "D6", "Zone 7"), function(zg) {
        df %>%
            filter(Zone_Group == zg) %>%
            gr_(per_, var_, wt_, zg)
    })
    all_rtop_df_out <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        gr_(per_, var_, wt_, "All RTOP")

    dplyr::bind_rows(select_(df, "Corridor", "Zone_Group", per_, var_, wt_, "delta"),
                     zgs,
                     all_rtop_df_out) %>%
        mutate(Corridor = factor(Corridor))
}

get_daily_avg <- function(df, var_, wt_="ones") {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    df %>%
        group_by(SignalID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_)) %>% # Sum of phases 2,6
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        select(SignalID, Date, !!var_, !!wt_, delta)
    
    # SignalID | Date | var_ | wt_ | delta
}
get_daily_sum <- function(df, var_, per_) {
    
    var_ <- as.name(var_)
    per_ <- as.name(per_)
    
    
    df %>%
        group_by(SignalID, !!per_) %>% 
        summarize(!!var_ := sum(!!var_)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        select(SignalID, !!per_, !!var_, delta)
}

get_weekly_sum_by_day <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- get_Tuesdays_(df)
    
    df %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := mean(!!var_)) %>% # Mean over 3 days in the week
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := sum(!!var_)) %>% # Sum of phases 2,6
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        left_join(Tuesdays) %>%
        select(SignalID, Date, Week, !!var_, delta)
    
    # SignalID | Date | var_
}
get_weekly_avg_by_day <- function(df, var_, wt_="ones") {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    Tuesdays <- get_Tuesdays_(df)
    
    df %>%
        filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                  !!wt_ := sum(!!wt_)) %>% # Mean over 3 days in the week
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_)) %>% # Sum of phases 2,6
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        left_join(Tuesdays) %>%
        select(SignalID, Date, Week, !!var_, !!wt_, delta)
    
    # SignalID | Date | vpd
}
get_cor_weekly_avg_by_day <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    # refactored averaging by RTOP1, RTOP2, All RTOP -- this is new
    group_corridors_(cor_df_out, "Date", var_, wt_)
}
get_monthly_avg_by_day <- function(df, var_, wt_=NULL) {
    
    var_ <- as.name(var_)
    
    gdf <- df %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(SignalID, CallPhase, Month)
        
    if (is.null(wt_)) {
        rdf <- gdf %>%
            summarize(!!var_ := mean(!!var_)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := sum(!!var_)) %>% # Sum over Phases (2,6)
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    } else {
        wt_ <- as.name(wt_)
        rdf <- gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                      !!wt_ := sum(!!wt_)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    }
    rdf
}
get_cor_monthly_avg_by_day <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Month", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, "Month", var_, wt_)
}

get_weekly_avg_by_hr <- function(df, var_, wt_=NULL) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- df %>% 
        mutate(Date = date(Hour)) %>%
        get_Tuesdays_()
    
    df_ <- left_join(df, Tuesdays) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)
    
    gdf <- df_ %>%
        group_by(SignalID, CallPhase, Week, Hour) 
    
    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := mean(!!var_)) %>% # Mean of phases 2,6
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                      !!wt_ := sum(!!wt_)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_)) %>% # Sum of phases 2,6
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            select(SignalID, Hour, Week, !!var_, !!wt_, delta)
    }
}
get_cor_weekly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    
    # refactored averaging by RTOP1, RTOP2, All RTOP -- this is new
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

get_sum_by_hr <- function(df, var_) {
    
    var_ <- as.name(var_)

    df %>%  
        mutate(DOW = wday(Timeperiod),
               Week = week(Timeperiod),
               Hour = floor_date(Timeperiod, unit = '1 hour')) %>%
        group_by(SignalID, CallPhase, Week, DOW, Hour) %>%
        summarize(!!var_ := sum(!!var_)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    
    # SignalID | CallPhase | Week | DOW | Hour | var_ | delta
    
} ## unused. untested

get_avg_by_hr <- function(df, var_, wt_=NULL) {
    
    df_ <- as.data.table(df)

    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour), 
                                           week(Date_Hour), 
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_)),
                       sum(get(wt_))), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret <- ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
    }
    ret
}

get_avg_by_hr_older <- function(df, var_, wt_=NULL) {
  
    var_ <- as.name(var_)
    
    gdf <- df %>%  
        mutate(DOW = wday(Date_Hour),
               Week = week(Date_Hour),
               Hour = floor_date(Date_Hour, unit = '1 hour')) %>%
        group_by(SignalID, CallPhase, Week, DOW, Hour)
    
    if (is.null(wt_)) {
        gdf %>% summarize(!!var_ := mean(!!var_)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    } else {
        wt_ <- as.name(wt_)
        gdf %>% summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                          !!wt_ := sum(!!wt_)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    }
    
    # SignalID | CallPhase | Week | DOW | Hour | var_ | wt_
} 
get_monthly_avg_by_hr <- function(df, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    df %>% 
        group_by(SignalID, Hour) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                  !!wt_ := sum(!!wt_)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                  !!wt_ := sum(!!wt_)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    
    # SignalID | CallPhase | Hour | vph
}
get_cor_monthly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

# Device Uptime from Excel files from Field Engineers
get_device_uptime_from_xl <- function(fn, range) {
    
    h <- names(readxl::read_excel(fn, range = "Device Status!A5:U5"))
    df <- readxl::read_excel(fn, range = range)
    names(df) <- h
    
    df[1,1] <- "Functioning"
    df[2,1] <- "Total"
    
    df %>% gather(Month, num, -X__1) %>% 
        spread(key = X__1, value = num) %>%
        transmute(Corridor = "Placeholder",
                  Month = dmy(paste0("1-", Month)),
                  up = as.integer(Functioning),
                  num = as.integer(Total),
                  uptime = up/num,
                  Filename = fn) %>% 
        filter(!is.na(up)) %>% 
        arrange(Month) %>%
        mutate(Corridor = get_corridor_name(fn))
}
get_device_uptime_from_xl_multiple <- function(fns, range, corridors) {
    dfs <- lapply(fns, function(x) get_device_uptime_from_xl(x, range))
    df <- bind_rows(dfs)
    
    corrs <- corridors %>% distinct(Corridor, Zone_Group) %>%
        mutate(Corridor = factor(Corridor),
               SignalID = factor(0))
    df_ <- left_join(df, corrs)
    
    df_ %>% select(Zone_Group, Corridor, Month, up, num, uptime)
    
}
# -- end Generic Aggregation Functions

get_daily_detector_uptime <- function(filtered_counts) {
    
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE)) %>%
        dplyr::filter(vol == 0) %>%
        select(-vol)
    fc <- anti_join(filtered_counts, bad_comms)
    
    ddu <- fc %>% 
        filter(!wday(Timeperiod) %in% c(1,7)) %>%
        mutate(Date_Hour = Timeperiod,
                                      Date = date(Date_Hour)) %>%
        select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup() %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
        summarize(uptime = as.double(sum(Good_Day))/as.double(n()), all = as.double(n()))
    split(ddu, ddu$setback)
}
get_avg_daily_detector_uptime <- function(daily_detector_uptime) {
    
    sb_daily_uptime <- get_daily_avg(daily_detector_uptime$Setback, "uptime", "all")
    pr_daily_uptime <- get_daily_avg(daily_detector_uptime$Presence, "uptime", "all")
    
    full_join(sb_daily_uptime, pr_daily_uptime, 
              by = c("SignalID", "Date"), 
              suffix = c(".sb", ".pr")) %>%
        mutate(uptime.all = (uptime.sb * all.sb + uptime.pr * all.pr)/(all.sb + all.pr)) %>% 
        select(-starts_with("delta"))
}
get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    cor_daily_sb_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb")
    cor_daily_pr_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr")
    cor_daily_all_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.all)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.all", "ones")
    
    full_join(select(cor_daily_sb_uptime, -c(all.sb, delta)),
              select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Zone_Group = factor(Zone_Group))
}

get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_weekly_sum_by_day(vpd, "vpd")
}
get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}
get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol")
}
get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles")
}
get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles")
}

get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}
get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}
get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}
get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}
get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}

get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(vpd, "vpd")
}
get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph")
}
get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol")
}
get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles")
}
get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles")
}

get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}
get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}
get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}
get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}
get_cor_monthly_tti <- function(cor_monthly_tti_by_hr, corridors) {
    cor_monthly_tti_by_hr %>% 
        mutate(Month = as_date(Hour)) %>%
        filter(!Corridor %in% c("RTOP1", "RTOP2", "All RTOP", "D5", "Zone 7")) %>%
        get_cor_monthly_avg_by_day(corridors, "tti", "pct")
}
get_cor_monthly_pti <- function(cor_monthly_pti_by_hr, corridors) {
    cor_monthly_pti_by_hr %>% mutate(Month = as_date(Hour))  %>%
        filter(!Corridor %in% c("RTOP1", "RTOP2", "All RTOP", "D5", "Zone 7")) %>%
        get_cor_monthly_avg_by_day(corridors, "pti", "pct")
}

get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        ungroup() %>%
        mutate(num.all = all.sb + all.pr,
               CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime.all", "num.all") %>%
        arrange(SignalID, Month)
}

get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    cor_daily_sb_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb")
    cor_daily_pr_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr")
    cor_daily_all_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.all)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.all", "ones")
    
    full_join(select(cor_daily_sb_uptime, -c(all.sb, delta)),
              select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}

get_vph <- function(counts) {
    df <- counts %>% mutate(Date_Hour = floor_date(Timeperiod, "1 hour"))
    get_sum_by_hr(df, "vol") %>% 
        filter(CallPhase %in% c(2,6)) %>% # sum over Phases 2,6
        group_by(SignalID, Week, DOW, Hour) %>% 
        summarize(vph = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vph
}
get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vol
}
get_sf_by_hr <- function(sf) {
    get_avg_by_hr(sf, "sf_freq", "cycles")
}
get_qs_by_hr <- function(qs) {
    get_avg_by_hr(qs, "qs_freq", "cycles")
}

get_monthly_vph <- function(vph) {
    
    vph %>% 
        ungroup() %>%
        filter(DOW %in% c(TUE,WED,THU)) %>%
        group_by(SignalID, Hour) %>% summarize(vph = sum(vph)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE))
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_aog_by_hr <- function(aog_by_hr) {
    
    aog_by_hr %>% 
        group_by(SignalID, Hour) %>% 
        summarize(aog = weighted.mean(aog, vol), vol = sum(vol)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol), vol = sum(vol))
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_sf_by_hr <- function(sf_by_hr) {
    
    get_monthly_aog_by_hr(rename(sf_by_hr, aog = sf_freq, vol = cycles)) %>%
        rename(sf_freq = aog, cycles = vol)
}
get_monthly_qs_by_hr <- function(qs_by_hr) {
    
    get_monthly_aog_by_hr(rename(qs_by_hr, aog = qs_freq, vol = cycles)) %>%
        rename(qs_freq = aog, cycles = vol)
}

get_cor_monthly_vph <- function(monthly_vph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_vph, corridors, "vph")
    # Corridor | Hour | vph
}
get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
    # Corridor | Hour | vph
}
get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}
get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {
    
    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>% 
        group_by(Corridor, month(Hour)) %>% 
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>% 
        group_by(Corridor, Zone_Group, hr = hour(Hour)) %>% 
        summarize(pct = mean(pct, na.rm = TRUE))
    
    df <- left_join(ti, corridors %>% distinct(Corridor, Zone_Group)) %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else {
        tindx = "oops"
    }
    
    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}
# No longer used. Generalized the previous function for both tti and pti
get_cor_monthly_pti_by_hr <- function(pti, cor_monthly_vph, corridors) {

    df <- left_join(pti, corridors %>% distinct(Corridor, Zone_Group)) %>%
        left_join(select(cor_monthly_vph, -Zone_Group)) %>%
        ungroup() %>%
        tidyr::replace_na(list(vph = 1))
    
    get_cor_monthly_avg_by_hr(df, corridors, "pti", "vph")
}

get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}
get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}
get_cor_weekly_vph_peak <- function(cor_weekly_vph) {
    dfs <- get_cor_monthly_vph_peak(cor_weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}
get_weekly_vph_peak <- function(weekly_vph) {
    dfs <- get_monthly_vph_peak(weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}

get_monthly_vph_peak <- function(monthly_vph) {
    
    am <- dplyr::filter(monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    pm <- dplyr::filter(monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}

# vph during peak periods
get_cor_monthly_vph_peak <- function(cor_monthly_vph) {
    
    am <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))

    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))

    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}
# aog during peak periods -- unused
get_cor_monthly_aog_peak <- function(cor_monthly_aog_by_hr) {
    
    am <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))

    pm <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>% 
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}

# Device Uptime from Excel
get_veh_uptime_from_xl_monthly_reports <- function(fns, corridors) {
    get_device_uptime_from_xl_multiple(fns, "Device Status!A5:U7", corridors)
}
get_ped_uptime_from_xl_monthly_reports <- function(fn, corridors) {
    get_device_uptime_from_xl_multiple(fn, "Device Status!A9:U11", corridors)
}
get_cctv_uptime_from_xl_monthly_reports <- function(fns, corridors) {
    get_device_uptime_from_xl_multiple(fns, "Device Status!A13:U15", corridors)
}

get_det_uptime_from_manual_xl <- function(fn, date_string) {
    
    xl <- readxl::read_excel(fn) %>% 
        fill(Corridor) %>%
        mutate_all(stringi::stri_trim) %>%
        mutate(Zone_Group = case_when(
            startsWith(as.character(Corridor), "Z1") ~ "RTOP1",
            startsWith(as.character(Corridor), "Z2") ~ "RTOP1",
            startsWith(as.character(Corridor), "Z3") ~ "RTOP1",
            TRUE ~ "RTOP2")
        ) %>%
        transmute(xl_Corridor = Corridor,
                  Corridor = factor(get_corridor_name(Corridor)),
                  Zone_Group = Zone_Group,
                  Month = ymd(date_string),
                  Type = factor(`Detector Type`),
                  up = as.integer(`# of Operational Detectors`),
                  num = as.integer(`Total # of Detectors`),
                  uptime = as.double(up)/num) %>% 
        group_by(Corridor, Zone_Group, Month, Type) %>% 
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE)) %>%
        ungroup() %>%
        select(Zone_Group, Corridor, Month, Type, up, num, uptime)
    xl
}
get_cor_monthly_xl_uptime <- function(df) {
    
    a <- df %>% group_by(Zone_Group, Corridor, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE)) %>%
        ungroup()

    b <- a %>% 
        group_by(Zone_Group, Corridor = Zone_Group, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE))
    
    c <- a %>% 
        group_by(Zone_Group = "All RTOP", Corridor = Zone_Group, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE))
    
    bind_rows(a, b, c) %>%
        arrange(Zone_Group, Corridor, Month) %>% 
        group_by(Zone_Group, Corridor) %>%
        mutate(delta = (uptime - lag(uptime))/lag(uptime)) %>%
        ungroup()
    
}

# Cross filter Daily Volumes Chart. For Monthly Report ------------------------
get_vpd_plot <- function(cor_weekly_vpd, cor_monthly_vpd) {
    
    sdw <- SharedData$new(cor_weekly_vpd, ~Corridor, group = "grp")
    sdm <- SharedData$new(dplyr::filter(cor_monthly_vpd, month(Month)==10), ~Corridor, group = "grp")
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vpd = mean(vpd)) %>% # This has to be just the current month's vpd
        arrange(vpd) %>%
        add_bars(x = ~vpd, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vpd)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = "October Volume (vpd)", zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, 
                  y = ~vpd, 
                  alpha = 0.3) %>%
        layout(xaxis = list(title = "Vehicles/day"),
               showlegend = FALSE)
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80),
               title = "Volume (veh/day) Trend") %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), 
                                            textposition = "inside"))
}
# Cross filter Hourly Volumes Chart. For Monthly Report -----------------------
get_vphpl_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vphpl = mean(vphpl)) %>% # This has to be just the current month's vphpl
        arrange(vphpl) %>%
        add_bars(x = ~vphpl, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vphpl)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vphpl, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}
get_vph_peak_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vph = mean(vph)) %>% # This has to be just the current month's vph
        arrange(vph) %>%
        add_bars(x = ~vph, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vph)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vph, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


# Convert Monthly data to quarterly for Quarterly Report ####
get_quarterly <- function(monthly_df, var_, wt_="ones", operation = "avg") {
  
  if (wt_ == "ones" & !"ones" %in% names(monthly_df)) {
    monthly_df <- monthly_df %>% mutate(ones = 1)
  }
  
  var_ <- as.name(var_)
  wt_ <- as.name(wt_)
  
  monthly_df <- monthly_df %>% 
    group_by(Corridor, 
             Zone_Group,
             Quarter = as.character(lubridate::quarter(Month, with_year = TRUE)))
  if (operation == "avg") {
    monthly_df <- monthly_df %>%
      summarize(!!var_ := weighted.mean(!!var_, !!wt_), 
                !!wt_ := sum(!!wt_))
  } else if (operation == "sum") {
    monthly_df <- monthly_df %>%
      summarize(!!var_ := sum(!!var_))
  }
  monthly_df %>%
    mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
}

# Activities
tidy_teams <- function(df) {
    df %>% unite(Location, `Location Groups`, County, sep = "-") %>%
        transmute(Task_Type = factor(`Task Type`),
                  Task_Subtype = factor(`Task Subtype`),
                  Task_Source = factor(`Task Source`),
                  Priority = factor(Priority),
                  Status = factor(Status),
                  Corridor = get_corridor_name(Location),
                  Location = factor(Location),
                  `Date Reported` = mdy(`Date Reported`),
                  `Date Resolved` = mdy(`Date Resolved`),
                  `Time To Resolve In Days` = `Time To Resolve In Days`,
                  Maintained_by = ifelse(
                      grepl(pattern = "District 1", `Maintained by`), "D1",
                      ifelse(grepl(pattern = "District 6", `Maintained by`),"D6",
                             ifelse(grepl(pattern = "Consultant|GDOT", `Maintained by`),"D6",
                                    `Maintained by`)))) %>%
        filter(!is.na(`Date Reported`)) %>% as_tibble()
}
read_teams_csv <- function(csv_fn) {
    df <- read.csv(csv_fn)
    names(df) <- gsub("\\.", " ", names(df))
    
    routes <- c("SR 10","SR 12","SR 120","SR 124","SR 13","SR 138E","SR 138S","SR 13N",
                "SR 13S","SR 140","SR 141","SR 154","SR 155N","SR 155S","SR 20","SR 237",
                "SR 280","SR 3","SR 314","SR 331","SR 360","SR 42","SR 5","SR 6","SR 8",
                "SR 8/US 278","SR 85","SR 9","SR 92","US 278")
    df2 <- mutate(df, `Location Groups` = "")
    for (rt in routes) {
        df2 <- dplyr::mutate(df2, `Location Groups` = ifelse(grepl(rt, `Custom Identifier`), rt, `Location Groups`))
    }
    tidy_teams(df2)
}
get_outstanding_events <- function(teams, group_var) {
    
    rep <- teams %>% 
        filter(!is.na(`Date Reported`)) %>%
        mutate(Month = `Date Reported` - days(day(`Date Reported`) - 1)) %>%
        arrange(Month) %>%
        group_by_(group_var, quote(Zone_Group), quote(Month)) %>%
        summarize(Rep = n()) %>% 
        group_by_(quote(Zone_Group), group_var) %>%
        mutate(cumRep = cumsum(Rep))
    
    res <- teams %>% 
        filter(!is.na(`Date Resolved`)) %>%
        mutate(Month = `Date Resolved` - days(day(`Date Resolved`) -1)) %>%
        arrange(Month) %>%
        group_by_(group_var, quote(Zone_Group), quote(Month)) %>%
        summarize(Res = n()) %>% 
        group_by_(quote(Zone_Group), group_var) %>%
        mutate(cumRes = cumsum(Res))
    
    left_join(rep, res) %>% 
        fill(cumRes, .direction = "down") %>% 
        replace_na(list(Rep = 0, cumRep = 0, 
                        Res = 0, cumRes = 0)) %>% 
        group_by_(quote(Zone_Group), group_var) %>%
        mutate(outstanding = cumRep - cumRes) %>%
        ungroup()
}

readRDS_multiple <- function(pattern) {
    lf <- list.files(pattern = pattern)
    lf <- lf[grepl(".rds", lf)]
    bind_rows(lapply(lf, readRDS))
}

volplot_plotly <- function(df, title = "title") {
    
    # Works but colors and labeling are not fully complete.
    pl <- function(dfi) {
        plot_ly(data = dfi) %>% 
            add_ribbons(x = ~Timeperiod, 
                        ymin = 0,
                        ymax = ~vol,
                        color = ~CallPhase,
                        colors = colrs,
                        name = paste('Phase', dfi$CallPhase[1])) %>%
            layout(yaxis = list(range = c(0,1000),
                                tickformat = ",.0"),
                   annotations = list(x = -.05,
                                      y = 0.5,
                                      xref = "paper",
                                      yref = "paper",
                                      xanchor = "right",
                                      text = paste0("[", dfi$Detector[1], "]"),
                                      font = list(size = 12),
                                      showarrow = FALSE)
            )
    }
    
    num_days <- length(unique(date(df$Timeperiod)))
    
    included_detectors <- df %>% 
        filter(!is.na(CallPhase)) %>%
        group_by(SignalID, CallPhase, Detector) %>% 
        summarize(Total_Volume = sum(vol, na.rm = TRUE)) %>% 
        dplyr::filter(Total_Volume > (100 * num_days)) %>%
        select(-Total_Volume) %>%
        mutate(Keep = TRUE)
    
    df <- left_join(df, included_detectors) %>% filter(Keep == TRUE)
    
    dfs <- split(df, df$Detector)
    
    plts <- lapply(dfs[lapply(dfs, nrow)>0], pl)
    subplot(plts, nrows = length(plts), shareX = TRUE) %>%
        layout(annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE))
}

perf_plotly <- function(df, per_, var_, range_max = 1.1, number_format = ",.0%", title = "title") {
    
    per_ <- as.name(per_)
    var_ <- as.name(var_)
    df <- rename(df, per = !!per_, var = !!var_)
    
    plot_ly(data = df) %>% 
        add_ribbons(x = ~per, 
                    ymin = 0,
                    ymax = ~var, 
                    color = I(DARK_GRAY)) %>%
        layout(yaxis = list(range = c(0, range_max),
                            tickformat = number_format),
               annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1.1,
                                  showarrow = FALSE))
}

perf_plotly_by_phase <- function(df, per_, var_, range_max = 1.1, number_format = ".0%", title = "title") {
    
    # Works but colors and labeling are not fully complete.
    pl <- function(dfi) {
        plot_ly(data = dfi) %>% 
            add_ribbons(x = ~per, 
                        ymin = 0,
                        ymax = ~var,
                        color = ~CallPhase,
                        colors = colrs,
                        name = paste('Phase', dfi$CallPhase[1])) %>%
            layout(yaxis = list(range = c(0, range_max),
                                tickformat = number_format))
    }
    
    per__ <- as.name(per_)
    var__ <- as.name(var_)
    df <- rename(df, per = !!per__, var = !!var__)
    
    dfs <- split(df, df$CallPhase)
    
    plts <- lapply(dfs[lapply(dfs, nrow)>0], pl)
    subplot(plts, nrows = length(plts), shareX = TRUE, margin = 0.03) %>%
        layout(annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE))
}

signal_dashboard <- function(sigid,
                             raw_counts_1hr = raw_counts_1hr_,
                             filtered_counts_1hr = filtered_counts_1hr_,
                             vpd = vpd_,
                             avg_daily_detector_uptime = avg_daily_detector_uptime_,
                             daily_comm_uptime = daily_comm_uptime_,
                             aog = aog_,
                             qs = qs_,
                             sf = sf_) {
    
    p_rc <- tryCatch({
        volplot_plotly(raw_counts_1hr %>% filter(SignalID == sigid) %>% mutate(vol = ifelse(is.na(vol), 0, vol)),
                       title = "Raw 1 hr Aggregated Counts") %>% 
            layout(showlegend = FALSE)
    },
    error = function(cond) {
        plot_ly()
    })
    
    p_fc <- tryCatch({
        volplot_plotly(filtered_counts_1hr %>% filter(SignalID == sigid) %>% mutate(vol = ifelse(is.na(vol), 0, vol)),
                       title = "Filtered 1 hr Aggregated Counts") %>% 
            layout(showlegend = FALSE)
    }, error = function(cond) {
        plot_ly()
    })

    p_vpd <- tryCatch({
        perf_plotly_by_phase(vpd %>% filter(SignalID==sigid & !is.na(vpd)), 
                             "Date", "vpd", 
                             range_max = max(vpd$vpd), 
                             number_format = ",.0",
                             title = "Daily Volume by Phase") %>% layout(showlegend = TRUE)
    }, error = function(cond) {
        plot_ly()
    })
    
    p_ddu <- tryCatch({
        perf_plotly_by_phase(avg_daily_detector_uptime %>% filter(SignalID==sigid & !is.na(uptime)), 
                             "Date", "uptime",
                             title = "Daily Detector Uptime") %>% 
            layout(showlegend = FALSE)
    }, error = function(cond) {
        plot_ly()
    })
    
    sr1a <- subplot(list(p_rc, p_vpd), nrows = 2, margin = 0.04, heights = c(0.6, 0.4), shareX = TRUE)
    sr1b <- subplot(list(p_fc, p_ddu), nrows = 2, margin = 0.04, heights = c(0.6, 0.4), shareX = TRUE)
    
    p_com <- perf_plotly(daily_comm_uptime %>% filter(SignalID==sigid), 
                         "Date", "uptime", title = "Daily Communications Uptime") %>% 
        layout(showlegend = FALSE)
    p_aog <- perf_plotly_by_phase(aog %>% filter(SignalID==sigid), 
                                  "Date", "aog", title = "Arrivals on Green") %>% 
        layout(showlegend = FALSE)
    p_qs <- perf_plotly_by_phase(qs %>% filter(SignalID==sigid), 
                                 "Date", "qs_freq", range_max = 0.5, title = "Queue Spillback Rate") %>% 
        layout(showlegend = FALSE)
    p_sf <- perf_plotly_by_phase(sf %>% filter(SignalID==sigid), 
                                 "Date", "sf_freq", range_max = 0.5, title = "Split Failure Rate") %>% 
        layout(showlegend = FALSE)
    
    sr2 <- subplot(list(p_com, p_aog, p_qs, p_sf), nrows = 4, margin = 0.04, heights = c(0.1, 0.2, 0.2, 0.5), shareX = TRUE)
    
    subplot(sr1a, sr1b, sr2)
}


patch_april <- function(df, df4) {
    
    f <- function(df_, df4_) {
        
        if ("Date" %in% names(df4_)) {
            dat <- as.name("Date")
        } else if ("Hour" %in% names(df4_)) {
            dat <- as.name("Hour")
        } else if ("Month" %in% names(df4_)) {
            dat <- as.name("Month")
        }
        
        df_ <- df_ %>% filter(month(!!dat) != 4)
        df4_ <- df4_ %>% filter(month(!!dat) == 4)
        
        bind_rows(df_, df4_) %>% 
            arrange(Zone_Group, Corridor, !!dat) %>%
            mutate(Corridor = factor(Corridor),
                   Zone_Group = factor(Zone_Group))
    }
    
    if (names(df4)[1] == "am") {
        am <- f(df$am, df4$am)
        pm <- f(df$pm, df4$pm)
        result <- list("am" = am, "pm" = pm)
    } else {
        result <- f(df, df4)
    }
    result
}
