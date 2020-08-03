
suppressMessages(library(aws.s3))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(stringr))
suppressMessages(library(arrow))
suppressMessages(library(httr))
suppressMessages(library(fst))
suppressMessages(library(lubridate))
suppressMessages(library(runner))
suppressMessages(library(qs))

read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}
Sys.setenv(https_proxy="http://GADOT%5CV0010894:935EConfed@gdot-enterprise:8080")


get_alerts <- function() {
    
    objs <- aws.s3::get_bucket(bucket = 'gdot-spm', 
                               prefix = 'mark/watchdog/')
    alerts <- lapply(objs, function(obj) {
        key <- obj$Key
        print(key)
        f <- NULL
        if (endsWith(key, "feather.zip")) {
            f <- read_zipped_feather
        } else if (endsWith(key, "fst")) {
            f <- read_fst
        }
        if (!is.null(f)) {
            aws.s3::s3read_using(FUN = f, 
                                 object = key, 
                                 bucket = 'gdot-spm') %>% 
                as_tibble() %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector),
                       Date = date(Date))
        }
    }) %>% bind_rows() %>%
        filter(!is.na(Corridor)) %>%
        replace_na(replace = list(Detector = 0, CallPhase = 0)) %>% # callphase = 0 added
        transmute(
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone),
            Corridor = factor(Corridor),
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Detector = factor(Detector),
            Date = Date,
            Name = as.character(Name),
            Alert = factor(Alert),
            ApproachDesc) %>%
        filter(Date > today() - days(90)) %>% 
        distinct() %>% # Hack to overcome configuration errors
        arrange(Alert, SignalID, CallPhase, Detector, Date) %>% 
        
        # Fill in ApproachDesc for older config files as well as possible
        group_by(
            SignalID, Detector
        ) %>% 
        tidyr::fill(ApproachDesc, .direction = "up") %>%
        ungroup() %>%
        replace_na(list(ApproachDesc = ""))

    rms_alerts <- alerts %>% 
        filter(Zone == "Ramp Meters")
    alerts <- alerts %>% 
        filter(Zone != "Ramp Meters")
    
    rms_alerts <- rms_alerts %>%
        # Small hack to account for isolated issue with CallPhase 2 on 9829
        # Not having ApproachDesc for ramp meters.
        # This ultimately needs to be fixed.
        group_by(
            SignalID, Detector) %>% 
        mutate(
            ApproachDesc = max(ApproachDesc)
        ) %>%
        ungroup() %>%
        
        # First step to group Mainline Detectors together
        # Drop "-Lead/-Trail" from Mainline ApproachDesc
        mutate(
            CallPhase = str_extract(ApproachDesc, "^[^ -]+"),
            CallPhase = if_else(
                CallPhase %in% c("Mainline", "Passage", "Demand", "Queue"),
                CallPhase,
                "Other"),
            ApproachDesc = str_replace(ApproachDesc, "Mainline-\\S+", "Mainline")
        ) %>%
        ungroup()
    
    # Group mainline detectors into Detector = 46/49, for instance
    ml_dets <- rms_alerts %>% 
        group_by(SignalID, ApproachDesc) %>% 
        distinct(Detector) %>% 
        ungroup() %>% 
        nest(data = c(Detector))
    ml_dets$detector <- lapply(ml_dets$data, function(x) {
        paste(as.character(x$Detector), collapse = "/")}) %>% unlist()
    rms_alerts <- left_join(
        rms_alerts, select(ml_dets, -data), by = c("SignalID", "ApproachDesc")
        ) %>%
        mutate(Detector = factor(detector)) %>%
        select(-detector) %>%
        distinct()
    
    bind_rows(alerts, rms_alerts) %>%
    
    group_by(
        Zone_Group, Zone, SignalID, CallPhase, Detector, Alert
    ) %>% 
        mutate(
            start_streak = ifelse(
                as.integer(Date - lag(Date), unit = "days") > 1 | 
                    Date == min(Date), 
                Date, 
                NA)) %>% 
        fill(start_streak) %>% 
        mutate(streak = streak_run(start_streak, k = 90)) %>%
        ungroup() %>%
        select(-start_streak)
}


alerts <- get_alerts()

s3saveRDS(
    alerts, 
    bucket = "gdot-spm", 
    object = "mark/watchdog/alerts.rds",
    opts = list(multipart = TRUE))
s3write_using(
    alerts, 
    qsave, 
    bucket = "gdot-spm", 
    object = "mark/watchdog/alerts.qs",
    opts = list(multipart = TRUE))

