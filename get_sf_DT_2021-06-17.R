
# Run top of Monthly_Report_Calcs.R

first_seconds_of_red <- 5
date_ <- "2021-06-09"
signals_list <- 1455

get_sf_utah <- function(date_, conf, signals_list = NULL, first_seconds_of_red = 5, parallel = FALSE) {

    # Helper function. Get detector occupancy over interval
    get_occ <- function(DE, grn_interval) {
        x <- foverlaps(DE[!is.na(DetOn),], grn_interval, type = "any")
        x <- x[, c("SignalID","Phase", "Detector", "CycleStart","PhaseStart","PhaseEnd", 
                   "IntervalStart","IntervalEnd", "DetOn","DetOff")]
        
        # Detector on/off interval
        x[, a := pmax(IntervalStart, DetOn)]
        x[, b := pmin(IntervalEnd, DetOff)]
        x[, det_int := as.numeric(b-a)]
        
        # Interval (grn/sor) interval
        x[, int_int := as.numeric(IntervalEnd-IntervalStart)]
        x <- x[!is.na(det_int),]
        
        y <- x[, .(va = sum(det_int)), by = .(SignalID, Phase, Detector, CycleStart)]
        z <- y[, .(va = max(va)), by = .(SignalID, Phase, CycleStart)]
        q <- x[, .(vb = max(int_int)), by = .(SignalID, Phase, CycleStart)]
        
        z[, occ := z$va/q$vb]
        z[, va := NULL]
        data.table(z)
    }
    
    
    # -- Detector config
    dc <- get_det_config_sf(date_)
    DC <- setDT(dc)
    DC <- DC[SignalID %in% signals_list, ]
    setnames(DC, c("CallPhase"), c("Phase"))
    setkeyv(DC, c("SignalID", "Phase", "Detector", "Date"))
    
    
    # -- Detection Events
    path <- glue("../detections/date={date_}/")
    if (dir.exists(path)) {
        print("local")
        de <- arrow::open_dataset(path) %>%
            filter(signal %in% signals_list) %>%
            collect() %>% 
            arrange(SignalID, Phase, CycleStart, PhaseStart) %>%
            mutate(Date = date_) %>%
            convert_to_utc()
    } else {
        print("S3")
        de %<-% (get_detection_events(date_, date_, conf$athena, signals_list) %>%
            filter(phase %in% c(3, 4, 7, 8)) %>%
            arrange(signalid, phase, cyclestart, phasestart) %>%
            collect()) # This takes several minutes
    }

    # -- Cycledata
    path <- glue("../cycles/date={date_}/")
    if (dir.exists(path)) {
        print("local")
        cd <- arrow::open_dataset(path) %>%
            filter(
                signal %in% signals_list,
                EventCode %in% c(1, 9)) %>%
            collect() %>% 
            arrange(SignalID, Phase, CycleStart, PhaseStart) %>%
            mutate(Date = date_) %>%
            convert_to_utc()
    } else {
        print("S3")
        cd %<-% (get_cycle_data(date_, date_, conf$athena, signals_list) %>%
                     filter(eventcode %in% c(1, 9)) %>%
                     arrange(signalid, phase, cyclestart, phasestart) %>%
                     collect()) # This takes several minutes
    }
    
    
    # Convert Detection Events to data.table and modify
    DE <- setDT(de)
    rm(de)
    setnames(
        DE, 
        names(DE), 
        c("SignalID", "Phase", "Detector", "CycleStart", "PhaseStart", "EventCode", 
          "DetTimeStamp", "DetDuration", "dettimeincycle", "dettimeinphase", "Date"))
    DE[, DetOn := DetTimeStamp]
    DE[, DetOff := DetTimeStamp + seconds(DetDuration)]
    DE[, Date := as_date(Date)]
    DE[, SignalID := factor(SignalID)]
    DE[, Phase := factor(Phase)]
    DE[, Detector := factor(Detector)]
    setkeyv(DE, c("SignalID", "Phase", "Detector", "Date"))
    
    
    # -- Join Detection Events with Detector Config
    DE <- DE[DC]
    DE <- DE[(!(is.na(DetOn) | is.na(DetOff))),]
    
    
    
    # Convert Cycle Data to data.table and modify
    CD <- setDT(cd)
    rm(cd)
    setnames(
        CD, 
        names(CD), 
        c("SignalID", "Phase", "CycleStart", "PhaseStart", "PhaseEnd","EventCode", 
          "TermType", "Duration", "volume", "Date"))
    CD[, Date := as_date(Date)]
    CD[, SignalID := factor(SignalID)]
    CD[, Phase := factor(Phase)]
    
    # Green Interval   
    grn_interval <- CD[EventCode == 1,]
    grn_interval[, IntervalStart := PhaseStart]
    grn_interval[, IntervalEnd := PhaseEnd]
    
    # Start of Red Interval
    sor_interval <- CD[EventCode == 9,]
    sor_interval[, IntervalStart := ymd_hms(PhaseStart)]
    sor_interval[, IntervalEnd := ymd_hms(IntervalStart) + seconds(first_seconds_of_red)]
    
    
    setkeyv(DE, c("SignalID", "Phase", "DetOn", "DetOff"))
    setkeyv(grn_interval, c("SignalID", "Phase", "IntervalStart", "IntervalEnd"))
    setkeyv(sor_interval, c("SignalID", "Phase", "IntervalStart", "IntervalEnd"))
    
    
    # Get Detector Occupancy over Green Interval
    grn <- get_occ(DE, grn_interval)
    setkeyv(grn, c("SignalID", "Phase", "CycleStart"))
    
    # Get Detector Occupancy over Start-of-Red Interval
    sor <- get_occ(DE, sor_interval)
    setkeyv(sor, c("SignalID", "Phase", "CycleStart"))
    

    # Merge Tables and apply Utah's split failure condition
    sf_df <- merge(grn, sor, suffixes = c("_grn", "_sor"))
    sf_df[, sf := 0]
    sf_df[is.na(gr_occ), gr_occ := 0]
    sf_df[is.na(sr_occ), sr_occ := 0]
    sf_df[(occ_grn>0.8) & (occ_sor>0.8), sf := 1]
    
    sf0 <- sf[, .(sf = max(sf), by = c("SignalID", "CycleStart")]
    sf0[, Phase := 0]
    
    data.table(sf_df)
}

