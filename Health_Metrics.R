# Corridor Health Metrics Functions

suppressMessages({
    library(shiny)
    library(dplyr)
    library(tidyr)
    library(DT)
    library(aws.s3)
    library(lubridate)
    library(purrr)
    library(stringr)
    library(formattable)
    library(data.table)
    library(DT)
    library(plotly)
    library(readr)
    library(yaml)
    library(qs)
})


############################################################################
### USER-DEFINED CONSTANTS AND LOOKUPS
############################################################################

health_conf <- read_yaml("health_metrics.yaml")

scoring_lookup <- health_conf$scoring_lookup %>%
    as.data.frame() %>%
    as.data.table()
weights_lookup <- health_conf$weights_lookup %>%
    as.data.frame() %>%
    as.data.table()


############################################################################
### USER-DEFINED FUNCTIONS
############################################################################

# function to commpile summary data from the sub/cor/sig datasets
get_summary_data <- function(df, current_month = NULL) {
    #' Converts sub or cor data set to a single data frame for the current_month
    #' for use in get_subcorridor_summary_table function
    #'
    #' @param df cor or sub data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor
    # current_month <- months[order(months)][match(current_month, months.formatted)]
    
    
    # Cleaner Version (mostly) for future use more broadly on the site.
    # Still have the issue of certain metrics not applying at certain levels,
    # e.g., tti in sig data frame has no meaning since it's a corridor, not a signal-level metric
    #
    # data <- list(
    #     select(df$mo[["du"]], Zone_Group, Corridor, Date, du.uptime = uptime, uptime.sb, uptime.pr),
    #     select(df$mo[["cu"]], Zone_Group, Corridor, Date, cu.uptime = uptime),
    #     select(df$mo[["pau"]], Zone_Group, Corridor, Date, pau.uptime = uptime),
    #     select(df$mo[["cctv"]], Zone_Group, Corridor, Date, cctv.uptime = uptime),
    #     select(df$mo[["ru"]], Zone_Group, Corridor, Date, ru.uptime = uptime),
    # 
    #     select(df$mo[["vpd"]], Zone_Group, Corridor, Date, vpd, Description),
    #     select(df$mo[["papd"]], Zone_Group, Corridor, Date, papd),
    #     select(df$mo[["pd"]], Zone_Group, Corridor, Date, pd),
    #     select(df$mo[["tp"]], Zone_Group, Corridor, Date, vph),
    #     select(df$mo[["aog"]], Zone_Group, Corridor, Date, aog),
    #     select(df$mo[["pr"]], Zone_Group, Corridor, Date, pr),
    #     select(df$mo[["qsd"]], Zone_Group, Corridor, Date, qs_freq),
    #     select(df$mo[["sfd"]], Zone_Group, Corridor, Date, sf_freq),
    #     select(df$mo[["sfo"]], Zone_Group, Corridor, Date, sf_freq),
    #     
    # 
    # ) %>%
    #     reduce(full_join, by = c("Zone_Group", "Corridor", "Date"))
    
    
    
    data <- list(
        rename(df$mo$du, du = uptime, du.delta = delta),
        rename(df$mo$pau, pau = uptime, pau.delta = delta),
        # rename(df$mo$pdc, pdc = Avg.Max.Ped.Delay, pdc.delta = delta),
        # rename(df$mo$pdf, pdf = Avg.Max.Ped.Delay, pdf.delta = delta),
        rename(df$mo$cctv, cctv = uptime, cctv.delta = delta),
        rename(df$mo$ru, rsu = uptime, rsu.delta = delta),
        rename(df$mo$cu, cu = uptime, cu.delta = delta),
        rename(df$mo$tp, tp.delta = delta),
        rename(df$mo$aogd, aog.delta = delta),
        rename(df$mo$prd, pr.delta = delta),
        rename(df$mo$qsd, qs.delta = delta),
        rename(df$mo$sfd, sf.delta = delta),
        rename(df$mo$pd, pd.delta = delta) %>% relocate(pd.delta, .after = pd),
        if (nrow(df$mo$tti) > 0) {
            rename(df$mo$tti, tti.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(select(df$mo$pd, 1:6), tti = pd, tti.delta = delta) %>%
                mutate(tti = NA, tti.delta = NA, Events = NA)
        },
        if (nrow(df$mo$pti) > 0) {
            rename(df$mo$pti, pti.delta = delta)
        } else { # if dealing with signals data - copy another df, rename, and set columns to NA
            rename(select(df$mo$pd, 1:6), pti = pd, pti.delta = delta) %>%
                mutate(pti = NA, pti.delta = NA)
        }
    ) %>%
        reduce(full_join, by = c("Zone_Group", "Corridor", "Month")) %>%
        filter(
            as.character(Zone_Group) != as.character(Corridor)
        ) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            -Duration,
            -Events,
            #-num,
            -starts_with("Description"),
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol"),
            -starts_with("num"),
        )
    if ("mttr" %in% names(df$mo)) { # cor, not sub
        data <- data %>%
            arrange(Month, Zone_Group, Corridor)
    } else {
        data <- data %>%
            rename(
                Subcorridor = Corridor,
                Corridor = Zone_Group
            ) %>%
            arrange(Month, Corridor, Subcorridor)
    }
    data <- data[, c(2, 1, 3:length(data))] # reorder Subcorridor, Corridor
    if (!is.null(current_month)) {
        data <- data %>% filter(Month == ymd(current_month))
    }
    return(data)
}

# function to compute scores/weights at sub/sig level
get_health_all <- function(df) {
    df <- df %>%
        select(
            Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            Context,
            
            # maintenance
            Detection_Uptime = du,
            Ped_Act_Uptime = pau,
            Comm_Uptime = cu,
            CCTV_Uptime = cctv,
            RSU_Uptime = rsu,
            Flash_Events,
            
            # operations
            Platoon_Ratio = pr,
            Ped_Delay = pd,
            Split_Failures = sf_freq,
            TTI = tti,
            pti # ,
            
            ## safety
            # KABCO_Severity,
            # Crash_Index,
            # Speeding_Percentage,
            # Speed
        ) %>%
        mutate( # scores for each metric
            # cleanup
            Flash_Events = ifelse(is.na(Flash_Events), 0, Flash_Events), # assuming we want this
            BI = pti - TTI,
            pti = NULL,
            
            # maintenance
            # backward: set roll to -Inf (higher value => higher score)
            Detection_Uptime_Score = get_lookup_value(
                scoring_lookup, "detection", "score", Detection_Uptime, "backward"),
            Ped_Act_Uptime_Score = get_lookup_value(
                scoring_lookup, "ped_actuation", "score", Ped_Act_Uptime, "backward"),
            Comm_Uptime_Score = get_lookup_value(
                scoring_lookup, "comm", "score", Comm_Uptime, "backward"),
            CCTV_Uptime_Score = get_lookup_value(
                scoring_lookup, "cctv", "score", CCTV_Uptime, "backward"),
            RSU_Uptime_Score = get_lookup_value(
                scoring_lookup, "rsu", "score", RSU_Uptime, "backward"),
            Flash_Events_Score = get_lookup_value(
                scoring_lookup, "flash_events", "score", Flash_Events),
            
            # operations
            Platoon_Ratio_Score = get_lookup_value(
                scoring_lookup, "pr", "score", Platoon_Ratio),
            Ped_Delay_Score = get_lookup_value(
                scoring_lookup, "ped_delay", "score", Ped_Delay),
            Split_Failures_Score = get_lookup_value
            (scoring_lookup, "sf", "score", Split_Failures),
            TTI_Score = get_lookup_value(
                scoring_lookup, "tti", "score", TTI),
            BI_Score = get_lookup_value(
                scoring_lookup, "bi", "score", BI) # ,
            
            ## safety
            # KABCO_Severity_Score = get_lookup_value(
            #     scoring_lookup, "kabco_severity", "score", KABCO_Severity),
            # Crash_Index_Score = get_lookup_value(
            #     scoring_lookup, "crash_index", "score", Crash_Index),
            # Speeding_Percentage_Score = get_lookup_value(
            #     scoring_lookup, "speeding_percentage", "score", Speeding_Percentage),
            # Speed_Score = get_lookup_value(
            #     scoring_lookup, "speed", "score", Speed)
        ) %>%
        inner_join(weights_lookup, by = c("Context"))
    
    # Maintenance
    df$Detection_Uptime_Weight[is.na(df$Detection_Uptime_Score)] <- NA
    df$Ped_Act_Uptime_Weight[is.na(df$Ped_Act_Uptime_Score)] <- NA
    df$Comm_Uptime_Weight[is.na(df$Comm_Uptime_Score)] <- NA
    df$CCTV_Uptime_Weight[is.na(df$CCTV_Uptime_Score)] <- NA
    df$RSU_Uptime_Weight[is.na(df$RSU_Uptime_Score)] <- NA
    df$Flash_Events_Weight[is.na(df$Flash_Events_Score)] <- NA

    # Operations
    df$Platoon_Ratio_Weight[is.na(df$Platoon_Ratio_Score)] <- NA
    df$Ped_Delay_Weight[is.na(df$Ped_Delay_Score)] <- NA
    df$Split_Failures_Weight[is.na(df$Split_Failures_Score)] <- NA
    df$TTI_Weight[is.na(df$TTI_Score)] <- NA
    df$BI_Weight[is.na(df$BI_Score)] <- NA

    # Safety
    # df$KABCO_Severity_Weight[is.na(df$KABCO_Severity_Score)] <- NA
    # df$Crash_Index_Weight[is.na(df$Crash_Index_Score)] <- NA
    # df$Speeding_Percentage_Weight[is.na(df$Speeding_Percentage_Score)] <- NA
    # df$Speed_Weight[is.na(df$Speed_Score)] <- NA
    
    df
}

# function to product maintenance % health data frame
get_health_maintenance <- function(df) {
    health <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Detection", "Ped_Act", "Comm", "CCTV", "RSU", "Flash_Events")))
    
    
    # filter out scores and weights and make sure they're both in the same sort order
    scores <- health %>% 
        select(ends_with("Score")) %>% 
        select(sort(tidyselect::peek_vars()))
    weights <- health %>% 
        select(ends_with("Weight")) %>% 
        select(sort(tidyselect::peek_vars()))

    health$Percent_Health <- rowSums(scores * weights, na.rm = T)/rowSums(weights, na.rm = T)/10
    health$Missing_Data <- 1 - rowSums(weights, na.rm = T)/100

    health <- health %>% 
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Percent_Health = mean(Percent_Health, na.rm = T),
            Missing_Data = mean(Missing_Data, na.rm = T)) %>%
        ungroup() %>%

                
        get_percent_health_subtotals() %>%
        select(
            `Zone Group` = Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            `Context` = Context_Category,
            `Percent Health` = Percent_Health,
            `Missing Data` = Missing_Data,
            `Detection Uptime Score` = Detection_Uptime_Score,
            `Ped Actuation Uptime Score` = Ped_Act_Uptime_Score,
            `Comm Uptime Score` = Comm_Uptime_Score,
            `CCTV Uptime Score` = CCTV_Uptime_Score,
            `RSU Uptime Score` = RSU_Uptime_Score,
            `Flash Events Score` = Flash_Events_Score,
            `Detection Uptime` = Detection_Uptime,
            `Ped Actuation Uptime` = Ped_Act_Uptime,
            `Comm Uptime` = Comm_Uptime,
            `CCTV Uptime` = CCTV_Uptime,
            `RSU Uptime` = RSU_Uptime,
            `Flash Events` = Flash_Events
        )
}

# function to product operations % health data frame
get_health_operations <- function(df) {
    health <- df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Platoon_Ratio", "Ped_Delay", "Split_Failures", "TTI", "BI")))
    
    
    # filter out scores and weights and make sure they're both in the same sort order
    scores <- health %>% 
        select(ends_with("Score")) %>% 
        select(sort(tidyselect::peek_vars()))
    weights <- health %>% 
        select(ends_with("Weight")) %>% 
        select(sort(tidyselect::peek_vars()))

    health$Percent_Health <- rowSums(scores * weights, na.rm = T)/rowSums(weights, na.rm = T)/10
    health$Missing_Data <- 1 - rowSums(weights, na.rm = T)/100

    health <- health %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Percent_Health = mean(Percent_Health, na.rm = T),
            Missing_Data = mean(Missing_Data, na.rm = T)) %>%
        ungroup() %>%

        
        get_percent_health_subtotals() %>%
        select(
            `Zone Group` = Zone_Group,
            Zone,
            Corridor,
            Subcorridor,
            Month,
            `Context` = Context_Category,
            `Percent Health` = Percent_Health,
            `Missing Data` = Missing_Data,
            `Platoon Ratio Score` = Platoon_Ratio_Score,
            `Ped Delay Score` = Ped_Delay_Score,
            `Split Failures Score` = Split_Failures_Score,
            `Travel Time Index Score` = TTI_Score,
            `Buffer Index Score` = BI_Score,
            `Platoon Ratio` = Platoon_Ratio,
            `Ped Delay` = Ped_Delay,
            `Split Failures` = Split_Failures,
            `Travel Time Index` = TTI,
            `Buffer Index` = BI
        )
}

# function used to look up health score for various metric
# data.table approach - update so last field is up/down
get_lookup_value <- function(dt, lookup_col, lookup_val, x, direction = "forward") {
    setkeyv(dt, lookup_col)
    cols <- c(lookup_val, lookup_col)
    dt <- dt[, ..cols]
    x <- data.table(x)
    # setkeyv(x, "x") #this re-orders x
    val <- dt[x, roll = ifelse(direction == "forward", Inf, -Inf)][, ..lookup_val]
    val %>%
        as_vector() %>%
        unname() # return as vector, not tibble, to avoid replacing column names in DF
}

# function that computes % health subtotals for corridor and zone level
get_percent_health_subtotals <- function(df) {
    corridor_subtotals <- df %>%
        group_by(Zone_Group, Zone, Corridor, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")
    zone_subtotals <- corridor_subtotals %>%
        group_by(Zone_Group, Zone, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")

    bind_rows(df, corridor_subtotals, zone_subtotals) %>%
        filter(!is.na(Subcorridor)) %>%
        mutate(
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone)
        ) %>%
        arrange(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Corridor = factor(coalesce(Corridor, Zone)),
            Subcorridor = factor(coalesce(Subcorridor, Corridor))
        )
}




if (FALSE) { # TRUE


    ############################################################################
    ### COMPILE RAW DATA FROM VARIOUS SOURCES
    ############################################################################
    cor <- s3read_using(qread, bucket = "gdot-spm", object = "cor_ec2.qs")
    sub <- s3read_using(qread, bucket = "gdot-spm", object = "sub_ec2.qs")
    sig <- s3read_using(qread, bucket = "gdot-spm", object = "sig_ec2.qs")

    # these are called within the Monthly_Report_s3 file
    # cor <- qs::qread("cor_ec2.qs")
    # sub <- qs::qread("sub_ec2.qs")
    # sig <- qs::qread("sig_ec2.qs")
    # corridors <- s3read_using(qread, bucket = "gdot-spm", object = "all_Corridors_Latest.qs")
}


if (TRUE) {
 
    # cmd <- get_summary_data(cor)
    csd <- get_summary_data(sub)
    ssd <- get_summary_data(sig)
    
    corridor_groupings <- s3read_using(
        read_excel, 
        bucket = conf$bucket, 
        object = conf$corridors_filename_s3, 
        sheet = "Contexts", 
        range = cell_cols("A:F")
        ) %>%
        mutate(Context = as.integer(Context))
    
    # # workaround to bring in flash events
    # flash_events <- read.csv("flash_events.csv")
    # # aggregate flash events by signal and month
    # flash_events <- flash_events %>%
    #     mutate(
    #         SignalID = as.factor(SignalID),
    #         TimeStamp = parse_date_time(TimeStamp, "YmdHMS"),
    #         Month = as.Date(format(TimeStamp, '%Y-%m-01'))
    #         ) %>%
    #     group_by(SignalID, Month) %>%
    #     summarise(Flash_Events = n())
    # # aggregate flash events by subcorridor and month
    # flash_events_sub <- flash_events %>%
    #     inner_join(corridors) %>%
    #     group_by(Subcorridor, Month) %>%
    #     summarise(Flash_Events = n())
    
    
    # ajt - input data frame for corridor health metrics
    # cor_health_data <- cmd %>%
    #     inner_join(corridor_groupings) %>%
    #     mutate(Flash_Events = NA)
    
    # input data frame for subcorridor health metrics
    sub_health_data <- csd %>%
        inner_join(corridor_groupings, by = c("Corridor", "Subcorridor")) %>%
        mutate(Flash_Events = NA)
    # %>% left_join(flash_events_sub)
    
    # input data frame for signal health metrics
    sig_health_data <- ssd %>%
        rename(
            SignalID = Corridor,
            Corridor = Zone_Group
        ) %>%
        left_join(select(corridors, Corridor, SignalID, Subcorridor), by = c("Corridor", "SignalID")) %>%
        inner_join(corridor_groupings, by = c("Corridor", "Subcorridor")) %>%
        mutate(Flash_Events = NA) %>%
        select(-Subcorridor) %>% # workaround - drop subcorridor column and replace with signalID
        rename(Subcorridor = SignalID)
    
    ############################################################################
    ### GENERATE COMPILED TABLES FOR WEBPAGE
    ############################################################################
    
    ## all data for all 3 health metrics - think this should be run in the background and cached?
    # compile all data needed for health metrics calcs - does not calculate % health yet
    #health_all_cor <- get_health_all(cor_health_data)
    health_all_sub <- get_health_all(sub_health_data) #%>% 
        # This is a hack due to missing Zone_Group for several Districts ("Zones")
        #mutate(Zone_Group = if_else(is.na(Zone_Group), Zone, as.character(Zone_Group))) %>% 
        #mutate(Zone_Group = as.factor(Zone_Group))
    health_all_sig <- get_health_all(sig_health_data)
    
    # factor levels for tables
    zone_group_levels <- levels(health_all_sub$Zone_Group)
    rtop <- zone_group_levels[grepl("RTOP", zone_group_levels)]
    districts <- zone_group_levels[grepl("District", zone_group_levels)]
    other <- zone_group_levels[!grepl("District", zone_group_levels) & !grepl("RTOP", zone_group_levels)]
    zone_group_levels <- factor(c(rtop, districts, other), levels = c(rtop, districts, other))
    
    # compile maintenance % health
    maintenance_sub <- get_health_maintenance(health_all_sub)
    maintenance_sig <- get_health_maintenance(health_all_sig)
    
    maintenance_cor <- maintenance_sub %>% 
        filter(as.character(Corridor) == as.character(Subcorridor)) %>% 
        select(-Subcorridor)
    
    # compile operations % health
    operations_sub <- get_health_operations(health_all_sub)
    operations_sig <- get_health_operations(health_all_sig)
    
    operations_cor <- operations_sub %>% 
        filter(as.character(Corridor) == as.character(Subcorridor)) %>% 
        select(-Subcorridor)
    
    ## compile safety % health
    # safety_sub <- get_health_safety(health_all_sub)
    # safety_sig <- get_health_safety(health_all_sig)
}

if (FALSE) {
    
    qsave(maintenance_cor, "maintenance_cor.qs")
    qsave(maintenance_sub, "maintenance_sub.qs")
    qsave(maintenance_sig, "maintenance_sig.qs")

    qsave(operations_cor, "operations_cor.qs")
    qsave(operations_sub, "operations_sub.qs")
    qsave(operations_sig, "operations_sig.qs")

    
    maintenance_cor <- qread("maintenance_cor.qs")
    maintenance_sub <- qread("maintenance_sub.qs")
    maintenance_sig <- qread("maintenance_sig.qs")

    operations_cor <- qread("operations_cor.qs")
    operations_sub <- qread("operations_sub.qs")
    operations_sig <- qread("operations_sig.qs")
}


get_health_metrics_plot_df <- function(df) {
    df %>% 
        select(
            Zone_Group = Corridor, 
            Corridor = Subcorridor, 
            #Description = Subcorridor, 
            everything()) %>% 
        select(-`Zone Group`, -Zone) %>% 
        mutate(
            Zone_Group = factor(Zone_Group), 
            Corridor = factor(Corridor)
            #Description = "Desc"
            ) %>%
        arrange(Zone_Group, Corridor, Month)
}

get_cor_health_metrics_plot_df <- function(df) {
    df %>% 
        select(-`Zone Group`) %>% 
        rename(Zone_Group = Zone) %>% 
        mutate(
            Zone_Group = factor(Zone_Group), 
            Corridor = factor(Corridor) 
            #Description = "Desc"
        ) %>%
        arrange(Zone_Group, Corridor, Month)
}

cor$mo$maint_plot <- get_cor_health_metrics_plot_df(maintenance_cor)
sub$mo$maint_plot <- get_health_metrics_plot_df(maintenance_sub)
sig$mo$maint_plot <- get_health_metrics_plot_df(maintenance_sig)

cor$mo$ops_plot <- get_cor_health_metrics_plot_df(operations_cor)
sub$mo$ops_plot <- get_health_metrics_plot_df(operations_sub)
sig$mo$ops_plot <- get_health_metrics_plot_df(operations_sig)

cor$mo$maint <- maintenance_cor
sub$mo$maint <- maintenance_sub
sig$mo$maint <- maintenance_sig

cor$mo$ops <- operations_cor
sub$mo$ops <- operations_sub
sig$mo$ops <- operations_sig


add_subcorridor <- function(df)  {
    df %>% 
        rename(SignalID = Subcorridor) %>% 
        left_join(
            select(corridors, Zone, Corridor, Subcorridor, SignalID), 
            by = c("Zone", "Corridor", "SignalID")) %>% 
        relocate(Subcorridor, .after = Corridor) %>% 
        filter(!is.na(Subcorridor))
}

sig$mo$maint
sig$mo$maint <- add_subcorridor(sig$mo$maint)
sig$mo$ops <- add_subcorridor(sig$mo$ops)

