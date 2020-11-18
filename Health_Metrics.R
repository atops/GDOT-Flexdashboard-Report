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

# function to compute scores/weights at sub/sig level once it's been cleaned up a bit - does not calculate % health
get_health_all <- function(df) {
    df %>%
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
            Detection_Uptime_Score = get_lookup_value(scoring_lookup, "detection", "score", Detection_Uptime, "backward"), # set roll to -Inf (higher value => higher score)
            Ped_Act_Uptime_Score = get_lookup_value(scoring_lookup, "ped_actuation", "score", Ped_Act_Uptime, "backward"), # set roll to -Inf (higher value => higher score)
            Comm_Uptime_Score = get_lookup_value(scoring_lookup, "comm", "score", Comm_Uptime, "backward"), # set roll to -Inf (higher value => higher score)
            CCTV_Uptime_Score = get_lookup_value(scoring_lookup, "cctv", "score", CCTV_Uptime, "backward"), # set roll to -Inf (higher value => higher score)
            RSU_Uptime_Score = get_lookup_value(scoring_lookup, "rsu", "score", RSU_Uptime, "backward"), # set roll to -Inf (higher value => higher score)
            Flash_Events_Score = get_lookup_value(scoring_lookup, "flash_events", "score", Flash_Events),
            # operations
            Platoon_Ratio_Score = get_lookup_value(scoring_lookup, "pr", "score", Platoon_Ratio),
            Ped_Delay_Score = get_lookup_value(scoring_lookup, "ped_delay", "score", Ped_Delay),
            Split_Failures_Score = get_lookup_value(scoring_lookup, "sf", "score", Split_Failures),
            TTI_Score = get_lookup_value(scoring_lookup, "tti", "score", TTI),
            BI_Score = get_lookup_value(scoring_lookup, "bi", "score", BI) # ,
            ## safety
            # KABCO_Severity_Score = get_lookup_value(scoring_lookup, "kabco_severity", "score", KABCO_Severity),
            # Crash_Index_Score = get_lookup_value(scoring_lookup, "crash_index", "score", Crash_Index),
            # Speeding_Percentage_Score = get_lookup_value(scoring_lookup, "speeding_percentage", "score", Speeding_Percentage),
            # Speed_Score = get_lookup_value(scoring_lookup, "speed", "score", Speed)
        ) %>%
        inner_join(weights_lookup) %>%
        mutate( # weights for each metric - set to NA if there's no score (no data)
            # maintenance
            Detection_Uptime_Weight = ifelse(!is.na(Detection_Uptime_Score), Detection_Uptime_Weight, NA),
            Ped_Act_Uptime_Weight = ifelse(!is.na(Ped_Act_Uptime_Score), Ped_Act_Uptime_Weight, NA),
            Comm_Uptime_Weight = ifelse(!is.na(Comm_Uptime_Score), Comm_Uptime_Weight, NA),
            CCTV_Uptime_Weight = ifelse(!is.na(CCTV_Uptime_Score), CCTV_Uptime_Weight, NA),
            RSU_Uptime_Weight = ifelse(!is.na(RSU_Uptime_Score), RSU_Uptime_Weight, NA),
            Flash_Events_Weight = ifelse(!is.na(Flash_Events_Score), Flash_Events_Weight, NA),
            # operations
            Platoon_Ratio_Weight = ifelse(!is.na(Platoon_Ratio_Score), Platoon_Ratio_Weight, NA),
            Ped_Delay_Weight = ifelse(!is.na(Ped_Delay_Score), Ped_Delay_Weight, NA),
            Split_Failures_Weight = ifelse(!is.na(Split_Failures_Score), Split_Failures_Weight, NA),
            TTI_Weight = ifelse(!is.na(TTI_Score), TTI_Weight, NA),
            BI_Weight = ifelse(!is.na(BI_Score), BI_Weight, NA) # ,
            ## safety
            # KABCO_Severity_Weight = ifelse(!is.na(KABCO_Severity_Score), KABCO_Severity_Weight, NA),
            # Crash_Index_Weight = ifelse(!is.na(Crash_Index_Score), Crash_Index_Weight, NA),
            # Speeding_Percentage_Weight = ifelse(!is.na(Speeding_Percentage_Score), Speeding_Percentage_Weight, NA),
            # Speed_Weight = ifelse(!is.na(Speed_Score), Speed_Weight, NA)
        )
}

# function to product maintenance % health data frame
get_health_maintenance <- function(df) {
    df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Detection", "Ped_Act", "Comm", "CCTV", "RSU", "Flash_Events"))
        ) %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            # percent health - possibly make calculation more efficient/clean? Using "ends_with()"?
            Percent_Health = sum(
                c(Detection_Uptime_Score, Ped_Act_Uptime_Score, Comm_Uptime_Score, CCTV_Uptime_Score, RSU_Uptime_Score, Flash_Events_Score) *
                    c(Detection_Uptime_Weight, Ped_Act_Uptime_Weight, Comm_Uptime_Weight, CCTV_Uptime_Weight, RSU_Uptime_Weight, Flash_Events_Weight),
                na.rm = TRUE
            ) /
                (10 * sum(
                    c(Detection_Uptime_Weight, Ped_Act_Uptime_Weight, Comm_Uptime_Weight, CCTV_Uptime_Weight, RSU_Uptime_Weight, Flash_Events_Weight),
                    na.rm = TRUE
                )),
            # missing data - possibly make calculation more efficient/clean? Using "ends_with()"?
            Missing_Data = 100 - sum(
                c(Detection_Uptime_Weight, Ped_Act_Uptime_Weight, Comm_Uptime_Weight, CCTV_Uptime_Weight, RSU_Uptime_Weight, Flash_Events_Weight),
                na.rm = TRUE
            )
        ) %>%
        group_by(Zone) %>%
        tidyr::fill(Zone_Group) %>%
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
    df %>%
        select(
            Zone_Group, Zone, Corridor, Subcorridor, Month, Context, Context_Category,
            starts_with(c("Platoon_Ratio", "Ped_Delay", "Split_Failures", "TTI", "BI"))
        ) %>%
        group_by(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            # percent health - possibly make calculation more efficient/clean? Using "ends_with()"?
            Percent_Health = sum(
                c(Platoon_Ratio_Score, Ped_Delay_Score, Split_Failures_Score, TTI_Score, BI_Score) *
                    c(Platoon_Ratio_Weight, Ped_Delay_Weight, Split_Failures_Weight, TTI_Weight, BI_Weight),
                na.rm = TRUE
            ) /
                (10 * sum(
                    c(Platoon_Ratio_Weight, Ped_Delay_Weight, Split_Failures_Weight, TTI_Weight, BI_Weight),
                    na.rm = TRUE
                )),
            # missing data - possibly make calculation more efficient/clean? Using "ends_with()"?
            Missing_Data = 100 - sum(
                c(Platoon_Ratio_Weight, Ped_Delay_Weight, Split_Failures_Weight, TTI_Weight, BI_Weight),
                na.rm = TRUE
            )
        ) %>%
        group_by(Zone) %>%
        tidyr::fill(Zone_Group) %>%
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
get_lookup_value <- function(dt, lookup_col, lookup_val, x, direction = "forward") { # data.table approach - update so last field is up/down
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
        group_by(Zone, Corridor, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")
    zone_subtotals <- corridor_subtotals %>%
        group_by(Zone, Month) %>%
        summarise(Percent_Health = mean(Percent_Health, na.rm = TRUE), .groups = "drop")

    df_all <- bind_rows(df, corridor_subtotals, zone_subtotals) %>%
        group_by(Zone, Month) %>%
        tidyr::fill(Zone_Group) %>%
        group_by(Corridor) %>%
        tidyr::fill(Context_Category) %>%
        ungroup() %>%
        mutate(
            Zone_Group = factor(Zone_Group, levels = zone_group_levels)
        ) %>%
        arrange(Zone_Group, Zone, Corridor, Subcorridor, Month) %>%
        mutate(
            Corridor = ifelse(!is.na(Zone) & is.na(Corridor), Zone, Corridor),
            Subcorridor = ifelse(!is.na(Zone) & !is.na(Corridor) & is.na(Subcorridor), Corridor, Subcorridor),
            Missing_Data = Missing_Data / 100
        )
    df_all
}

# function to filter health data based on user inputs
get_health_data_filtered <- function(data_, zone_group_, corridor_) {
    health_data <- data_
    
    health_data <- filter_mr_data(mutate(data_, Zone_Group = Zone), zone_group_)
    
    # filter by corridor - do we want this in the barplots?
    if (corridor_ != "All Corridors") {
        health_data <- filter(health_data, Corridor == corridor_)
    }
    

    # # filter by zone group
    # if (startsWith(zone_group_, "Zone") | startsWith(zone_group_, "District")) {
    #     health_data <- filter(health_data, Zone == zone_group_)
    # 
    #     # what if zone group is RTOP1 / RTOP 2 / Cobb County / Ramp Meters?
    # 
    #     # filter by corridor - do we want this in the barplots?
    #     if (corridor_ != "All Corridors") {
    #         health_data <- filter(health_data, Corridor == corridor_)
    #     }
    # }
    # 
    # return(health_data)
    health_data
}


# separate functions for maintenance/ops/safety datatables since formatting is different - would be nice to abstractify
get_monthly_maintenance_health_table <- function(data_, month_, zone_group_, corridor_) {
    single_month_table <- get_health_data_filtered(data_, zone_group_, corridor_) %>%
        filter(Month == month_) %>%
        ungroup() %>%
        select(-Month) %>%
        mutate(
            Subcorridor = ifelse(Subcorridor == Corridor, "ALL", Subcorridor),
            Corridor = ifelse(Corridor == Zone, "ALL", Corridor)
        )

    fn <- tempfile(pattern = paste(month_, zone_group_, corridor_, "_"), tmpdir = ".", fileext = ".qs")
    qsave(single_month_table, fn)

    datatable(single_month_table,
        filter = "top",
        rownames = FALSE,
        extensions = "Scroller",
        options = list(
            scrollY = 500,
            scrollX = TRUE,
            pageLength = 1000,
            columnDefs = list(
                list(
                    className = "dt-left",
                    targets = c(0, 1, 2, 3, 4)
                )
            ),
            dom = "t",
            selection = "none"
        )
    ) %>%
        formatPercentage(c(6:7, 14:18)) %>%
        formatRound(8:13, 19, digits = 0) %>%
        formatStyle("Subcorridor",
            target = "row",
            backgroundColor = styleEqual("ALL", "lightgray"),
            fontStyle = styleEqual("ALL", "italic")
        ) %>%
        formatStyle("Corridor",
            target = "row",
            backgroundColor = styleEqual("ALL", "gray"),
            fontWeight = styleEqual("ALL", "bold")
        ) %>%
        formatStyle("Missing Data",
            color = styleInterval(c(0.1, 0.3, 0.5), c("black", "gold", "orangered", "crimson")),
            borderRight = "2px solid #ddd"
        ) %>%
        formatStyle("Flash Events Score", borderRight = "2px solid #ddd") %>%
        formatStyle("Flash Events", borderRight = "2px solid #ddd")
}

# separate functions for maintenance/ops/safety datatables since formatting is different - would be nice to abstractify
get_monthly_operations_health_table <- function(data_, month_, zone_group_, corridor_) {
    single_month_table <- get_health_data_filtered(data_, zone_group_, corridor_) %>%
        filter(Month == month_) %>%
        ungroup() %>%
        select(-Month) %>%
        mutate(
            Subcorridor = ifelse(Subcorridor == Corridor, "ALL", Subcorridor),
            Corridor = ifelse(Corridor == Zone, "ALL", Corridor)
        )

    datatable(single_month_table,
        filter = "top",
        rownames = FALSE,
        extensions = "Scroller",
        options = list(
            scrollY = 500,
            scrollX = TRUE,
            pageLength = 1000,
            columnDefs = list(
                list(
                    className = "dt-left",
                    targets = c(0, 1, 2, 3, 4)
                )
            ),
            dom = "t",
            selection = "none"
        )
    ) %>%
        formatPercentage(c(6:7, 15)) %>%
        formatRound(8:12, digits = 0) %>%
        formatRound(14, digits = 1) %>%
        formatRound(c(13, 16, 17), digits = 2) %>%
        formatStyle("Subcorridor",
            target = "row",
            backgroundColor = styleEqual("ALL", "lightgray"),
            fontStyle = styleEqual("ALL", "italic")
        ) %>%
        formatStyle("Corridor",
            target = "row",
            backgroundColor = styleEqual("ALL", "gray"),
            fontWeight = styleEqual("ALL", "bold")
        ) %>%
        formatStyle("Missing Data",
            color = styleInterval(c(0.1, 0.3, 0.5), c("black", "gold", "orangered", "crimson")),
            borderRight = "2px solid #ddd"
        ) %>%
        formatStyle("Buffer Index Score", borderRight = "2px solid #ddd") %>%
        formatStyle("Buffer Index", borderRight = "2px solid #ddd")
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
 
    # cmd <- get_summary_data(cor) %>% distinct() # temporary hack
    csd <- get_summary_data(sub) %>% distinct() # temporary hack
    ssd <- get_summary_data(sig) %>% distinct() # temporary hack
    
    # workaround to bring in mapping for subcorridors to context/zones 
    # INCLUDES CONTEXT (1-5, which is NOT in corridors.qs)
    #corridor_mapping <- read_csv("zone-corridors-subcorridors-context_mapping.csv")
    corridor_mapping <- s3read_using(
        read_csv, 
        bucket = conf$bucket, 
        object = "Health_Context.csv",
        col_types = cols(
            Zone = col_character(),
            Corridor = col_character(),
            Subcorridor = col_character(),
            Context = col_integer()
        )
    )
    
    # AJT: This seems like a hack to fill in some context info where we have none.
    corridor_groupings <- corridors %>%
        distinct(Zone_Group, Zone, Corridor, Subcorridor) %>%
        full_join(corridor_mapping, by = c("Zone", "Corridor", "Subcorridor")) %>%
        # workaround to fill in Context
        group_by(Corridor) %>%
        tidyr::fill(Context, .direction = "downup") %>%
        group_by(Zone) %>%
        tidyr::fill(Context, .direction = "downup") %>%
        group_by(Zone_Group) %>%
        tidyr::fill(Context, .direction = "downup") %>%
        ungroup()
    
    
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
        inner_join(corridor_groupings) %>%
        mutate(Flash_Events = NA)
    # %>% left_join(flash_events_sub)
    
    # input data frame for signal health metrics
    sig_health_data <- ssd %>%
        rename(
            SignalID = Corridor,
            Corridor = Zone_Group
        ) %>%
        mutate(Subcorridor = NA) %>%
        inner_join(corridor_groupings) %>%
        mutate(Flash_Events = NA) %>%
        # left_join(flash_events) %>%
        select(-Subcorridor) %>% # workaround - drop subcorridor column and replace with signalID
        rename(Subcorridor = SignalID)
    
    ############################################################################
    ### GENERATE COMPILED TABLES FOR WEBPAGE
    ############################################################################
    
    ## all data for all 3 health metrics - think this should be run in the background and cached?
    # compile all data needed for health metrics calcs - does not calculate % health yet
    #health_all_cor <- get_health_all(cor_health_data)
    health_all_sub <- get_health_all(sub_health_data) %>% 
        # This is a hack due to missing Zone_Group for several Districts ("Zones")
        mutate(Zone_Group = if_else(is.na(Zone_Group), Zone, as.character(Zone_Group))) %>% mutate(Zone_Group = as.factor(Zone_Group))
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
    
    maintenance_cor <- maintenance_sub %>% filter(Corridor == Subcorridor) %>% select(-Subcorridor)
    
    # compile operations % health
    operations_sub <- get_health_operations(health_all_sub)
    operations_sig <- get_health_operations(health_all_sig)
    
    operations_cor <- operations_sub %>% filter(Corridor == Subcorridor) %>% select(-Subcorridor)
    
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
            Description = Subcorridor, 
            everything()) %>% 
        select(-`Zone Group`, -Zone) %>% 
        mutate(
            Zone_Group = factor(Zone_Group), 
            Corridor = factor(Corridor), 
            Description = "Desc") %>%
        arrange(Zone_Group, Corridor, Month)
}

get_cor_health_metrics_plot_df <- function(df) {
    df %>% 
        select(-`Zone Group`) %>% 
        rename(Zone_Group = Zone) %>% 
        mutate(
            Zone_Group = factor(Zone_Group), 
            Corridor = factor(Corridor), 
            Description = "Desc") %>%
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
