
# Monthly_Report_UI_Functions

library(flexdashboard)
library(shiny)

library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(feather)
library(forcats)
library(plotly)
library(crosstalk)
library(memoise)

library(DT)

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

DARK_GRAY = "#636363"
BLACK = "#000000"
DARK_GRAY_BAR = "#252525"
LIGHT_GRAY_BAR = "#bdbdbd"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"; GDOT_BLUE_RGB = "rgba(37, 97, 148, 0.80)"
GDOT_YELLOW = "#EEB211"; GDOT_YELLOW_RGB = "rgba(238, 178, 17, 0.80)"

colrs <- c("1" = LIGHT_BLUE, "2" = BLUE, "3" = LIGHT_GREEN, "4" = GREEN, 
           "5" = LIGHT_RED, "6" = RED, "7" = LIGHT_ORANGE, "8" = ORANGE, 
           "9" = LIGHT_PURPLE, "10" = PURPLE, "11" = LIGHT_BROWN, "12" = BROWN,
           "0" = DARK_GRAY)

# ###########################################################

corridors <- read_feather("corridors.feather")
teams_tables <- readRDS("teams_tables.rds")

cor <- readRDS("cor.rds")
sig <- readRDS("sig.rds")


as_int <- function(x) {scales::comma_format()(as.integer(x))}
as_2dec <- function(x) {sprintf(x, fmt = "%.2f")}
as_pct <- function(x) {sprintf(x * 100, fmt = "%.1f%%")}


#raw_counts_1hr_ <- read_feather("sig_raw_counts_1hr.feather")
#filtered_counts_1hr_ <- read_feather("sig_filtered_counts_1hr.feather")
#adjusted_counts_1hr_ <- read_feather("sig_adjusted_counts_1hr.feather")
#vpd_ <- read_feather("sig_vpd.feather")
#aog_ <- read_feather("sig_aog.feather")
#sf_ <- read_feather("sig_sf.feather")
#qs_ <- read_feather("sig_qs.feather")
#avg_daily_detector_uptime_ <- read_feather("sig_avg_daily_detector_uptime.feather")
#daily_comm_uptime_ <- read_feather("sig_daily_comm_uptime.feather")



get_valuebox_ <- function(cor_monthly_df, var_, var_fmt, break_ = FALSE, 
                          zone, mo, qu = NULL) {
    vals <- cor_monthly_df %>% 
        replace_na(list(delta = 0))
    
    if (is.null(qu)) { # want monthly, not quarterly data
        vals <- vals %>%
            dplyr::filter(Corridor == zone & Month == mo) %>% as.list()
    } else {
        vals <- vals %>%
            dplyr::filter(Corridor == zone & Quarter == qu) %>% as.list()
    }
    val <- var_fmt(vals[[var_]])
    del <- paste0(ifelse(vals$delta > 0, " (+", " ( "), as_pct(vals$delta), ")")
    
    validate(need(val, message = "NA"))
    
    if (break_) {
        tags$div(HTML(paste(
            val,
            tags$p(del, style = "font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;")
        )))
    } else {
        tags$div(HTML(paste(
            val, tags$span(del, style = "font-size: 70%;")
        )))
    }
}
get_valuebox <- memoise(get_valuebox_)



perf_plot <- function(data_, value_, name_, color_, 
                      format_func = function(x) {x},
                      hoverformat_ = ",.0f") {
    
    ax <- list(title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, hoverformat = hoverformat_)
    
    value_ <- as.name(value_)
    data_ <- dplyr::rename(data_, value = !!value_)
    
    first <- data_[which.min(data_$Month), ]
    last <- data_[which.max(data_$Month), ]
    
    plot_ly(type = "scatter", mode = "markers") %>% 
        add_trace(data = data_, 
                  x = ~Month, y = ~value, 
                  name = name_,
                  line = list(color = color_), 
                  mode = 'lines+markers',
                  marker = list(size = 8,
                                line = list(width = 1,
                                            color = 'rgba(255, 255, 255, 255.8)'))) %>%
        add_annotations(x = first$Month,
                        y = first$value,
                        text = format_func(first$value),
                        showarrow = FALSE,
                        xanchor = "right",
                        xshift = -10) %>%
        add_annotations(x = last$Month,
                        y = last$value,
                        text = format_func(last$value),
                        font = list(size = 16),
                        showarrow = FALSE,
                        xanchor = "left",
                        xshift = 20) %>%
        layout(xaxis = ax, 
               yaxis = ay,
               annotations = list(x = -.02,
                                  y = 0.4,
                                  xref = "paper",
                                  yref = "paper",
                                  xanchor = "right",
                                  text = name_,
                                  font = list(size = 12),
                                  showarrow = FALSE),
               showlegend = FALSE,
               margin = list(l = 120,
                             r = 40)) %>% 
        config(displayModeBar = F)
}

no_data_plot_ <- function(name_) {
    
    ax <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    
    plot_ly(type = "scatter", mode = "markers") %>%
        layout(xaxis = ax, 
               yaxis = ay,
               annotations = list(list(x = -.02,
                                       y = 0.4,
                                       xref = "paper",
                                       yref = "paper",
                                       xanchor = "right",
                                       text = name_,
                                       font = list(size = 12),
                                       showarrow = FALSE),
                                  list(x = 0.5,
                                       y = 0.5,
                                       xref = "paper",
                                       yref = "paper",
                                       text = "NO DATA",
                                       font = list(size = 16),
                                       showarrow = FALSE)),
               
               margin = list(l = 180,
                             r = 100)) %>% 
        config(displayModeBar = F)
    
}
no_data_plot <- memoise(no_data_plot_)

# Empty plot - space filler
x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
p0 <- plot_ly(type = "scatter", mode = "markers") %>% layout(xaxis = x0, yaxis = x0)

get_bar_line_dashboard_plot_ <- function(cor_weekly, 
                                         cor_monthly, 
                                         cor_hourly = NULL, 
                                         var_,
                                         num_format, # percent, integer, decimal
                                         highlight_color,
                                         month_, 
                                         zone_group_, 
                                         x_bar_title = "___",
                                         x_line1_title = "___",
                                         x_line2_title = "___",
                                         plot_title = "___ ") {
    
    var_ <- as.name(var_)
    if (num_format == "percent") {
        var_fmt <- as_pct
        tickformat_ <- ".0%"
    } else if (num_format == "integer") {
        var_fmt <- as_int
        tickformat_ <- ",.0"
    } else if (num_format == "decimal") {
        var_fmt <- as_2dec
        tickformat_ <- ".2f"
    }
    
    highlight_color_ <- highlight_color
    
    if (zone_group_ == "All RTOP") {
        mdf <- cor_monthly %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"), 
                   Month == month_)
        wdf <- cor_weekly %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"), 
                   Date < month_ + months(1))
    } else {
        mdf <- cor_monthly %>% 
            filter(Zone_Group == zone_group_, Month == month_)
        wdf <- cor_weekly %>% 
            filter(Zone_Group == zone_group_, Date < month_ + months(1))
    }
    if (nrow(mdf) > 0 & nrow(wdf) > 0) {
        # Current Month Data
        mdf <- mdf %>%
            arrange(!!var_) %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        sdm <- SharedData$new(mdf, ~Corridor, group = "grp")
        
        bar_chart <- plot_ly(sdm,
                             type = "bar",
                             x = ~var, 
                             y = ~Corridor,
                             marker = list(color = ~col),
                             text = ~var_fmt(var),
                             textposition = "auto",
                             insidetextfont = list(color = "black"),
                             hoverinfo = "none") %>%
            layout(
                barmode = "overlay",
                xaxis = list(title = x_bar_title, 
                             zeroline = FALSE, 
                             tickformat = tickformat_),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
        
        # Weekly Data - historical trend
        wdf <- wdf %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor == zone_group_, 0, 1))) %>%
            group_by(Corridor)
        
        sdw <- SharedData$new(wdf, ~Corridor, group = "grp")
        
        weekly_line_chart <- plot_ly(sdw) %>%
            add_lines(x = ~Date, 
                      y = ~var, 
                      color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                      alpha = 0.6) %>%
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(tickformat = tickformat_,
                                hoverformat = tickformat_),
                   title = "__plot1_title__",
                   showlegend = FALSE,
                   margin = list(t = 50)
            )
        
        if (!is.null(cor_hourly)) {
            
            if (zone_group_ == "All RTOP") {
                hdf <- cor_hourly %>%
                    filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                           !Corridor %in% c("RTOP1", "RTOP2"), 
                           date(Hour) == month_)
            } else {
                hdf <- cor_hourly %>%
                    filter(Zone_Group == zone_group_, date(Hour) == month_)
            }
            
            # Hourly Data - current month
            hdf <- hdf %>%
                mutate(var = !!var_,
                       col = factor(ifelse(Corridor == zone_group_, 0, 1))) %>%
                group_by(Corridor)
            
            sdh <- SharedData$new(hdf, ~Corridor, group = "grp")
            
            hourly_line_chart <- plot_ly(sdh) %>%
                add_lines(x = ~Hour,
                          y = ~var,
                          color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                          alpha = 0.6) %>%
                layout(xaxis = list(title = x_line2_title),
                       yaxis = list(tickformat = tickformat_),
                       title = "__plot2_title__",
                       showlegend = FALSE)
            
            ax0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
            p0 <- plot_ly() %>% layout(xaxis = ax0, yaxis = ax0)
            
            s1 <- subplot(weekly_line_chart, p0, hourly_line_chart, 
                          titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        } else {
            s1 <- weekly_line_chart
        }
        
        subplot(bar_chart, s1, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100),
                   title = plot_title) %>%
            highlight(color = highlight_color_, opacityDim = 0.9, defaultValues = c(zone_group_),
                      selected = attrs_selected(insidetextfont = list(color = "white"), 
                                                textposition = "auto"))
    } else(
        no_data_plot("")
    )
}
get_bar_line_dashboard_plot <- memoise(get_bar_line_dashboard_plot_)

get_tt_plot_ <- function(cor_monthly_tti, cor_monthly_tti_by_hr, 
                         cor_monthly_pti, cor_monthly_pti_by_hr, 
                         highlight_color = RED2,
                         month_,
                         zone_group_,
                         x_bar_title = "___",
                         x_line1_title = "___",
                         x_line2_title = "___",
                         plot_title = "___ ") {
    
    var_fmt <- as_2dec
    tickformat <- ".2f"
    
    mott <- full_join(cor_monthly_tti, cor_monthly_pti,
                      by = c("Corridor", "Zone_Group", "Month"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>% #,
        mutate(bti = pti - tti) %>%
        select(Corridor, Zone_Group, Month, tti, pti, bti)
    
    hrtt <- full_join(cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
                      by = c("Corridor", "Zone_Group", "Hour"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%#,
        mutate(bti = pti - tti) %>%
        select(Corridor, Zone_Group, Hour, tti, pti, bti)
    
    if (zone_group_ == "All RTOP") {
        mott <- mott %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"))
        hrtt <- hrtt %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"))
    } else {
        mott <- mott %>% 
            filter(Zone_Group == zone_group_)
        hrtt <- hrtt %>% 
            filter(Zone_Group == zone_group_) 
    }
    
    if (nrow(mott) > 0 & nrow(hrtt) > 0) {    
        sdb <- SharedData$new(dplyr::filter(mott, Month==month_), 
                              ~Corridor, group = "grp")
        sdm <- SharedData$new(mott, 
                              ~Corridor, group = "grp")
        sdh <- SharedData$new(dplyr::filter(hrtt, date(Hour)==month_), 
                              ~Corridor, group = "grp")
        
        highlight_color_ <- RED2 # Colorbrewer red
        
        base_b <- plot_ly(sdb, color = I("gray")) %>%
            group_by(Corridor)
        base_m <- plot_ly(sdm, color = I("gray")) %>%
            group_by(Corridor)
        base_h <- plot_ly(sdh, color = I("gray")) %>%
            group_by(Corridor)
        
        pbar <- base_b %>%
            
            arrange(tti) %>%
            add_bars(x = ~tti, 
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(tti),
                     color = I("gray"),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     hoverinfo = "none") %>%
            add_bars(x = ~bti,
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(pti),
                     color = I(LIGHT_BLUE),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "stack",
                xaxis = list(title = x_bar_title, 
                             zeroline = FALSE, 
                             tickformat = tickformat,
                             range = c(0, 2)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )
        pttimo <- base_m %>%
            add_lines(x = ~Month, 
                      y = ~tti, 
                      alpha = 0.6) %>%
            layout(xaxis = list(title = "Travel Time Index (TTI"),
                   yaxis = list(tickformat = tickformat,
                                range = c(1, 2.5)),
                   showlegend = FALSE)
        
        pttihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~tti,
                      alpha = 0.6) %>%
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(range = c(1, 3),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        pptimo <- base_m %>%
            add_lines(x = ~Month, 
                      y = ~pti, 
                      alpha = 0.6) %>%
            layout(xaxis = list(title = "Planning Time Index (PTI)"),
                   yaxis = list(tickformat = tickformat,
                                range = c(1, 2.5)),
                   showlegend = FALSE)
        
        pptihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~pti,
                      alpha = 0.6) %>%
            layout(xaxis = list(title = x_line2_title),
                   yaxis = list(range = c(1, 3),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
        p0 <- plot_ly(type = "scatter", mode = "markers") %>% 
            layout(xaxis = x0, 
                   yaxis = x0)
        
        stti <- subplot(pttimo, p0, pttihr, 
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        
        sbti <- subplot(pptimo, p0, pptihr, 
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        
        subplot(pbar, stti, sbti, titleX = TRUE, widths = c(0.2, 0.4, 0.4), margin = 0.03) %>%
            layout(margin = list(l = 120, r = 80),
                   title = "Travel Time and Planning Time Index") %>%
            highlight(color = highlight_color_, opacityDim = 0.9, 
                      defaultValues = c(zone_group_),
                      selected = attrs_selected(insidetextfont = list(color = "white"), 
                                                textposition = "auto", base = 0))
        
    } else {
        no_data_plot("")
    }
}
get_tt_plot <- memoise(get_tt_plot_)

get_pct_ch_plot_ <- function(cor_monthly_vpd,
                             month_,
                             zone_group_) {
    pl <- function(df) { 
        neg <- dplyr::filter(df, delta < 0)
        pos <- dplyr::filter(df, delta > 0)
        
        title_ <- df$Corridor[1]
        
        plot_ly() %>% 
            add_bars(data = neg,
                     x = ~Month, 
                     y = ~delta, 
                     marker = list(color = "#e31a1c")) %>%
            add_bars(data = pos,
                     x = ~Month, 
                     y = ~delta, 
                     marker = list(color = "#33a02c")) %>%
            layout(xaxis = list(title = "",
                                nticks = nrow(df)),
                   yaxis = list(title = "",
                                tickformat = "%"),
                   showlegend = FALSE,
                   annotations = list(text = title_,
                                      font = list(size = 12),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "center",
                                      align = "center",
                                      x = 0.5,
                                      y = 0.95,
                                      showarrow = FALSE))}
    
    
    if (zone_group_ == "All RTOP") {
        df <- cor_monthly_vpd %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"), 
                   Month <= month_)
    } else {
        df <- cor_monthly_vpd %>% 
            filter(Zone_Group == zone_group_ & Month <= month_)
    }
    if (nrow(df) > 0) {
        
        pcts <- split(df, df$Corridor)
        plts <- lapply(pcts[lapply(pcts, nrow)>0], pl)
        subplot(plts,
                margin = 0.03, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE) %>%
            layout(title = "Percent Change from Previous Month (vehicles/day)",
                   margin = list(t = 60))
    } else {
        no_data_plot("")
    }
}
get_pct_ch_plot <- memoise(get_pct_ch_plot_)

get_vph_peak_plot_ <- function(df, chart_title, bar_subtitle, 
                               month_ = current_month, zone_group_ = zone_group()) {
    
    if (zone_group_ == "All RTOP") {
        df <- df %>%
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"))
    } else {
        df <- df %>%
            filter(Zone_Group == zone_group_)
    }
    if (nrow(df) > 0) {
        
        sdw <- SharedData$new(dplyr::filter(df, Month <= month_), ~Corridor, group = "grp")
        sdm <- SharedData$new(dplyr::filter(df, Month == month_), ~Corridor, group = "grp")
        
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
                     textfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "overlay",
                xaxis = list(title = bar_subtitle, zeroline = FALSE),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )
        p2 <- base %>%
            add_lines(x = ~Month, y = ~vph, alpha = 0.6) %>%
            layout(xaxis = list(title = "Date"),
                   showlegend = FALSE,
                   annotations = list(text = chart_title,
                                      font = list(size = 12),
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
            highlight(color = "#256194", opacityDim = 0.9, defaultValues = c(zone_group_),
                      selected = attrs_selected(insidetextfont = list(color = "white"), textposition = "inside"))
    } else {
        no_data_plot("")
    }
}
get_vph_peak_plot <- memoise(get_vph_peak_plot_)

get_minmax_hourly_plot_ <- function(cor_monthly_vph, 
                                    month_ = current_month(), 
                                    zone_group_ = zone_group()) {
    
    mm_pl <- function(mm, tm) {
        
        title_ <- mm$Corridor[1]
        
        plot_ly() %>%
            add_bars(data = mm,
                     x = ~Hour,
                     y = ~min_vph,
                     opacity = 0) %>%
            add_bars(data = mm,
                     x = ~Hour,
                     y = ~(max_vph-min_vph),
                     marker = list(color = "#a6cee3")) %>%
            add_markers(data = tm,
                        x = ~Hour,
                        y = ~vph,
                        marker = list(color = "#ca0020")) %>%
            layout(barmode = "stack",
                   showlegend = FALSE,
                   xaxis = list(title = "",
                                tickformat = "%H:%M"),
                   yaxis = list(title = "",
                                range = c(0, round(max(minmax$max_vph), -3) + 1000)),
                   title = "Current Month (veh/hr) compared to range over previous months",
                   annotations = list(text = title_,
                                      font = list(size = 12),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "center",
                                      align = "center",
                                      x = 0.5,
                                      y = 0.85,
                                      showarrow = FALSE))
    }
    
    if (zone_group_ == "All RTOP") {
        df <- cor_monthly_vph %>% 
            filter(!Corridor %in% c("RTOP1", "RTOP2", "D3", "D4", "D5", "Zone 7") & date(Hour) <= month_)
    } else {
        df <- cor_monthly_vph %>% 
            filter(Zone_Group == zone_group_ & date(Hour) <= month_)
    }
    
    # Current Month Data
    this_month <- df %>% 
        filter(date(Hour) == month_) 
    
    if (nrow(df) > 0) {
        
        minmax <- df %>% 
            mutate(Hour = (Hour + (date(max(Hour)) - date(Hour)))) %>%
            group_by(Corridor, Hour) %>% 
            summarize(min_vph = min(vph), max_vph = max(vph))
        
        mms <- split(minmax, minmax$Corridor)
        tms <- split(this_month, this_month$Corridor)
        mms_ <- mms[lapply(mms, nrow)>0]
        tms_ <- tms[lapply(mms, nrow)>0]
        
        plts <- lapply(seq_along(mms_), function(i) mm_pl(mms_[[i]], tms_[[i]]))
        subplot(plts, 
                margin = 0.02, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE) %>%
            layout(title = "Current Month (veh/hr) compared to range over previous months",
                   yaxis = list(title = "vph"),
                   margin = list(t = 60))
    } else {
        no_data_plot("")
    }
}
get_minmax_hourly_plot <- memoise(get_minmax_hourly_plot_)


det_uptime_bar_plot_ <- function(df, xtitle, month_) {
    
    data = df %>%
        dplyr::filter(month(X)==month_) %>%
        group_by(Corridor) %>%
        summarize(Uptime = mean(Y)) %>%
        arrange(Uptime)
    
    
    plot_ly(data) %>%
        add_bars(x = ~Uptime,
                 y = ~factor(Corridor, levels = Corridor),
                 color = I(BLUE),
                 text = ~scales::percent(Uptime),
                 textposition = "inside",
                 textfont = list(color = "white"),
                 hoverinfo = "y+x",
                 showlegend = FALSE) %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = xtitle,
                         zeroline = FALSE,
                         tickformat = "%"),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = list(size = 11),
            margin = list(pad = 4),
            paper_bgcolor = "#f0f0f0",
            plot_bgcolor = "#f0f0f0"
        )
}
det_uptime_bar_plot <- memoise(det_uptime_bar_plot_)

# Subplots
det_uptime_line_plot_ <- function(df, corr, showlegend_) {
    plot_ly()
    plot_ly(data = df) %>%
        add_lines(x = ~X,
                  y = ~Y,
                  color = ~C,
                  colors = cols,
                  legendgroup = ~C,
                  showlegend = showlegend_) %>%
        layout(yaxis = list(title = "",
                            range = c(0, 1.1),
                            tickformat = "%"),
               xaxis = list(title = ""),
               annotations = list(text = corr,
                                  font = list(size = 14),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "left",
                                  align = "center",
                                  x = 0.1,
                                  y = 0.1,
                                  showarrow = FALSE),
               plot_bgcolor = "#ffffff")
}
det_uptime_line_plot <- memoise(det_uptime_line_plot_)

get_cor_det_uptime_plot_ <- function(avg_daily_uptime, 
                                     month_,
                                     zone_group_,
                                     month_name) {
    
    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>% 
            add_lines(x = ~Date, 
                      y = ~uptime.pr,
                      color = I(LIGHT_BLUE),
                      name = "Presence",
                      legendgroup = "Presence",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date, 
                      y = ~uptime.sb,
                      color = I(BLUE),
                      name = "Setback",
                      legendgroup = "Setback",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {
        
        df <- df %>% 
            filter(Date - days(day(Date) -1) == month_) %>%
            group_by(Corridor, Zone_Group) %>%
            summarize(uptime = mean(uptime.all)) %>%
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime, 
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                hoverinfo = "y+x",
                showlegend = FALSE) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%
            
            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Detector Uptime (%)"), 
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }
    
    
    if (zone_group_ == "All RTOP") {
        avg_daily_uptime <- avg_daily_uptime %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"), 
                   Date <= month_ + months(1))
    } else {
        avg_daily_uptime <- avg_daily_uptime %>% 
            filter(Zone_Group == zone_group_ & Date <= month_ + months(1))
    }
    
    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]
        
        p1 <- plot_detector_uptime_bar(avg_daily_uptime)
        
        plts <- lapply(seq_along(cdfs), function(i) { 
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE)) 
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}
get_cor_det_uptime_plot <- memoise(get_cor_det_uptime_plot_)

get_cor_comm_uptime_plot_ <- function(avg_daily_uptime,
                                      avg_monthly_uptime,
                                      month_,
                                      zone_group_,
                                      month_name) {
    
    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>% 
            add_lines(x = ~Date, 
                      y = ~uptime, #
                      color = I(BLUE),
                      name = "Uptime", #
                      legendgroup = "Uptime", #
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {
        
        df <- df %>% 
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime, 
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                hoverinfo = "y+x",
                showlegend = FALSE) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%
            
            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Comms Uptime (%)"), 
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }
    
    
    if (zone_group_ == "All RTOP") {
        avg_daily_uptime <- avg_daily_uptime %>% 
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2"), 
                   Date <= month_ + months(1))
        avg_monthly_uptime <- avg_monthly_uptime %>%
            filter(Zone_Group %in% c("RTOP1", "RTOP2", "All RTOP"), 
                   !Corridor %in% c("RTOP1", "RTOP2") 
                   & Month == month_)
    } else {
        avg_daily_uptime <- avg_daily_uptime %>% 
            filter(Zone_Group == zone_group_ & Date <= month_ + months(1))
        avg_monthly_uptime <- avg_monthly_uptime %>%
            filter(Zone_Group == zone_group_ & Month == month_)
    }
    
    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]
        
        p1 <- plot_detector_uptime_bar(avg_monthly_uptime)
        
        plts <- lapply(seq_along(cdfs), function(i) { 
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE)) 
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}
get_cor_comm_uptime_plot <- memoise(get_cor_comm_uptime_plot_)

# Reshape to show multiple on the same chart
gather_outstanding_events <- function(cor_monthly_events) {
    
    cor_monthly_events %>% 
        ungroup() %>% 
        gather(Events, Status, -c(Month, Corridor, Zone_Group))
}

plot_teams_tasks_ <- function(tab, var_,
                              title_ = "", textpos = "auto", height_ = 300) { #
    
    var_ <- as.name(var_)
    
    tab <- tab %>% 
        arrange(!!var_) %>% 
        mutate(var = fct_rev(factor(!!var_, levels = !!var_)))
    
    p1 <- plot_ly(data = tab, height = height_) %>%
        add_bars(x = ~Rep, 
                 y = ~var,
                 color = I(LIGHT_BLUE),
                 name = "Reported",
                 text = ~Rep,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "black"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Reported this Month",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p2 <- plot_ly(data = tab) %>%
        add_bars(x = ~Res, 
                 y = ~var,
                 color = I(BLUE),
                 name = "Resolved",
                 text = ~Res,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Resolved this Month",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p3 <- plot_ly(data = tab) %>% 
        add_bars(x = ~outstanding, 
                 y = ~var, 
                 color = I(ORANGE),
                 name = "Outstanding",
                 text = ~outstanding,
                 textposition = textpos, 
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Cumulative Tasks Outstanding",
                            zeroline = FALSE),
               margin = list(pad = 4))
    subplot(p1, p2, p3, shareY = TRUE, shareX = TRUE) %>% 
        layout(title = title_)
}
plot_teams_tasks <- memoise(plot_teams_tasks_)

plot_tasks_ <- function(type_plot, source_plot, priority_plot, 
                        month_name = input$month) {
    
    p1 <- subplot(type_plot, source_plot, priority_plot, 
                  heights = c(0.42, 0.42, 0.16), nrows = 3, margin = 0.05) %>% 
        layout(paper_bgcolor = "#f0f0f0", 
               annotations = list(
                   list(text = paste("Tasks by Type -", month_name), 
                        font = list(size = 12), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "bottom",
                        xanchor = "center",
                        x = 0.5, 
                        y = 1.0,
                        showarrow = FALSE,
                        showlegend = FALSE), 
                   list(text = paste("Tasks by Source -", month_name), 
                        font = list(size = 12), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "top",
                        xanchor = "center",
                        x = 0.5, 
                        y = 0.58,
                        showarrow = FALSE,
                        showlegend = FALSE), 
                   list(text = paste("Tasks by Priority -", month_name), 
                        font = list(size = 12), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "top",
                        xanchor = "center",
                        x = 0.5, 
                        y = 0.16,
                        showarrow = FALSE,
                        showlegend = FALSE)))
    
    p2 <- subtype_plot %>% 
        layout(annotations = list(text = paste("Tasks by Subtype -", month_name), 
                                  font = list(size = 12), 
                                  xref = "paper", 
                                  yref = "paper", 
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  x = 0.5, 
                                  y = 1.0,
                                  showarrow = FALSE))
    
    subplot(p1, p2, nrows = 1, margin = 0.1)
}
plot_tasks <- memoise(plot_tasks_)

# Number reported and resolved in each month. Side-by-side.
cum_events_plot_ <- function(df) {
    plot_ly(height = 300) %>%
        add_bars(data = filter(df, Events == "Reported"),
                 x = ~Month,
                 y = ~Status,
                 name = "Reported",
                 marker = list(color = LIGHT_BLUE)) %>%
        add_bars(data = filter(df, Events == "Resolved"),
                 x = ~Month,
                 y = ~Status,
                 name = "Resolved",
                 marker = list(color = BLUE)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter', mode = 'lines', name = 'Outstanding', #fill = 'tozeroy',
                  line = list(color = ORANGE)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter',
                  name = 'Outstanding',
                  marker = list(color = ORANGE),
                  showlegend = FALSE) %>%
        layout(barmode = "group",
               yaxis = list(title = "Events"),
               xaxis = list(title = ""))
}
cum_events_plot <- memoise(cum_events_plot_)


plot_cctvs <- function(df, month_) {
    
    start_date <- ymd("2018-02-01")
    end_date <- end_date %m+% months(1) - days(1)
    
    df_ <- filter(df, Date >= start_date & Date <= end_date & Size > 0)
    
    if (nrow(df) > 0) {
        
        #date_range <- ymd(date_range)
        
        p <- ggplot() + 
            
            # tile plot
            geom_tile(data = df, 
                      aes(x = Date, 
                          y = CameraID), 
                      fill = "steelblue", 
                      color = "white") + 
            
            # fonts, text size and labels and such
            theme(panel.grid.major = element_blank(),
                  panel.grid.minor = element_blank(),
                  axis.ticks.x = element_line(color = "gray50"),
                  axis.text.x = element_text(size = 11),
                  axis.text.y = element_text(size = 11),
                  axis.ticks.y = element_blank(),
                  axis.title = element_text(size = 11)) +
            scale_x_date(position = "top", limits = c(start_date, end_date)) +
            labs(x = "", 
                 y = "Intersection (and phase, if applicable)") +
            
            # draw white gridlines between tick labels
            geom_vline(xintercept = as.numeric(seq(start_date, end_date, by = "1 day")) - 0.5, 
                       color = "white")
        
        if (length(unique(df$CameraID)) > 1) {
            p <- p +
                geom_hline(yintercept = seq(1.5, length(unique(df$CameraID)) - 0.5, by = 1), 
                           color = "white")
            
        }
        p
    }
}