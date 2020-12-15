#
# Define classes and functions that operate on classes
# Possibly make this file all about the metrics class and rename to metrics.R
#

library(shiny)
library(yaml)

metrics <- read_yaml("metrics.yaml")



vpd <- structure(metrics[["daily_traffic_volume"]], class = "metric")
throughput <- structure(metrics[["throughput"]], class = "metric")
aog <- arrivals_on_green <- structure(metrics[["arrivals_on_green"]], class = "metric")
progression_ratio <- structure(metrics[["progression_ratio"]], class = "metric")
queue_spillback_rate <- structure(metrics[["queue_spillback_rate"]], class = "metric")
peak_period_split_failures <- structure(metrics[["peak_period_split_failures"]], class = "metric")
off_peak_split_failures <- structure(metrics[["off_peak_split_failures"]], class = "metric")
travel_time_index <- structure(metrics[["travel_time_index"]], class = "metric")
planning_time_index <- structure(metrics[["planning_time_index"]], class = "metric")
average_speed <- structure(metrics[["average_speed"]], class = "metric")
daily_pedestrian_pushbuttons <- structure(metrics[["daily_pedestrian_pushbuttons"]], class = "metric")
detector_uptime <- structure(metrics[["detector_uptime"]], class = "metric")
ped_button_uptime <- structure(metrics[["ped_button_uptime"]], class = "metric")
cctv_uptime <- structure(metrics[["cctv_uptime"]], class = "metric")
comm_uptime <- structure(metrics[["comm_uptime"]], class = "metric")
rsu_uptime <- structure(metrics[["rsu_uptime"]], class = "metric")



get_descriptionBlock <- function(x, zone_group, month, quarter = NULL) {

    if (is.null(quarter)) { # want monthly, not quarterly data
        vals <- cor$mo[[x$table]] %>%
            dplyr::filter(Corridor == zone_group & Month == month) %>% as.list()
    } else {
        vals <- cor$qu[[x$table]] %>%
            dplyr::filter(Corridor == zone_group & Quarter == quarter) %>% as.list()
    }
    
    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)
    
    if (vals$delta > 0) {
        delta_prefix <- " +"
        color <- "success"
        color_icon <- "caret-up"
    } else if (vals$delta == 0) {
        delta_prefix <- "  "
        color <- "black"
        color_icon <- NULL
    } else { # vals$delta < 0
        delta_prefix <- "  "
        color <- "failure"
        color_icon <- "caret-down"
    }
    
    value <- data_format(x$data_type)(vals[[x$variable]])
    delta <- glue('{delta_prefix} {as_pct(vals$delta)}')
    
    
    validate(need(value, message = "NA"))
    
    
    descriptionBlock(
        number = delta,
        numberColor = color,
        numberIcon = color_icon,
        header = value,
        text = x$label,
        rightBorder = TRUE,
        marginBottom = FALSE
    )
}



get_minimal_trendline <- function(x, zone_group) {
    ggplot(
        data = filter(cor$wk[[x$table]], 
                      as.character(Zone_Group) == as.character(Corridor), 
                      Zone_Group == zone_group), 
        mapping = aes_string(x = "Date", y = x$variable)) + 
        geom_line(color = GDOT_BLUE) + 
        theme_void()
}



# Empty plot - space filler
x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
p0 <- plot_ly(type = "scatter", mode = "markers") %>% layout(xaxis = x0, yaxis = x0)



get_trend_multiplot <- function(x, zone_group, month) {
    
    var_ <- as.name(x$variable)

    mdf <- filter_mr_data(cor$mo[[x$table]], zone_group)
    
    per <- if_else(x$has_weekly, "wk", "mo")
    wdf <- filter_mr_data(cor[[per]][[x$table]], zone_group)
    
    if (nrow(mdf) > 0 & nrow(wdf) > 0) {
        # Current Month Data
        mdf <- mdf %>%
            filter(Month == month) %>%
            arrange(!!var_) %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor == zone_group, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        sdm <- SharedData$new(mdf, ~Corridor, group = "grp")
        
        bar_chart <- plot_ly(sdm,
                             type = "bar",
                             x = ~var, 
                             y = ~Corridor,
                             marker = list(color = ~col),
                             text = ~data_format(x$data_type)(var),
                             textposition = "auto",
                             insidetextfont = list(color = "black"),
                             name = "",
                             customdata = ~glue(paste(
                                 "<b>{Description}</b>",
                                 "<br>{x$label}: <b>{data_format(x$data_type)(var)}</b>")),
                             hovertemplate = "%{customdata}",
                             hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>% layout(
                barmode = "overlay",
                xaxis = list(title = "Selected Month", 
                             zeroline = FALSE, 
                             tickformat = tick_format(x$data_type)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
        if (!is.null(x$goal)) {
            bar_chart <- bar_chart %>% 
                add_lines(x = x$goal,
                          y = ~Corridor,
                          mode = "lines",
                          marker = NULL,
                          line = list(color = LIGHT_RED),
                          name = "Goal",
                          showlegend = FALSE)
        }
        
        # Weekly Data - historical trend
        wdf <- wdf %>%
            filter(Date < month + months(1)) %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor == zone_group, 0, 1))) %>%
            group_by(Corridor)
        
        sdw <- SharedData$new(wdf, ~Corridor, group = "grp")
        
        weekly_line_chart <- plot_ly(sdw,
                                     type = "scatter",
                                     mode = "lines",
                                     x = ~Date, 
                                     y = ~var, 
                                     color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                                     alpha = 0.6,
                                     name = "",
                                     customdata = ~glue(paste(
                                         "<b>{Description}</b>",
                                         "<br>Week of: <b>{format(Date, '%B %e, %Y')}</b>",
                                         "<br>{x$label}: <b>{data_format(x$data_type)(var)}</b>")),
                                     hovertemplate = "%{customdata}",
                                     hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>% layout(xaxis = list(title = "Weekly Trend"),
                   yaxis = list(tickformat = tick_format(x$data_type),
                                hoverformat = tick_format(x$data_type)),
                   title = "__plot1_title__",
                   showlegend = FALSE,
                   margin = list(t = 50)
            )

        # Hourly Data - current month
        if (!is.null(x$hourly_table)) {
            
            hdf <- filter_mr_data(cor$mo[[x$hourly_table]], zone_group) %>%
                filter(date(Hour) == month) %>%
                mutate(var = !!var_,
                       col = factor(ifelse(Corridor == zone_group, 0, 1))) %>%
                group_by(Corridor)
            
            sdh <- SharedData$new(hdf, ~Corridor, group = "grp")
            
            hourly_line_chart <- plot_ly(sdh) %>%
                add_lines(x = ~Hour,
                          y = ~var,
                          color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                          alpha = 0.6,
                          name = "",
                          customdata = ~glue(paste(
                              "<b>{Description}</b>",
                              "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                              "<br>{x$label}: <b>{data_format(x$data_type)(var)}</b>")),
                          hovertemplate = "%{customdata}",
                          hoverlabel = list(font = list(family = "Source Sans Pro"))
                ) %>% layout(xaxis = list(title = x$label),
                       yaxis = list(tickformat = tick_format(x$data_type)),
                       title = "__plot2_title__",
                       showlegend = FALSE)
            
            s1 <- subplot(weekly_line_chart, p0, hourly_line_chart, 
                          titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        } else {
            s1 <- weekly_line_chart
        }
        
        subplot(bar_chart, s1, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100),
                   title = x$label) %>%
            highlight(
                color = x$highlight_color, 
                opacityDim = 0.9, 
                defaultValues = c(zone_group),
                selected = attrs_selected(
                    insidetextfont = list(color = "white"), 
                    textposition = "auto"),
                on = "plotly_click",
                off = "plotly_doubleclick")
        
    } else(
        no_data_plot("")
    )
}



get_valuebox_value <- function(x, zone_group, month, quarter = NULL, line_break = FALSE) {
    
    
    if (is.null(quarter)) { # want monthly, not quarterly data
        vals <- get_metric(aurora, x, "cor", "mo", zone_group, month) %>% as.list()
    } else {
        vals <- get_metric(aurora, x, "cor", "mo", zone_group, quarter) %>% as.list()
    }
    
    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)
    
    if (vals$delta > 0) {
        delta_prefix <- " +"
    } else if (vals$delta == 0) {
        delta_prefix <- "  "
    } else { # vals$delta < 0
        delta_prefix <- "  "
    }
    
    value <- data_format(x$data_type)(vals[[x$variable]])
    delta <- glue('{delta_prefix} {as_pct(vals$delta)}')
    
    
    validate(need(value, message = "NA"))
    
    if (line_break) {
        tags$div(HTML(paste(
            value,
            tags$p(delta, style = "font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;")
        )))
    } else {
        tags$div(HTML(paste(
            value, 
            tags$span(delta, style = "font-size: 70%;")
        )))
    }
}



get_metric <- function(aurora, x, agg, per, corridor = NULL, current_period = NULL) {
    
    # agg is one of: cor, sub, sig
    # per is one of: qu, mo, wk, dy
    # get_cor_metric(aurora, vpd, "cor", "mo", "All RTOP", ymd("2020-12-01"))
    
    table_name <- glue("{agg}_{per}_{x$table}")

    period <- switch(
        per,
        "mo" = "Month",
        "wk" = "Date",
        "dy" = "Date",
        "qu" = "Quarter")
    
    as_modifier <- switch (
        period,
        "Month" = as_date,
        "Date" = as_date,
        "Hour" = as_datetime,
        "Quarter" = factor)
    
    
    # TODO: Incorporate filter_mr_data here. 
    # Or just simplify the data frame so we're not re-using and abusing zone_group
    
    
    df <- tbl(aurora, table_name)
    if (!is.null(corridor)) {
        df <- df %>% filter(Corridor == corridor)
    }
    if (!is.null(current_period)) {
        df <- df %>% filter(!!as.name(period) == current_period)
    }
    df %>% collect() %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor),
               !!as.name(period) := as_modifier(!!as.name(period)))
}
