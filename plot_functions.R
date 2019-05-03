
# plot_functions.R

library(lubridate)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)
library(ggthemes)

GDOT_BLUE = "#256194";



plot_alerts <- function(df, date_range) {
    
    if (nrow(df) > 0) {
        
        #date_range <- ymd(date_range)
        
        p <- ggplot() + 
            
            # tile plot
            geom_tile(data = df, 
                      aes(x = Date, 
                          y = signal_phase), 
                      fill = GDOT_BLUE, 
                      color = "white") + 
            
            # fonts, text size and labels and such
            theme(panel.grid.major = element_blank(),
                  panel.grid.minor = element_blank(),
                  axis.ticks.x = element_line(color = "gray50"),
                  axis.text.x = element_text(size = 11),
                  axis.text.y = element_text(size = 11),
                  axis.ticks.y = element_blank(),
                  axis.title = element_text(size = 11)) +

            scale_x_date(position = "top") + #, limits = date_range) #+
            scale_y_discrete(limits = rev(levels(df$signal_phase))) +
            labs(x = "",
                 y = "") + #Intersection (and phase, if applicable)") +


            # draw white gridlines between tick labels
            geom_vline(xintercept = as.numeric(seq(date_range[1], date_range[2], by = "1 day")) - 0.5,
                       color = "white")
        
        if (length(unique(df$signal_phase)) > 1) {
            p <- p +
                geom_hline(yintercept = seq(1.5, length(unique(df$signal_phase)) - 0.5, by = 1), 
                           color = "white")
        }
        p
    }
}

plot_height <- function(n) {
    css_px <- as.character(50 + n * 14)
    return (paste0(css_px, "px"))
}

plot_empty <- function(zone) {
    
    ggplot() + 
        annotate("text", x = 1, y = 1, 
                 label = "No Data") + 
        theme(panel.grid.major = element_blank(), 
              panel.grid.minor = element_blank(), 
              axis.ticks = element_blank(), 
              axis.text = element_blank(), 
              axis.title = element_blank())
}
