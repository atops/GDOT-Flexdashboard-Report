
setwd("c:/users/V0010894/Code/GDOT/GDOT-Flexdashboard-Report")
source("Monthly_Report_Functions.R")

fns <- list.files(pattern = ".*2018.*.fst$")

lapply(fns, function(fn) {
    print(fn)
	df <- read_fst(fn) %>% as_tibble()
	if ("Week" %in% names(df)) {
		if ("Date" %in% names(df)) {
			df <- df %>% mutate(Week = week(Date),
			                    Date = date(Date)) 
		} else if ("Month" %in% names(df)) {
			df <- df %>% mutate(Week = week(Month))
		} else if ("Hour" %in% names(df)) {
		    df <- df %>% mutate(Week = week(date(Hour)))
		}
	    #print(df)
	    write_fst(df, fn)
	} else {
	    print("No match")
	}
})
