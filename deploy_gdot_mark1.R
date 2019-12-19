


# Deploy GDOT_MARK1_beta to shinyapps.io


#
# -- PRE-RENDERED (test) --
#

library(rsconnect)

deployApp(appPrimaryDoc = "Monthly_Report_s3_prerendered.Rmd", 
          appFileManifest = "GDOT_MARK1_prerendered.manifest", 
          appName="GDOT_MARK1_prerendered", 
          logLevel="normal")




#
# -- BETA --
#

library(rsconnect)

deployApp(appPrimaryDoc = "Monthly_Report_beta_async.Rmd", 
          appFileManifest = "GDOT_MARK1_beta.manifest", 
          appName="GDOT_MARK1_beta", 
          logLevel="normal")




#
# -- STAGING --
#

library(rsconnect)

deployApp(appPrimaryDoc = "Monthly_Report_s3.Rmd", 
          appFileManifest = "GDOT_MARK1.manifest", 
          appName="GDOT_MARK1_staging", 
          logLevel="normal")






#
# -- PRODUCTION --
#

library(rsconnect)

deployApp(appPrimaryDoc = "Monthly_Report_s3.Rmd", 
          appFileManifest = "GDOT_MARK1.manifest", 
          appName="GDOT_MARK1", 
          logLevel="normal")









# Deploy Test Signal_Dashboards to shinyapps.io

library(rsconnect)

deployApp(appPrimaryDoc = "Monthly_Report_sd_temp.Rmd", 
          appFileManifest = "GDOT_MARK1.manifest", 
          appName="GDOT_MARK1_temp", 
          logLevel="normal")



