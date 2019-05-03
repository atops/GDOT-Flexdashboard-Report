#!/bin/bash

H=$(TZ=America/New_York date +%H)

echo "This always runs. The hour is ${H#0}"

# Only start the MARK 1 calcs and package scripts on startup if it's early morning
# i.e., it is being started by the script on The SAM after loading raw data into S3
#
# If starting the server up manually (which will be after 6am, hopefully), don't
# run this script.
#

cd /home/rstudio/Code/GDOT/GDOT-Flexdashboard-Report

if [[ ${H#0} -lt 6 ]]; then
    echo $(TZ=America/New_York date) #>> nightly.log
    Rscript Monthly_Report_Calcs_ec2.R #>> nightly.log
    Rscript Monthly_Report_Package.R #>> nightly.log
else
    echo "$(TZ=America/New_York date) - after 6am" #>> nightly.log
fi
