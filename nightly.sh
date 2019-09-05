#!/bin/bash

H=$(TZ=America/New_York date +%H)

echo "System booted. The hour is ${H#0}"

# Only start the MARK 1 calcs and package scripts on startup if it's early morning
# i.e., it is being started by the script on The SAM after loading raw data into S3
#
# If starting the server up manually (which will be after 6am, hopefully), don't
# run this script.
#

cd /home/rstudio/Code/GDOT/GDOT-Flexdashboard-Report

if [[ ${H#0} -lt 6 ]]; then
    echo $(TZ=America/New_York date)
    Rscript Monthly_Report_Calcs_ec2.R
    Rscript Monthly_Report_Package.R
    
    # this was moved to crontab to run at midnight. creates empty nightly.log.
    #/usr/sbin/logrotate /home/rstudio/logrotate_nightly.conf --state=/home/rstudio/logrotate-state --verbose
    aws s3 sync /home/rstudio/ s3://gdot-spm/logs --exclude "*" --include "nightly.lo*"
    
    systemctl poweroff -i
else
    echo "$(TZ=America/New_York date) - after 6am"
fi

