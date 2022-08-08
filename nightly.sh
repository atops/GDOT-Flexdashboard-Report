#!/bin/bash
source /home/rstudio/.bashrc

H=$(TZ=America/New_York date +%H)

echo "System booted. The hour is ${H#0}"

# Only start the MARK 1 calcs and package scripts on startup if it's early morning
# i.e., it is being started by the script on The SAM after loading raw data into S3
#
# If starting the server up manually (which will be after 6am, hopefully), don't
# run this script.
#

cd /home/rstudio/Code/GDOT/production_scripts

bucket=`cat Monthly_Report.yaml | yq -r .bucket`

if [[ ${H#0} -lt 6 ]]; then
    echo $(TZ=America/New_York date)

    # this was moved to crontab to run at midnight. creates empty nightly.log.
    /usr/sbin/logrotate /home/rstudio/logrotate_nightly.conf --state /home/rstudio/logrotate-state

    Rscript Monthly_Report_Calcs_ec2.R
    Rscript Monthly_Report_Package.R
    echo "------------------------"

    cd SigOps
    Rscript Monthly_Report_Package.R
    echo "------------------------"
    Rscript Monthly_Report_Package_1hr.R
    echo "------------------------"
    Rscript Monthly_Report_Package_15min.R
    cd ..

    # Run User Delay Cost on the SAM on the 1st, 11th and 21st of the month
    if [[ $(date +%d) =~ 01|11|21 ]]; then
        python user_delay_costs.py
    fi

    aws s3 sync /home/rstudio/ s3://$bucket/logs --exclude "*" --include "nightly.lo*" --region us-east-1
    
    # Shut down when script completes
    aws ec2 stop-instances --instance-ids i-0ddfe60da0c6fe4bd

else
    echo "$(TZ=America/New_York date) - after 6am"
fi
