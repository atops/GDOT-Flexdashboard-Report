# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 14:16:38 2020

@author: Alan.Toppen
"""

import boto3
from datetime import datetime
import time
import string



# -- Functions --

def image_exists(image_name):

    response = ec2_client.describe_images(
        Filters=[
            {'Name': 'name',
             'Values': [image_name,]}
        ]
    )
    return len(response['Images']) > 0

def create_ami(today):

    root_volume = {
        'DeviceName': '/dev/sda1',
        'Ebs': {
            'VolumeSize': 8,
        },
    }

    cache_volume = {
        'DeviceName': '/dev/sdb',
        'VirtualName': 'Shiny-Server Production Cache',
        'Ebs': {
            # 'Iops': 3000,
            # 'Throughput': 125
            'VolumeSize': 20,
            'VolumeType': 'gp2' # 'gp3'
        },
    }

    image_name = f'Shiny-Server-{today}'
    extensions = list(string.ascii_lowercase)[1:]

    while image_exists(image_name):
        image_name = f'Shiny-Server-{today}' + extensions.pop(0)

    response = ec2_client.create_image(
        BlockDeviceMappings=[
            root_volume,
            cache_volume,
        ],
        Description='',
        InstanceId=EC2_INSTANCE_ID,
        Name=image_name,
        NoReboot=False,
        DryRun=False,
    )
    return response


def ami_is_available(image_id):
    response = ec2_client.describe_images(
        ImageIds=[
            image_id,
        ],
        DryRun=False
    )
    return response['Images'][0]['State']=='available'



def create_launch_template(ec2_client, image_id):
    response = ec2_client.create_launch_template_version(
        DryRun=False,
        LaunchTemplateId=LAUNCH_TEMPLATE_ID,
        LaunchTemplateData={
            'EbsOptimized': False,
            'IamInstanceProfile': {
                'Arn': 'arn:aws:iam::322643905670:instance-profile/GDOT-Rstudio-EC2'
            },
            'ImageId': image_id,
            'InstanceType': EC2_INSTANCE_TYPE,
            'KeyName': 'ec2_kp',
            'TagSpecifications': [
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Owner',
                            'Value': 'GDOT'
                        },
                    ]
                },
            ],
            'SecurityGroupIds': [
                'sg-0b24db860733f6bcc',
            ],
        }
    )
    return response


def set_version(ec2_client):
    response = ec2_client.modify_launch_template(
        DryRun=False,
        LaunchTemplateId=LAUNCH_TEMPLATE_ID,
        DefaultVersion=str(launch_template_response['LaunchTemplateVersion']['VersionNumber'])
    )
    return response



def delete_old_versions(ec2_client, keep=10):
    template_versions_response = ec2_client.describe_launch_template_versions(
        LaunchTemplateId=LAUNCH_TEMPLATE_ID,
    )
    versions = template_versions_response['LaunchTemplateVersions']
    version_numbers = sorted([v['VersionNumber'] for v in versions], reverse=True)
    version_numbers_to_delete = version_numbers[keep:]

    for v in version_numbers_to_delete :
        print(f'Deleting Launch Template Version: {v}')
        ec2_client.delete_launch_template_versions(
            DryRun=False,
            LaunchTemplateId=LAUNCH_TEMPLATE_ID,
            Versions=[str(v)]
        )


def refresh_autoscaling(autoscaling_client):
    response = autoscaling_client.start_instance_refresh(
        AutoScalingGroupName=AUTOSCALING_GROUP_NAME,
        Strategy='Rolling',
        Preferences={
            'MinHealthyPercentage': 90,
            'InstanceWarmup': 300
        }
    )
    return response


if __name__=='__main__':

    # -- Constants and Globals --

    ec2_client = boto3.client('ec2')
    autoscaling_client = boto3.client('autoscaling')

    today = datetime.today().strftime("%F")

    EC2_INSTANCE_ID = 'i-00a90d0152470f49b'
    EC2_INSTANCE_TYPE = 't3.large'
    AUTOSCALING_GROUP_NAME = 'Shiny-Server-Auto-Scaling-Group-2020-09-18'
    LAUNCH_TEMPLATE_ID = 'lt-0f0fa90ee94062747'

    # -- Run --

    # Step 1: Create Image from AMI Instance and wait for it to be available
    create_image_response = create_ami(today)

    while not ami_is_available(create_image_response['ImageId']):
        print('.', end='')
        time.sleep(5)
    print('.')

    # Step 2: Create New Launch Template
    launch_template_response = create_launch_template(
        ec2_client, create_image_response['ImageId'])

    # Step 3: Make Latest version the default version
    #   Auto Scaling Group is automatically updated with new Launch Template
    modify_launch_template_response = set_version(ec2_client)

    # delete older launch templates. Maybe keep the last 10 versions.
    delete_old_versions(ec2_client, keep=10)

    # Step 4: Refresh Instance
    autoscaling_instance_refresh_response = refresh_autoscaling(autoscaling_client)

    autoscaling_description_response = autoscaling_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[AUTOSCALING_GROUP_NAME]
    )

    instances = autoscaling_description_response['AutoScalingGroups'][0]['Instances']

    for instance in instances:
        print(f"Version {instance['LaunchTemplate']['Version']}: {instance['LifecycleState']}")

