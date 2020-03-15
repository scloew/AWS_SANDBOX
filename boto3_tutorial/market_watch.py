"""
following along with
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-creating-alarms.html

terminology:

alarm: watches a single metric over period of time; performs 1+ actions based
       on value of metric relative to threshold over number of time periods
"""

import boto3


def print_alarms(cloudwatch= None):
    #create CloudWatch client if one does not exist
    if not cloudwatch:
        cloudwatch = boto3.client('cloudwatch')

    #List alarms of insufficent data through the pagination interface
    #paginator docs: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    paginator = cloudwatch.get_paginator('describe_alarms')
    for response in paginator.paginate(StateValue='INSUFFICIENT_DATA'):
        print(response['MetricAlarms'])


def create_alarm(alarm_name = 'Web_Server_CPU_Utilization'):
    """
    Note, the alarm name may change but everything else will always be the same
    alarm docs found at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html
    """

    cloudwatch = boto3.client('cloudwatch')
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=60,
        Statistic='Average',
        Threshold=70.0,
        ActionsEnabled=False,
        AlarmDescription='Alarm when server CPU exceeds 70%',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': 'INSTANCE_ID'
            },
        ],
        Unit='Seconds'
    )

    print_alarms(cloudwatch)


def delete_alarm(alarm_name='Web_Server_CPU_Utilization'):
    cloudwatch = boto3.client('cloudwatch')

    cloudwatch.delete_alarms(
        AlarmNames=[alarm_name]
    )
    print_alarms(cloudwatch)


if __name__ == '__main__':
    print('\n***\ncreate\n***\n')
    create_alarm()
    print('\n***\ndelete\n***\n')
    delete_alarm()
    input('\n***\nbreak\n***\n')
    print('\n***\ncreate foo\n***\n')
    create_alarm('foo')
    print('\n***\ncreate bar\n***\n')
    create_alarm('bar')
    print('\n***\ndelete bar\n***\n')
    delete_alarm('bar')
    print('\n***\ndelete foo\n***\n')
    delete_alarm('foo')