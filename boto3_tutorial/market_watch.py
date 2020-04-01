"""
following along with tutorial beginning at
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-creating-alarms.html

terminology:

alarm: watches a single metric over period of time; performs 1+ actions based
       on value of metric relative to threshold over number of time periods

rule: Allows to match events and route them to 1+ target event
    related cloudwatch functions:
        1) put_rule
        2) put_targets
        3) put_events

ARN: "Amazon resource name"

subscriptions: provide access to a real-time feed of log events from CloudWatch Logs and
               deliver that feed to services (ex: AWS Lambda) for custom processing,
               analysis, or loading to other systems

subscription filter: defines the pattern to use for filtering which log events
                     are delivered to your AWS resource
                     -- destination of is an AWS Lambda function

related subscription filter methods:
        1) get_paginator
        2) put_subscription_filter
        3) delete_subscription_filter
"""

import boto3
import json


def print_alarms():
    # create CloudWatch client if one does not exist
    cloudwatch = boto3.client('cloudwatch')

    # List alarms of insufficent data through the pagination interface
    # paginator docs: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    paginator = cloudwatch.get_paginator('describe_alarms')
    for response in paginator.paginate(StateValue='INSUFFICIENT_DATA'):
        print(response['MetricAlarms'])


def create_alarm(alarm_name='Web_Server_CPU_Utilization'):
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
    print_alarms()


def delete_alarm(alarm_name='Web_Server_CPU_Utilization'):
    cloudwatch = boto3.client('cloudwatch')

    cloudwatch.delete_alarms(
        AlarmNames=[alarm_name]
    )
    print_alarms()


def create_alarm_with_actions(alarm_name='Web_Server_CPU_Utilization'):
    cloudwatch = boto3.client('cloudwatch')

    # Create alarm with actions enabled
    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator='GreaterThanThreshold',
        EvaluationPeriods=1,
        MetricName='CPUUtilization',
        Namespace='AWS/EC2',
        Period=60,
        Statistic='Average',
        Threshold=70.0,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:swf:us-east-1:{CUSTOMER_ACCOUNT}:action/actions/AWS_EC2.InstanceId.Reboot/1.0'
        ],
        AlarmDescription='Alarm when server CPU exceeds 70%',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': 'INSTANCE_ID'
            },
        ],
        Unit='Seconds'
    )
    print_alarms()


def disable_action(alarm_name='Web_Server_CPU_Utilization'):
    """
    this function will turn 'ActionsEnabled' field
    for <alarm_name> to False
    """
    cloudwatch = boto3.client('cloudwatch')

    # disable alarm
    cloudwatch.disable_alarm_actions(
        AlarmNames=[alarm_name]
    )
    print_alarms()


def list_metrics(args=None):
    """
    function code from: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-metrics.html
    """
    if not args:
        kargs = {'Dimensions': [{'Name': 'LogGroupName'}],
                 'MetricName': 'IncomingLogEvents',
                 'Namespace': 'AWS/Logs'}
    else:
        print(f'args are triggered {args}')
        kargs = args

    cloudwatch = boto3.client('cloudwatch')

    # list metrics through the paginator interface
    # more on paginators at: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    paginator = cloudwatch.get_paginator('list_metrics')

    for response in paginator.paginate(**kargs):
        print(response['Metrics'])


def publish_metric():
    cloudwatch = boto3.client('cloudwatch')

    args = {'Dimensions': [{'Name': 'UNIQUE_PAGES', 'Value': 'URLS'}],
            'MetricName': 'PAGES_VISITED',
            'Namespace': 'SITE/TRAFFIC'}

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'PAGES_VISITED',
                'Dimensions': [
                    {
                        'Name': 'UNIQUE_PAGES',
                        'Value': 'URLS'
                    },
                ],
                'Unit': 'None',
                'Value': 1.0
            },
        ],
        Namespace='SITE/TRAFFIC'
    )

    list_metrics(args)


def create_rule(acc_id=None, args=None):
    """
    Create a scheduled rule; demonstrates put_rule method.
    For more on enabling put_rule:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#CloudWatchEvents.Client.put_rule
    For disabling events see:
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#CloudWatchEvents.Client.disable_rule
    """
    cloudwatch_events = boto3.client('events')
    kargs = None
    if args:
        kargs = args
    else:
        kargs = {
            'Name':'DEMO_EVENT',
            'RoleArn':f'arn:aws:iam::{acc_id}:role/aws-service-role/events.amazonaws.com/AWSServiceRoleForCloudWatchEvents',
            'ScheduleExpression':'rate(5 minutes)',
            'State':'ENABLED'
        }

    #put an event rule
    response = cloudwatch_events.put_rule(**kargs)
    print(response['RuleArn'])


def add_lambda_alarm(acc_id, region='us-east-1', func_name='LogEC2InstanceStateChange'):
    """
    demonstrates put_targets method; for more see
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#CloudWatchEvents.Client.put_targets
    """
    cloudwatch_events = boto3.client('events')

    #put target for rule
    response = cloudwatch_events.put_targets(
        Rule='DEMO_EVENT',
        Targets=[
            {
                'Arn': f'arn:aws:lambda:{region}:{acc_id}:function:{func_name}',
                'Id': 'myCloudWatchEventsTarget',
            }
        ]
    )
    print(response)


def send_events():
    """
    demonstrates put_events method; for more see
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#CloudWatchEvents.Client.put_events
    """

    cloudwatch_events = boto3.client('events')

    # Put an event
    response = cloudwatch_events.put_events(
        Entries=[
            {
                'Detail': json.dumps({'key1': 'value1', 'key2': 'value2'}),
                'DetailType': 'appRequestSubmitted',
                'Resources': [
                    'RESOURCE_ARN',
                ],
                'Source': 'com.company.myapp'
            }
        ]
    )
    print(response['Entries'])


def list_existing_subscription_filters():
    """
    demonstrates the use of get_paginator; for more see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    """
    cloudwatch_logs=boto3.client('logs')

    #Use paginator interface to list subcription filters
    paginator=cloudwatch_logs.get_paginator('describe_subscription_filters')
    for response in paginator.paginate(logGroupName='GROUP_NAME'):
        print(response['subscriptionFilters'])


def create_subscription_filter(acc_id, region='us-east-1', func_name='LogEC2InstanceStateChange'):
    """
    demonstrates the use of put_subscription_filter; for more see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.put_subscription_filter
    """
    cloudwatch_logs=boto3.client('logs')
    return #below content causes error
    #create subscription filter
    cloudwatch_logs.put_subscription_filter(
        #destinationArn=f'arn:aws:lambda:{region}:{acc_id}:function:{func_name}',
        destinationArn='LogEC2InstanceStateChange',
        filterName='FILTER_NAME',
        filterPattern='ERROR',
        logGroupName=f'arn:aws:{region}:{acc_id}:log-group:/codebuild-sandbox-bf179249-670d-461b-86b4-e515c7da8354:*' #/aws/codebuild/sandbox
        #need ligitimate group for this to work
    )
    # cloudwatch_logs.put_subscription_filter(
    #     destinationArn='LAMBDA_FUNCTION_ARN',
    #     filterName='FILTER_NAME',
    #     filterPattern='ERROR',
    #     logGroupName='LOG_GROUP', #need ligitimate group for this to work
    # )


def delete_subscription_filter():
    """
    demonstrates the use of delete_subscription_filter; for more see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.delete_subscription_filter
    """

    cloudwatch_logs = boto3.client('logs')

    #delete filter
    cloudwatch_logs.delete_subscription(
        filterName='FILTER_NAME',
        logGroupName='LOG_GROUP'
    )



if __name__ == '__main__':
    acc_id = input('enter account id: ')
    print('\n***\nstep 1 of tutorial')
    print('https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-creating-alarms.html\n***\n')
    print('\n***\ncreate alarm\n***\n')
    create_alarm()
    print('\n***\ndelete alarm\n***\n')
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
    print('\n***\nstep 2 of tutorial')
    print('https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-using-alarms.html\n***\n')
    print('\n***\ncreate alarm with action\n***\n')
    create_alarm_with_actions()
    print('\n***\ndisable action on alarm\n***\n')
    disable_action()
    print('\n***\ndelete alarm\n***\n')
    delete_alarm()
    print('\n***\nstep 3 of tutorial; metrics')
    print('https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-metrics.html\n***\n')
    print('\n***\nlist metrics\n***\n')
    list_metrics()
    print('\n***\nadd metric\n***\n')
    publish_metric()
    print('\n***\nstep 4 of tutorial; rules and events')
    print('https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-events.html\n***\n')
    print('\n***\ncreate rule\n***\n')
    create_rule(acc_id)
    print('\n***\ncreate lambda alarm\n***\n')
    add_lambda_alarm(acc_id)
    print('\n***\nsend event\n***\n')
    send_events()
    print('\n***\nstep 5 of tutorial; subscription filters')
    print('https://boto3.amazonaws.com/v1/documentation/api/latest/guide/cw-example-subscription-filters.html\n***\n')
    print('\n***\ncreate subscription filters\n***\n')
    create_subscription_filter(acc_id)
    print('\n***\nlist existing subscription filters\n***\n')
    #list_existing_subscription_filters()


