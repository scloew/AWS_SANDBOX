"""
following along with
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
"""

import boto3


def create_queue(name='test'):
    sqs = boto3.resource('sqs')

    # Create the queue. This returns an SQS.Queue instance
    queue = sqs.create_queue(QueueName=name, Attributes={'DelaySeconds': '5'})

    # You can now access identifiers and attributes
    print(queue.url)
    print(queue.attributes.get('DelaySeconds'))


def get_queue_by_name(name='test'):
    try:
        # Get the service resource
        sqs = boto3.resource('sqs')

        # Get the queue. This returns an SQS.Queue instance
        queue = sqs.get_queue_by_name(QueueName=name)

        # You can now access identifiers and attributes
        print(queue.url)
        print(queue.attributes.get('DelaySeconds'))
    except:
        print(f'failed to find queue: "{name}"')


def print_all_queues():
    sqs = boto3.resource('sqs')
    for queue in sqs.queues.all():
        print(queue.url)


def send_message(queue_name='test', message='hello test'):
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName=queue_name)
    #create message
    response = queue.send_message(MessageBody=message)

    #RESPONSE IS NOT A RESOURCES but gives you a message ID and MD5
    print(response.get('MessageId'))
    print(response.get('MD5OfMessageBody'))

    #create message with costume attributes
    queue.send_message(MessageBody='boto3', MessageAttributes={
        'Author': {
            'StringValue': 'Daniel',
            'DataType': 'String'
        }
    })

    dict_mess = {
        'Id': '3',
        'MessageBody': 'boto3',
        'MessageAttributes': {
            'Author': {
                'DataType': 'String',
                'StringValue': 'Oscar Wilde'
            },
            'Age': {
                'DataType': 'Number',
                'StringValue': '165'
            }
        }
    }

    #send message in batches
    #below line sends the two previous messages
    #Careful, when sending messages this way,
    #the funciton is send_messages (plural)
    response = queue.send_messages(Entries=[
        {
            'Id': '1',
            'MessageBody': message
        },
        {
            'Id': '2',
            'MessageBody': 'boto3',
            'MessageAttributes': {
                'Author': {
                    'StringValue': 'Daniel',
                    'DataType': 'String'
                }
            }
        },
        dict_mess
    ])

    #print failues
    print(response.get('Failed'))


def process_message(queue_name='test'):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    print(f'messages currently in queue {queue}')

    for msg in queue.receive_messages(MessageAttributeNames=['Author']):
        author_text = ''
        if msg.message_attributes:
            author_name = msg.message_attributes.get('Author').get('StringValue')
            author_text = f'{author_name}' if author_name else ''
        print(f'Hello {msg.body}, {author_text}')
        msg.delete()

if __name__ == '__main__':
    create_queue('tes_2')
    get_queue_by_name('test')
    get_queue_by_name('fail bot 3000')
    print_all_queues()
    send_message()
    process_message()
