"""
turorial code from: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
"""

import boto3
from boto3.dynamodb.conditions import Key, Attr


def create_table_demo(table_name):
    """
    demonstrates use of dynamoDB resource create_table
    for more on create_table see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.ServiceResource.create_table
    """
    dynamodb = boto3.resource('dynamodb')

    # create table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'last_name',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'last_name',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # wait until table exists
    table.meta.client.get_waiter('table_exists').wait(TableName='users')

    # print table data
    print(table.item_count)


def use_existing_table(table_name):
    """
    demonstrates creating DynamoDB.Table resource from existing table
    Note attributes of this table are lazy-loaded
    for more on DynamoDB.Table resource see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table
    """
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)

    print(table.creation_date_time)


def create_item(table_name):
    """
    demonstrate adding new item to table via DynamoDB.Table.put_item method
    For more on put_item see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    table.put_item(
        Item={
            'username': 'Homer_Jay',
            'first_name': 'Homer',
            'last_name': 'Simpson',
            'age': 39,
            'account_type': 'standard_user',
        }
    )


def get_item(table_name, user_name, last_name):
    """
    demonstrate adding new item to table via DynamoDB.Table.get_item method
    For more on get_item see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.get_item(
        Key={
            'username': user_name,
            'last_name': last_name
        }
    )
    print(response['Item'])


def update_item(table_name, user_name, last_name):
    """
    demonstrate adding new item to table via DynamoDB.Table.update_item method
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    table.update_item(
        Key={
            'username': user_name,
            'last_name': last_name
        },
        UpdateExpression='SET age = :new_age',
        ExpressionAttributeValues={
            ':new_age': 40
        }
    )
    get_item(table_name, user_name, last_name)


def delete_item_demo(table_name, user_name, last_name):
    """
    demonstrate adding new item to table via DynamoDB.Table.delete_item method
    For more on delete_item see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.delete_item
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    table.delete_item(
        Key={
            'username': user_name,
            'last_name': last_name
        }
    )
    get_item(table_name, user_name, last_name)


def batch_writing(table_name):
    """
    demonstrate adding new item to table via DynamoDB.Table.batch_writer method
    Used for loading many items or more on batch_writer see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.batch_writer
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        batch.put_item(
            Item={
                'account_type': 'standard_user',
                'username': 'johndoe',
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 25,
                'address': {
                    'road': '1 Jefferson Street',
                    'city': 'Los Angeles',
                    'state': 'CA',
                    'zipcode': 90001
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'super_user',
                'username': 'janedoering',
                'first_name': 'Jane',
                'last_name': 'Doering',
                'age': 40,
                'address': {
                    'road': '2 Washington Avenue',
                    'city': 'Seattle',
                    'state': 'WA',
                    'zipcode': 98109
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'standard_user',
                'username': 'bobsmith',
                'first_name': 'Bob',
                'last_name': 'Smith',
                'age': 18,
                'address': {
                    'road': '3 Madison Lane',
                    'city': 'Louisville',
                    'state': 'KY',
                    'zipcode': 40213
                }
            }
        )
        batch.put_item(
            Item={
                'account_type': 'super_user',
                'username': 'alicedoe',
                'first_name': 'Alice',
                'last_name': 'Doe',
                'age': 27,
                'address': {
                    'road': '1 Jefferson Street',
                    'city': 'Los Angeles',
                    'state': 'CA',
                    'zipcode': 90001
                }
            }
        )

        with table.batch_writer() as batch_2:
            for i in range(50):
                batch_2.put_item(
                    Item={
                        'account_type': 'anonymous',
                        'username': f'user_{i}',
                        'first_name': '???',
                        'last_name': '???'
                    }
                )

        new_entries = [('harry', 'potter'), ('harmonie', 'granger'), ('RONALD', 'WEASLEY!!!')]
        with table.batch_writer(overwrite_by_pkeys=['username', 'last_name']) as batch_3:
            for f_name, l_name in new_entries:
                batch_3.put_item(
                    Item={
                        'username': f_name,
                        'last_name': l_name,
                    }
                )


def query_on_username(table_name, user_name, key='username'):
    """
    demonstrate adding new item to table via DynamoDB.Table.query
    for more on query see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # query for all users who username = user_name
    response = table.query(
        KeyConditionExpression=Key(key).eq(user_name)
    )
    print(response['Items'])


def scan_on_attr(table_name, attr_val, attr='age'):
    """
    demonstrate adding new item to table via DynamoDB.Table.scan
    for more on scan see:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan

    for continuous scanning need boto3.dynamodb.conditions.Key and boto3.dynamodb.conditions.Attr classes
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.scan(
        FilterExpression=Attr(attr).lt(attr_val)
    )
    print(f'users under the {attr} of {attr_val}')
    print(response['Items'])

    response_2 = table.scan(
        FilterExpression=Attr(attr).gt(attr_val)
    )
    print(f'users over the {attr} of {attr_val}')
    print(response_2['Items'])

    response_3 = table.scan(
        FilterExpression=Attr(attr).eq(attr_val)
    )
    print(f'users with {attr} equal to {attr_val}')
    print(response_3['Items'])


def more_scans(table_name, attr_val, attr_val_2, account_type='super_user', attr='first_name',
               attr_2='address.state'):
    """
    more scan examples
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print(f'scan for {attr} beginning with {attr_val} and acccount type is {account_type}')
    response = table.scan(
        FilterExpression=Attr(attr).begins_with(attr_val) & Attr('account_type').eq(account_type)
    )
    print(response['Items'])

    print(f'scan for {attr_2} equal to {attr_val_2}')
    response_2 = table.scan(
        FilterExpression=Attr(attr_2).eq(attr_val_2)
    )
    print(response_2['Items'])


def delete_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    table.delete()


if __name__ == '__main__':
    table_name = input('enter table name to create, update, query, scan, and delete: ')
    try:
        print('\n***\nCreate Table\n***\n')
        create_table_demo(table_name)
        print('\n***\nUse Existing Table\n***\n')
        use_existing_table(table_name)
        print('\n***\nCreate Table\n***\n')
        create_item(table_name)
        print('\n***\nGet Item\n***\n')
        get_item(table_name, 'Homer_Jay', 'Simpson')
        print('\n***\nUpdate Item\n***\n')
        update_item(table_name, 'Homer_Jay', 'Simpson')
        print('\n***\nDelete Item\n***\n')
        try:
            delete_item_demo(table_name, 'Homer_Jay', 'Simpson')
        except Exception as e:
            print('failed to retrieve deleted item (i.e. delete_item works)')
        print('\n***\nWrite Items with batch\n***\n')
        batch_writing(table_name)
        print('\n***\nQuery Table\n***\n')
        query_on_username(table_name, 'johndoe')
        print('\n***\nScan Table\n***\n')
        scan_on_attr(table_name, 25)
        more_scans(table_name, 'J', 'CA')
        more_scans(table_name, 'J', 90001, account_type='standard_user', attr_2='address.zipcode')
    except Exception as e:
        print('an unexpected error occured')
        print(e)
        print(e.__traceback__)
        input('break ')
    finally:
        print(f'\n***\nDelete table {table_name}\n***\n')
        delete_table(table_name)
