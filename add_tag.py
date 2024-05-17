import boto3
import pandas as pd
import numpy as np


# jingamz@ add tag
def check_tag(client, arn, tag):
    tag = tag
    response = client.get_resources(
        ResourceARNList=[
            arn,
        ],
        TagFilters=[]
    )
    if not response['ResourceTagMappingList']:
        tag_flag = True
        print('Arn error not in this account:', arn)
        return tag_flag

    tags = response['ResourceTagMappingList'][0]['Tags']
    if tag not in tags:
        # need add tag
        tag_flag = False
        print('Need add tag')
    else:
        # no need add tag
        tag_flag = True
        print('No need add tag')
    return tag_flag


def add_tag(client, arn, add_tags):
    response = client.tag_resources(
        ResourceARNList=[
            arn,
        ],
        Tags=add_tags
    )

    if not response['FailedResourcesMap']:
        print('add tag successfully')
        isSuccess = True
    else:
        print('add tag', response['FailedResourcesMap'][arn]['ErrorCode'])
        isSuccess = False
    return isSuccess


def boto3_client(region):
    client = boto3.client('resourcegroupstaggingapi', region_name=region)
    return client


def load_csv_data(file_path_name):
    dataframe = pd.read_csv(file_path_name)
    return dataframe


def map_list(dataframe):
    dataframe = dataframe.drop_duplicates(['product_product_name'])
    product_name = dataframe.drop(columns=['line_item_resource_id', 'product_region'])
    product_name.insert(loc=len(product_name.columns), column='map', value=0)

    # Services Not in MAP
    # 1: means not in  MAP
    # 0: means in MAP
    product_name['map'] = np.where(
        (product_name['product_product_name'].str.contains('Amazon MemoryDB')) |
        (product_name['product_product_name'].str.contains('Amazon QuickSight')) |
        (product_name['product_product_name'].str.contains('Claude'))
        , 1, product_name['map'])
    return product_name


def add_map_flag(original_dataframe, map_list):
    # Add map flag for every arn
    Final_result = pd.merge(original_dataframe, map_list, on=['product_product_name'], how='left')
    return Final_result


if __name__ == "__main__":
    # 导入 CUR 中生成的数据
    # 122a1df4-39c1-487d-932c-92242783e505.csv
    # df = load_csv_data('/Users/jingamz/Downloads/604f694b-7a04-4c65-9256-02148a52a7bc.csv')
    df = load_csv_data('/home/ec2-user/tags/604f694b-7a04-4c65-9256-02148a52a7bc.csv')
    # print(df.head())

    # MAP=='Y' list
    product_name = map_list(df)
    Final_result = add_map_flag(df, product_name)

    # 循环添加 Tag
    client = boto3_client('us-west-2')
    for row in Final_result.index:
        if Final_result.loc[row]['map'] == 0 and Final_result.loc[row]['product_region'] == 'us-west-2':
            print(Final_result.loc[row]['line_item_resource_id'], Final_result.loc[row]['product_region'])
            # Check Tag Mock speed!
            # arn = 'arn:aws:athena:us-east-1:890717383483:workgroup/primary'
            arn = Final_result.loc[row]['line_item_resource_id']
            add_tags = {
                'xxxx': 'yyyyy'
            }
            tag = {'Key': 'xxxx', 'Value': 'yyyyy'}
            region = Final_result.loc[row]['product_region']
            # client = boto3_client(region=region)
            if check_tag(client, arn, tag):
                continue
            else:
                add_tag(client, arn, add_tags)
        else:
            continue
