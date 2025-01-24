from sqlalchemy import create_engine
import pandas as pd

#按照需求创建目的表字典存储sys_disk相关字段
table_mapping_sys = {
    'sys_disk': ['sys_disk_id', 'sys_disk_no', 'sys_disk_name', 'sys_disk_uuid', 'instance_id', 'region_id', 'az_id',
                 'cloud_region_id', 'user_id', 'product_no', 'product_name', 'image_id', 'sys_disk_type_id',
                 'service_tree_id', 'create_time', 'update_time', 'type', 'label', 'device', 'due_time', 'flag',
                 'source_volum_id', 'backup_id', 'sys_disk_source'],
    'status': ['instance_id', 'status_id', 'status_en_3', 'status_flag_3', 'oper_status_en_3', 'last_update_time_3'],
    'instance': ['instance_id', 'region_id', 'az_id', 'cloud_region_id', 'user_id', 'account_id',
                 'resource_group_id', 'resource_group_operator', 'product_no', 'product_name', 'service_tree_id',
                 'request_id_2', 'instance_name', 'type_2',
                 'label_2', 'instance_params_2', 'create_time_2', 'start_time_2', 'end_time_2', 'time_consume_2',
                 'update_time_2', 'expiration_time_2', 'payment_type_2', 'vpc_id_2']}

#创建第二个字典存储storage_block相关字段
table_mapping_data = {
    'storage_block': ['storage_block_id', 'storage_block_no', 'storage_block_name', 'storage_block_uuid', 'instance_id', 'request_no', 'region_id', 'az_id',
                      'cloud_region_id', 'user_id', 'product_no', 'product_name', 'storage_type_id', 'create_time', 'update_time', 'storage_block_size', 'storage_block_unit',
                      'wether_share', 'disk_model', 'type', 'label', 'is_default', 'portable', 'due_time', 'flag',
                      'device',  'source_volum_id', 'backup_id', 'speed_limit', 'recycle_time'],
    'status': ['instance_id', 'status_id', 'status_en_3', 'status_flag_3', 'oper_status_en_3', 'last_update_time_3'],
    'instance': ['instance_id', 'region_id', 'az_id', 'cloud_region_id', 'user_id', 'account_id', 'resource_group_id',
                 'resource_group_operator', 'product_no', 'product_name', 'service_tree_id', 'request_id_2',
                 'instance_name', 'type_2', 'label_2', 'instance_params_2', 'create_time_2', 'start_time_2',
                 'end_time_2', 'time_consume_2', 'update_time_2', 'expiration_time_2', 'payment_type_2', 'vpc_id_2']}


def trans_df(df, table_name):
    """将dataframe写入table_name数据库表。"""
    engine = create_engine('mysql+pymysql://root:yunyuansheng@172.25.61.3:3306/cmp_e', pool_pre_ping=True)
    # 将数据写入数据库对应的表
    df.to_sql(
        name=table_name,  # 表名
        con=engine,  # 数据库连接
        if_exists='replace',  # 如果表存在，追加数据
        index=False  # 不写入索引
    )

    print(f"Data has been inserted into table '{table_name}'.")


def put_data_into_new_df(df, table_mapping):
    """按照字典中的键值对关系，将数据写入对应的dataframe。"""
    # 创建一个字典来存储各个表格的DataFrame
    tables = {}

    # 遍历字典，将字段插入对应的表格
    for table_name, column_list in table_mapping.items():
        if table_name not in tables:
            tables[table_name] = pd.DataFrame()
        for column in column_list:
            tables[table_name][column] = df[column]

    return tables
