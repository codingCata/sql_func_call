"""
模拟用户输入的查询语句，调用function来完成任务
工具函数定义：
- `route_ds`：根据模型返回信息，路由到相应工具，并调用工具返回结果。
- `exec_`：DeepSeek执行用户提供的查询语句，并调用相应的工具函数。
- `create_table`：创建表格，并执行查询语句。
- `random_hex_gen`：生成16进制随机数，并替换空值。
- `snowflake_id_gen`：生成雪花码，并填入指定的字段中。
- `json_gen`：将指定字段对应的值放入字典，转为JSON字符串存入指定字段。
- `mapping_write`：按照字典映射关系，将数据写入新数据库表。
"""
import json
import time

import func_utils
import function_tool
from sql_func.func.DeepseekSendMessage import send_message_tool

functions = [func_utils.convert_to_ds_function(f) for f in function_tool.tools]

template = [
    "你是一位sql专家，请根据用户提供的查询语句和表名称，将查询结果写入一个新表",
    "你是一位sql专家，请帮助用户完成需求"
]
table_name1 = "sql1_sys"
table_name2 = "sql2_data"


def route_ds(result):
    """
    功能：根据模型返回信息，路由到相应工具，并调用模型返回结果。
    如果没有调用工具，则直接返回模型输出的content内容，
    否则根据调用的工具名，找到对应的工具
    并执行工具的run方法，将工具的参数作为字典传入
    """
    if result.tool_calls is None:
        return result.content
    else:
        print(f"正在调用工具{result.tool_calls[0].function.name}...")
        response = result.tool_calls[0].function.arguments
        response_dict = json.loads(response)
        res = function_tool.tools_dict[result.tool_calls[0].function.name].run(response_dict)
        return res


def exec_(prompt):
    """------DeepSeek执行用户提供的查询语句------"""
    start_time = time.time()
    message = send_message_tool(prompt, functions)
    res = route_ds(message)
    end_time = time.time()
    print(f"{res}\n总耗时：{end_time - start_time}秒")


def create_table():
    """------创建表格------"""
    # sql查询语句,用于创建表格
    query_sys = """
            select 
              '{Utils.getId()}' as sys_disk_id
            , null as sys_disk_no
            , v.display_name as sys_disk_name
            , v.id as sys_disk_uuid
            --
            , '{Utils.getId()}' as instance_id
            --
            , z.region_id as region_id
            , az.az_id as az_id
            , z.cloud_region_id as cloud_region_id
            , az.az_name as az_name
            , az.az_uuid as az_uuid
            , t.account_id as user_id
            , t.account_id as account_id
            , '${defaultResourceGroupId}' as resource_group_id
            ,  t.account_id as resource_group_operator
            , '200-1002' as product_no
            , '块存储-块存储' as product_name
            --
            , null as image_id
            , sbt.storage_block_type_id as sys_disk_type_id
            , null as image_ref
            , null as service_tree_id
            --
            , v.created_at as create_time
            , v.updated_at as update_time
            --
            , cast(v.size as char) as sys_disk_size
            , '4' as type
            , '1' as label
            , va.mountpoint as device
            , null as due_time
            , null as flag
            , null as source_volum_id
            , null as backup_id
            , null as sys_disk_source
            --
            , '{Utils.getId()}' as request_id_2
            , v.display_name as instance_name
            , '4' as type_2
            , '1' as label_2
            , null as instance_params_2
            , v.created_at as create_time_2
            , null as start_time_2
            , null as end_time_2
            , null as time_consume_2
            , null as update_time_2
            , null as expiration_time_2
            , null as payment_type_2
            , null as vpc_id_2
            --
            , '{Utils.getId()}' status_id
            , if(v.status='in-use','running','available') as status_en_3
            , '200' as status_flag_3
            , if(v.status='in-use','bind-success','create-success') as oper_status_en_3
            , v.updated_at as last_update_time_3
            from cinder_e.volumes as v
            left join cinder_e.volume_attachment as va on v.id = va.volume_id
            LEFT JOIN cmp_e.tenant t ON v.user_id = t.i_user_id
            LEFT JOIN cmp_e.storage_block_type sbt ON v.volume_type_id = sbt.storage_block_type_uuid and sbt.cloud_region_id = '7975919571338715136'
            LEFT JOIN cmp_e.available_zone az ON az.az_name = v.availability_zone and az.cloud_region_id = '7975919571338715136'
            LEFT JOIN cmp_e.zone z ON z.az_id = az.az_id  and z.cloud_region_id = '7975919571338715136'


            where v.bootable = 1
            """
    query_data = """
            select
              '{Utils.getId()}' as storage_block_id
            , null as storage_block_no
            , v.display_name as storage_block_name
            , v.id as storage_block_uuid
            --
            , '{Utils.getId()}' as instance_id
            , uuid() as request_no
            --
            , z.region_id as region_id
            , z.az_id as az_id
            , z.cloud_region_id as cloud_region_id
            , az.az_name as az_name
            , az.az_uuid as az_uuid
            , t.account_id as user_id
            , t.account_id as account_id
            , '${defaultResourceGroupId}' as resource_group_id
            , t.account_id as resource_group_operator
            , '200-1002' as product_no
            , '块存储-块存储' as product_name
            --
            , sbt.storage_block_type_id as storage_type_id
            , null as service_tree_id
            --
            , v.created_at as create_time
            , v.updated_at as update_time
            --
            , concat(v.size,'') as storage_block_size
            , 'GB' as storage_block_unit
            , 'false' as wether_share
            , null as disk_model
            , '2' as type
            , '1' as label
            , 'true' as is_default
            , null as portable
            , null as due_time
            , null as flag
            , null as device
            , null as source_volum_id
            , null as backup_id
            , null as speed_limit
            , null as recycle_time
            --
            , '{Utils.getId()}' as request_id_2
            , v.display_name as instance_name
            , '2' as type_2
            , '1' as label_2
            , null as instance_params_2
            , v.created_at as create_time_2
            , null as start_time_2
            , null as end_time_2
            , null as time_consume_2
            , null as update_time_2
            , null as expiration_time_2
            , null as payment_type_2
            , null as vpc_id_2
            --
            , '{Utils.getId()}' status_id
            , if(v.status='in-use','running','available') as status_en_3
            , '200' as status_flag_3
            , if(v.status='in-use','bind-success','create-success') as oper_status_en_3
            , v.updated_at as last_update_time_3
            from cinder_e.volumes as v
            left join cinder_e.volume_attachment as va on v.id = va.volume_id
            LEFT JOIN cmp_e.tenant t ON v.user_id = t.i_user_id
            LEFT JOIN cmp_e.storage_block_type sbt ON v.volume_type_id = sbt.storage_block_type_uuid and sbt.cloud_region_id = '7975919571338715136'
            LEFT JOIN cmp_e.available_zone az ON az.az_name = v.availability_zone and az.cloud_region_id = '7975919571338715136'
            LEFT JOIN cmp_e.zone z ON z.az_id = az.az_id  and z.cloud_region_id = '7975919571338715136'


            where v.bootable = 0
            """

    query_create_table = [
        f"表名称：{table_name1}，查询语句{query_sys}",
        f"表名称：{table_name2}，查询语句{query_data}",
    ]

    for item in query_create_table:
        prompt = [
            {"role": "system", "content": template[0]},
            {"role": "user", "content": item}
        ]
        exec_(prompt)

    print("建表成功!!!")


def random_hex_gen():
    """------生成16进制随机数------"""
    col_list1 = ["sys_disk_name", "instance_name"]
    col_list2 = ["storage_block_name", "instance_name"]

    query_hex = [
        f"将表{table_name1}中的sys_disk_name替换为16进制随机数",
        f"将表{table_name1}中的instance_name替换为16进制随机数",
        f"将表{table_name2}中的storage_block_name替换为16进制随机数",
        f"将表{table_name2}中的instance_name替换为16进制随机数",
    ]
    for item in query_hex:
        prompt = [
            {"role": "system", "content": template[1]},
            {"role": "user", "content": item}
        ]

        exec_(prompt)

    print("16进制数 -- 替换成功!!!")


def snowflake_id_gen():
    """------生成雪花码------"""
    snowflake_sys = ["sys_disk_id", "instance_id", "request_id_2", "status_id"]
    snowflake_data = ["storage_block_id", "instance_id", "request_id_2", "status_id"]
    query_snow = [
        f"生成雪花码，填入表{table_name1}中的字段{snowflake_sys}",
        f"生成雪花码，填入表{table_name2}中的字段{snowflake_data}"
    ]

    for item in query_snow:
        prompt = [
            {"role": "system", "content": template[1]},
            {"role": "user", "content": item}
        ]

        exec_(prompt)

    print("雪花码 -- 生成成功!!!")


def json_gen():
    """------指定字段存入json------"""
    json_col_name = "instance_params_2"
    col_name_sys = ['az_id', 'sys_disk_name']
    col_name_data = ['az_id', 'storage_block_size', 'request_id_2', 'instance_name']
    query_json = [
        f"将表{table_name1}中的字段{col_name_sys}对应的值放入字典,转为json字符串存入字段{json_col_name}",
        f"将表{table_name2}中的字段{col_name_data}对应的值放入字典,转为json字符串存入字段{json_col_name}",
    ]

    for item in query_json:
        prompt = [
            {"role": "system", "content": template[1]},
            {"role": "user", "content": item}
        ]
        exec_(prompt)

    print("json字符串 -- 生成成功!!!")


def mapping_write():
    """------按照字典中的键值对关系，将数据写入对应的数据库------"""
    table_mapping_sys = {
        'sys_disk': ['sys_disk_id', 'sys_disk_no', 'sys_disk_name', 'sys_disk_uuid', 'instance_id', 'region_id',
                     'az_id',
                     'cloud_region_id', 'user_id', 'product_no', 'product_name', 'image_id', 'sys_disk_type_id',
                     'service_tree_id', 'create_time', 'update_time', 'type', 'label', 'device', 'due_time', 'flag',
                     'source_volum_id', 'backup_id', 'sys_disk_source'],
        'status': ['instance_id', 'status_id', 'status_en_3', 'status_flag_3', 'oper_status_en_3',
                   'last_update_time_3'],
        'instance': ['instance_id', 'region_id', 'az_id', 'cloud_region_id', 'user_id', 'account_id',
                     'resource_group_id', 'resource_group_operator', 'product_no', 'product_name', 'service_tree_id',
                     'request_id_2', 'instance_name', 'type_2',
                     'label_2', 'instance_params_2', 'create_time_2', 'start_time_2', 'end_time_2', 'time_consume_2',
                     'update_time_2', 'expiration_time_2', 'payment_type_2', 'vpc_id_2']}
    table_mapping_data = {
        'storage_block': ['storage_block_id', 'storage_block_no', 'storage_block_name', 'storage_block_uuid',
                          'instance_id', 'request_no', 'region_id', 'az_id',
                          'cloud_region_id', 'user_id', 'product_no', 'product_name', 'storage_type_id', 'create_time',
                          'update_time', 'storage_block_size', 'storage_block_unit',
                          'wether_share', 'disk_model', 'type', 'label', 'is_default', 'portable', 'due_time', 'flag',
                          'device', 'source_volum_id', 'backup_id', 'speed_limit', 'recycle_time'],
        'status': ['instance_id', 'status_id', 'status_en_3', 'status_flag_3', 'oper_status_en_3',
                   'last_update_time_3'],
        'instance': ['instance_id', 'region_id', 'az_id', 'cloud_region_id', 'user_id', 'account_id',
                     'resource_group_id',
                     'resource_group_operator', 'product_no', 'product_name', 'service_tree_id', 'request_id_2',
                     'instance_name', 'type_2', 'label_2', 'instance_params_2', 'create_time_2', 'start_time_2',
                     'end_time_2', 'time_consume_2', 'update_time_2', 'expiration_time_2', 'payment_type_2',
                     'vpc_id_2']}
    table_mapping = [table_mapping_sys, table_mapping_data]
    table_name = [table_name1, table_name2]
    query_map_write = f"按照字典中的键值对关系，将{table_name}中的数据写入新数据库表。<dict>{table_mapping}</dict>"

    prompt = [
        {"role": "system", "content": template[1]},
        {"role": "user", "content": query_map_write}
    ]

    exec_(prompt)

    print("写入成功!!!")


if __name__ == "__main__":
    create_table()  # 一个表创建时间20s左右
    random_hex_gen()  # 3s左右
    snowflake_id_gen()  # 20s左右
    json_gen()  # 15s左右
    mapping_write()  # 64s左右
