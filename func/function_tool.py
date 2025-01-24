"""
工具函数用于执行数据库操作，包括创建表、生成16位随机16进制数填充空值、生成雪花码、转换列数据为JSON以及映射并写入数据。

工具函数列表：
1. `create_table_by_sql`：根据SQL语句创建表。
2. `fill_empty_with_hex`：将空值替换为16位随机16进制数。
3. `gen_snowflake_id`：生成雪花码，并将其填入指定的字段中。
4. `convert_list_col_to_json`：将列数据转换为JSON字符串存入指定字段。
5. `map_and_write_data`：按照字典映射关系，将数据写入新数据库表。

依赖库：
- `langchain_core.tools`：工具函数的基础库。
- `pydantic`：用于数据验证和设置默认值。
- `sqlalchemy`：用于数据库连接和操作。
- `sql_func.sql_utils.sql_exec`：执行SQL查询。
- `sql_func.trans_table`：将数据写入新数据表。
- `sql_func.snowflake`：生成雪花码。
- `json`：用于处理JSON数据。
- `pandas`：用于数据处理。

数据模型：
- `CreateTableInput`：创建表输入参数。
- `HexInput`：填充空值输入参数。
- `SnowInput`：生成雪花码输入参数。
- `JsonInput`：转换列数据为JSON输入参数。
- `DictNewTableInput`：映射并写入数据输入参数。

工具函数定义：
- `create_table_by_sql`：根据SQL语句创建表。
- `fill_empty_with_hex`：将空值替换为16位随机16进制数。
- `gen_snowflake_id`：生成雪花码，并将其填入指定的字段中。
- `convert_list_col_to_json`：将列数据转换为JSON字符串存入指定字段。
- `map_and_write_data`：按照字典映射关系，将数据写入新数据库表。

工具函数列表：
- `tools`：包含所有工具函数的列表。
- `tools_dict`：将工具函数映射为字典，键为函数名，值为函数对象。
"""
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

from sql_func.sql_utils.sql_exec import query_exec
from sql_func.trans_table import put_data_into_new_df
from sql_func.snowflake import SnowflakeIDGenerator

import json
import time
import pandas as pd


class CreateTableInput(BaseModel):
    # 省略号用于指示该参数是必需的，不能省略。
    query: str = Field(..., description="用于创建表格的sql语句")
    table_name: str = Field(..., description="表名称")


class HexInput(BaseModel):
    # 省略号用于指示该参数是必需的，不能省略。
    col_name: str = Field(..., description="字段名称")
    table_name: str = Field(..., description="表名称")


class SnowInput(BaseModel):
    # 省略号用于指示该参数是必需的，不能省略。
    col_list: list = Field(..., description="包含字段名称的列表")
    table_name: str = Field(..., description="表名称")


class JsonInput(BaseModel):
    col_list: list = Field(..., description="包含字段名称的列表")
    table_name: str = Field(..., description="表名称")
    json_col_name: str = Field(..., description="存储json的字段名称")


class DictNewTableInput(BaseModel):
    # 省略号用于指示该参数是必需的，不能省略。
    table_name: list = Field(..., description="表格名称组成的列表")
    table_mapping: list = Field(..., description="列表由字典组成，字典中的键值对表示表和字段的对应关系")


@tool(args_schema=CreateTableInput)
def create_table_by_sql(query: str, table_name: str) -> str:
    """根据sql创建表"""
    _ = query_exec(f"DROP TABLE IF EXISTS {table_name};")

    _ = query_exec(f"CREATE TABLE {table_name} AS {query};")

    return f"create table {table_name} done!"


@tool(args_schema=HexInput)
def fill_empty_with_hex(col_name: str, table_name: str) -> str:
    """将空值替换为16位随机16进制数"""
    query = f"""
        UPDATE {table_name}
        SET {col_name} = CASE
        WHEN {col_name} IS NULL OR {col_name} = ""
        THEN LPAD(HEX(FLOOR(RAND() * 0x7FFFFFFFFFFFFFFF)), 16, '0')
        ELSE {col_name}
        END;
    """
    _ = query_exec(query)

    return f"{table_name}--{col_name} hex done!"


@tool(args_schema=SnowInput)
def gen_snowflake_id(col_list: list, table_name: str) -> str:
    """生成雪花码，并将其填入指定的字段中。"""
    # 这里不得不使用pandas再数据库外部来处理数据，读取完整表格进行处理，最后将结果存入数据库
    # 如果直接在数据库中操作 或者是 将部分列提取出数据库处理最后再通过连接的方式存入数据库，速度都非常慢
    def snowflake_id_gen(df, item_list):
        """生成雪花码"""
        # 初始化 SnowflakeID 生成器
        generator = SnowflakeIDGenerator(datacenter_id=1, worker_id=1)
        for item in item_list:
            # 生成雪花码并替换 'id' 列的值
            df[item] = [generator.generate() for _ in range(len(df))]

        # 打印结果
        return df

    while True:
        try:
            engine = create_engine('mysql+pymysql://root:yunyuansheng@172.25.61.3:3306/cmp_e', pool_size=1)

            query = f"SELECT * FROM {table_name}"

            df = pd.read_sql(query, con=engine)

            # 生成雪花码
            df = snowflake_id_gen(df, col_list)

            df.to_sql(
                name=table_name,  # 表名
                con=engine,  # 数据库连接
                if_exists='replace',  # 如果表存在，追加数据
                index=False  # 不写入索引
            )

            return f"{table_name}--{col_list} snowflake done!"

        except Exception as e:
            print(f"数据库连接错误: {str(e)}")
            print("尝试重新连接...")
            time.sleep(1)  # 等待5秒后重新尝试连接


@tool(args_schema=JsonInput)
def convert_list_col_to_json(col_list: list, table_name: str, json_col_name: str) -> str:
    """将col_list中字段对应的值放入字典，转为json字符串存入json_col_name字段"""
    def json_trans(df, list_col):
        """将list_col中包含的列对应的值放入字典，转为json字符串存入instance_params_2字段"""
        for index, row in df.iterrows():
            # 创建一个字典，key为列名，value为该列在这一行对应的值
            json_dict = {col: row[col] for col in list_col}

            # 将字典转换为JSON字符串
            json_str = json.dumps(json_dict)

            # 将JSON字符串赋值给instance_params_2列
            df.at[index, json_col_name] = json_str

        return df

    while True:
        try:
            engine = create_engine('mysql+pymysql://root:yunyuansheng@172.25.61.3:3306/cmp_e', pool_size=1)

            query = f"SELECT * FROM {table_name}"

            df = pd.read_sql(query, con=engine)

            df = json_trans(df, col_list)

            df.to_sql(
                name=table_name,  # 表名
                con=engine,  # 数据库连接
                if_exists='replace',  # 如果表存在，追加数据
                index=False  # 不写入索引
            )

            return f"json done!!!: {table_name}--{col_list}--{json_col_name}"

        except Exception as e:
            print(f"数据库连接错误: {str(e)}")
            print("尝试重新连接...")
            time.sleep(1)  # 等待5秒后重新尝试连接


@tool(args_schema=DictNewTableInput)
def map_and_write_data(table_name: list, table_mapping: list) -> str:
    """按照字典table_mapping中的键值对关系，将table_name中的数据写入新数据库表"""
    def process(table_name, table_mapping):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=engine)
        new_table_dict = put_data_into_new_df(df, table_mapping)
        return new_table_dict

    def write_df_to_sql(df, table_name):
        df.to_sql(
            name=table_name,  # 表名
            con=engine,  # 数据库连接
            if_exists='replace',  # 如果表存在，追加数据
            index=False  # 不写入索引
        )
        print(f"write df to {table_name} done!!!")

    while True:
        try:
            engine = create_engine('mysql+pymysql://root:yunyuansheng@172.25.61.3:3306/cmp_e', pool_size=1)

            df_list = []
            for name, mapping in zip(table_name, table_mapping):
                df_list.append(process(name, mapping))

            # 两个字典合并,后期优化为多个字典合并
            a = set(df_list[0].keys())
            b = set(df_list[1].keys())
            c = a & b
            d1 = a - b
            d2 = b - a
            for key in d1:
                write_df_to_sql(df_list[0].get(key), key)
            for key in d2:
                write_df_to_sql(df_list[1].get(key), key)
            for key in c:
                df_new = pd.concat([df_list[0].get(key), df_list[1].get(key)], axis=0)
                write_df_to_sql(df_new, key)

            return f"mapping and write done!!!"

        except Exception as e:
            print(f"数据库连接错误: {str(e)}")
            print("尝试重新连接...")
            time.sleep(1)  # 等待5秒后重新尝试连接


tools = [create_table_by_sql, fill_empty_with_hex, gen_snowflake_id, convert_list_col_to_json, map_and_write_data]
tools_dict = {func.name: func for func in tools}
