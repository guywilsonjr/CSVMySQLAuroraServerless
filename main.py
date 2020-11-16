import csv
import logging
import yaml
import asyncio
import aioboto3
from aiofile import LineReader, AIOFile
import click
from tenacity import retry, retry_if_exception, stop_after_attempt, before_log, wait_exponential, after_log

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
cold_start_err_msg = 'Communications link failure'
MAX_QUERY_SIZE = 65536
INSERT_QUERY_SIZE = MAX_QUERY_SIZE // 2
resource_arn = 'arn:aws:rds:us-west-2:936272581790:cluster:data'
secret_arn = 'arn:aws:secretsmanager:us-west-2:936272581790:secret:datacreds-eGCBb0'
create_table_query_format ='''
CREATE TABLE IF NOT EXISTS `{}`.`{}` (
  {}
  PRIMARY KEY ({}));
'''


def exception_is_retryable(error: Exception) -> bool:
    logger.debug(error)
    if hasattr(error, 'response'):
        logger.debug(error.response)
    if (hasattr(error, 'response') and
         'Error' in error.response and
         'Code' in error.response['Error'] and
         'Message' in error.response['Error'] and
         error.response['Error']['Code'] == 'BadRequestException' and
         cold_start_err_msg in error.response['Error']['Message']
    ):
        return True
    return False


@retry(before=before_log(logger, logging.DEBUG),
       after=after_log(logger, logging.DEBUG),
       retry=retry_if_exception(exception_is_retryable),
       stop=stop_after_attempt(7),
       reraise=True, wait=wait_exponential())
async def run_query(query: str, client=None, trans_id: str=None):
    if trans_id:
        return await client.execute_statement(sql=query, resourceArn=resource_arn, secretArn=secret_arn, transactionId=trans_id)
    else:
        return await client.execute_statement(sql=query, resourceArn=resource_arn, secretArn=secret_arn)


def load_config(filepath: str):
    with open(filepath, 'r') as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)
        return conf


async def send_if_sized(client, tuple_list, schema, table_name, always_send=False):
    str_tups = [str(tup) for tup in tuple_list]
    val_list_str = ','.join(str_tups)
    insert_format = '''
    REPLACE INTO 
    `{}`.`{}`
    VALUES
    {{}}
    '''.format(schema, table_name)
    query = insert_format.format(val_list_str)
    query = query.replace("'NULL'", 'NULL')
    if len(query) > INSERT_QUERY_SIZE or always_send:
        await run_query(query, client)
        return True
    return False


async def process_line(line, column_names, col_types, procs, sends, client, tuple_list, schema, table_name):
    for row in csv.DictReader([line], delimiter=',', fieldnames=column_names):
        val_list = list(row.values())
        ifields = [i for i in range(len(col_types)) if col_types[i] == 'INT']
        dfields = [i for i in range(len(col_types)) if col_types[i] == 'DOUBLE']
        bfields = [i for i in range(len(col_types)) if col_types[i] == 'BOOL']
        for ifield in ifields:
            val_list[ifield] = int(val_list[ifield]) if val_list[ifield] else 'NULL'
        for dfield in dfields:
            val_list[dfield] = float(val_list[dfield]) if val_list[dfield] else 'NULL'
        for bfield in bfields:
            val_list[bfield] = bool(val_list[bfield]) if val_list[bfield] else 'NULL'
        vals = tuple(val_list)
        tuple_list.append(vals)
        procs += 1

        if await send_if_sized(client, tuple_list, schema, table_name):
            logger.info('Rows processed:%s \nBatchesUploaded: %s', procs, sends)
            sends += 1
            # Race condition shouldn't exceed half of limit and since it's async only 1 running at a single time
            tuple_list = []
    return procs, sends, tuple_list


def get_create_table_query(column_names, column_types, schema, table_name, conf):
    create_table_col_strs = ''
    for i in range(len(column_names)):
        create_table_col_strs += column_names[i] + ' ' + column_types[i] + ', '
    primary_key = conf['PRIMARY_KEY']
    return create_table_query_format.format(schema, table_name, create_table_col_strs, primary_key)


async def process_file(file_path: str, conf: dict, client):
    schema = conf['SCHEMA_NAME']
    table_name = conf['TABLE_NAME']
    col_types = conf['COL_TYPES']

    async with AIOFile(file_path, mode='r') as f:
        tuple_list = []
        procs = 0
        sends = 0
        first_line = True
        async for line in LineReader(f):
            column_names: list
            if first_line:
                first_line = False
                column_names = line.strip().split(',')
                create_table_query = get_create_table_query(column_names, col_types, schema, table_name, conf)
                await run_query(create_table_query, client)
            else:
                procs, sends, tuple_list = await process_line(line, column_names, col_types, procs, sends, client, tuple_list, schema, table_name)
        if len(tuple_list):
            await send_if_sized(client, tuple_list, schema, table_name, True)
            procs += len(tuple_list)
            sends += 1
            logger.info('For File: %s Rows processed:%s \nBatchesUploaded: %s', file_path, procs, sends)


async def run(conf: dict):
    file_paths = conf['FILE_PATHS']
    async with aioboto3.client('rds-data', region_name='us-west-2') as client:
        await asyncio.gather(*[process_file(file_path, conf, client) for file_path in file_paths])


@click.command()
@click.option("--conf-path", type=str, help="File location")
def main(conf_path: str):
    logger.info('Using conf-path: %s', conf_path)
    conf_data = load_config(conf_path)
    logger.info('Conf data: %s', conf_data)
    asyncio.run(run(conf_data))

if __name__ == '__main__':
    main()
