import csv
import logging
import asyncio
import aioboto3
import aiofiles
from aiocsv import AsyncReader, AsyncDictReader
from tenacity import retry, retry_if_exception, stop_after_attempt, before_log, wait_exponential, after_log

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
cold_start_err_msg = 'Communications link failure'
MAX_QUERY_SIZE = 65536
INSERT_QUERY_SIZE = MAX_QUERY_SIZE // 2
resource_arn = 'arn:aws:rds:us-west-2:936272581790:cluster:data'
secret_arn = 'arn:aws:secretsmanager:us-west-2:936272581790:secret:datacreds-eGCBb0'

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

columns = []
file_path = '/home/guy/Downloads/nfl-big-data-bowl-2021/plays.csv'
ifields = [3,4,5, 9, 12,13,16,17,19,23,24]
dfields = [25]
bfields =[26]
column_types = [
    'VARCHAR(20)',
    'VARCHAR(20)',
    'VARCHAR(1023)',
    'INT',
    'INT',
    'INT',
    'VARCHAR(10)',
    'VARCHAR(63)',
    'VARCHAR(63)',
    'INT',#YARDLINENUMBER
    'VARCHAR(63)',
    'VARCHAR(63)',#
    'INT',
    'INT',
    'VARCHAR(63)',#
    'VARCHAR(63)',#DROP
    'INT',
    'INT',
    'VARCHAR(63)',#CLOCK
    'INT',
    'VARCHAR(63)',#PCODE
    'VARCHAR(63)',
    'VARCHAR(10)',
    'INT',
    'INT',#PRES
    'DOUBLE',
    'BOOL'
]
schema = 'bdb'
table_name = 'plays'
create_table_query_format =\
'''
CREATE TABLE IF NOT EXISTS `{}`.`{}` (
  {}
  PRIMARY KEY ({}));
'''
insert_format ='''
REPLACE INTO 
`{}`.`{}`
VALUES
{{}}
'''.format(schema, table_name)

async def send_if_sized(client, tuple_list, always_send=False):
    str_tups = [str(tup) for tup in tuple_list]
    val_list_str = ','.join(str_tups)
    query = insert_format.format(val_list_str)

    if len(query) > INSERT_QUERY_SIZE:
        await run_query(query, client)
        return True
    return False


async def process_line(line, column_names, procs, sends, client, tuple_list):
    for row in csv.DictReader([line], delimiter=',', fieldnames=column_names):
        val_list = list(row.values())
        for ifield in ifields:
            val_list[ifield] = int(val_list[ifield]) if val_list[ifield] else -1
        for dfield in dfields:
            val_list[dfield] = float(val_list[ifield])
        for bfield in bfields:
            val_list[bfield] = bool(val_list[ifield])
        vals = tuple(val_list)
        tuple_list.append(vals)
        procs += 1
        print(procs, sends)
        if await send_if_sized(client, tuple_list):
            sends += 1
            # Race condition shouldn't exceed half of limit and since it's async only 1 running at a single time
            tuple_list = []
    return procs, sends, tuple_list

async def scan_file_for_col_sizes():
    print('Scanning')
    async with aiofiles.open(file_path, mode="r", encoding="utf-8", newline="") as afp:
        for line in await afp.readlines():
            async for row in AsyncDictReader([line], delimiter=','):
                print(row)


async def run():
    async with aiofiles.open(file_path, mode='r') as f:
        columns_str = await f.readline()
        column_names = columns_str.strip().split(',')
        create_table_col_strs = ''
        for i in range(len(column_names)):
            create_table_col_strs += column_names[i] + ' ' + column_types[i] +', '

        primary_key = ','.join([column_names[0], column_names[1]])

        create_table_query = create_table_query_format.format(schema, table_name, create_table_col_strs, primary_key)

        tuple_list = []
        procs = 0
        sends = 0
        async with aioboto3.client('rds-data', region_name='us-west-2') as client:
            await run_query(create_table_query, client)
            async for line in f:
                procs, sends, tuple_list = process_line(line, column_names, procs, sends, client, tuple_list)

            if len(tuple_list):
                await send_if_sized(client, tuple_list, True)



asyncio.run(scan_file_for_col_sizes())
