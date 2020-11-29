from multiprocessing import pool
import multiprocessing as mp
import csv
import logging
import yaml
import asyncio
import aioboto3
import click

from tenacity import retry, retry_if_exception, stop_after_attempt, before_log, wait_exponential, after_log
import time

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
deadlock_msg = 'Deadlock found when trying to get lock'


def exception_is_retryable(error: Exception) -> bool:
    logger.debug(error)
    return True
    if hasattr(error, 'response'):
        logger.debug(error.response)
    if (hasattr(error, 'response') and
         'Error' in error.response and
         'Code' in error.response['Error'] and
         'Message' in error.response['Error'] and
         error.response['Error']['Code'] == 'BadRequestException'
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

def create_insert_queries(line_list, schema, table_name, pid):
    send_list = []
    query_list = []
    tups_proced = 0
    for i, tup in enumerate(line_list):
        tups_proced += 1
        send_list.append(tup)
        str_tups = [str(tup) for tup in send_list]
        val_list_str = ','.join(str_tups)
        insert_format = '''
            REPLACE INTO 
            `{}`.`{}`
            VALUES
            {{}}
            '''.format(schema, table_name)
        query = insert_format.format(val_list_str)
        query = query.replace("'NULL'", 'NULL')
        if i % 100000 == 0:
            logger.debug('PID: %s Processed: %s tuples', pid, i)

        if len(query) > INSERT_QUERY_SIZE:
            query_list.append(query)
            send_list = []
    if send_list:
        str_tups = [str(tup) for tup in send_list]
        val_list_str = ','.join(str_tups)
        insert_format = '''
                    REPLACE INTO 
                    `{}`.`{}`
                    VALUES
                    {{}}
                    '''.format(schema, table_name)
        query = insert_format.format(val_list_str)
        query = query.replace("'NULL'", 'NULL')
        query_list.append(query)

    return query_list


def get_create_table_query(column_names, column_types, schema, table_name, conf):
    create_table_col_strs = ''
    for i in range(len(column_names)):
        create_table_col_strs += column_names[i] + ' ' + column_types[i] + ', '
    primary_key = conf['PRIMARY_KEY']
    return create_table_query_format.format(schema, table_name, create_table_col_strs, primary_key)


def multi_process_files(file_paths, conf):
    #create_table_query = [sync_process_files(file_path, conf)[1] for file_path in file_paths][0]
    results = [sync_process_files(file_path, conf)[0] for file_path in file_paths]
    flattened_lines = [line for line_list in results for line in line_list]

    return flattened_lines


def sync_process_files(file_path, conf):
    schema = conf['SCHEMA_NAME']
    table_name = conf['TABLE_NAME']
    col_types = conf['COL_TYPES']
    tuple_list = []
    with open(file_path, mode='r') as f:
        procs = 0
        first_line = True
        for row in csv.DictReader(f, delimiter=','):
            if first_line:
                first_line = False
                column_names = list(row.keys())
                create_table_query = get_create_table_query(column_names, col_types, schema, table_name, conf)
                print(create_table_query)
            else:
                val_list = list(row.values())
                ifields = [i for i in range(len(col_types)) if 'INT' in col_types[i]]
                dfields = [i for i in range(len(col_types)) if col_types[i] == 'DOUBLE' or col_types[i] == 'FLOAT']
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
                if procs % 100000 == 0:
                    logger.debug('In File: %s Processed %s lines', file_path, procs)
    return tuple_list, create_table_query


def get_even_sliced_lists(flat_list, num_partitions):
    num_items = len(flat_list)

    if num_items < num_partitions:
        num_partitions = num_items
    min_items_per_part = num_items // num_partitions
    rem = num_items % num_partitions
    items_for_partition = [flat_list[i* min_items_per_part:(i+1)*min_items_per_part] for i in range(num_partitions)]
    prefilled_size = min_items_per_part * num_partitions
    for i in range(rem):
        items_for_partition[i].append(flat_list[prefilled_size + i])
    return num_partitions, items_for_partition


def read_files(num_cores, file_paths, conf):
    logger.info('Starting to read files')
    num_processors, files_for_core = get_even_sliced_lists(file_paths, num_cores)
    file_proc_args = [(file_list, conf) for file_list in files_for_core]
    with pool.Pool(num_processors) as p:
        line_lists = p.starmap(multi_process_files, file_proc_args)
        return [line for line_list in line_lists for line in line_list]

def create_insert_tuples(num_cores, all_lines, schema, table_name):
    num_processors, lines_for_core = get_even_sliced_lists(all_lines, num_cores)
    line_proc_args = [(lines, schema, table_name, pid) for pid, lines in enumerate(lines_for_core)]
    logger.info('Creating insert queries')
    with pool.Pool(num_cores) as p:
        qlist_procs = p.starmap(create_insert_queries, line_proc_args)
        return qlist_procs

async def run_all_queries(queries):
    async with aioboto3.client('rds-data', region_name='us-west-2') as client:
        return await asyncio.gather(*[run_query(query, client) for query in queries])

def sync_run(conf: dict):
    start_time = time.time()
    file_paths = conf['FILE_PATHS'][9:]
    schema = conf['SCHEMA_NAME']
    table_name = conf['TABLE_NAME']
    num_cores = mp.cpu_count()
    all_lines = read_files(num_cores, file_paths, conf)
    post_lines_time = time.time()
    logger.info('Processed reading lines in %s seconds', post_lines_time - start_time)

    qlist_procs = create_insert_tuples(num_cores, all_lines, schema, table_name)

    query_list_time = time.time()
    flattened_queries = [query for query_list in qlist_procs for query in query_list]

    logger.info('Approxsize of all queries: %s', len(str(flattened_queries)))
    logger.info('Approx num queries: %s', len(flattened_queries))
    logger.info('Processed creating query lists in %s seconds', query_list_time - post_lines_time)

    logger.info('Sending insert requests via rds data-api')
    asyncio.run(run_all_queries(flattened_queries))
    query_send_time = time.time()
    logger.info('Sent queries in %s seconds', query_send_time - query_list_time)
    end = time.time()
    total_seconds = end - start_time
    total_minutes = total_seconds / 60
    total_hours = total_minutes / 60
    if total_hours >= 1:
        logger.info('Total time: %s hours', total_hours)
    elif total_minutes >= 1:
        logger.info('Total time: %s minutes', total_minutes)
    else:
        logger.info('Total time: %s seconds', total_seconds)
    logger.info('Completed insertion of %s rows', len(all_lines))



@click.command()
@click.option("--conf-path", type=str, help="File location")
def main(conf_path: str):

    logger.info('Using conf-path: %s', conf_path)
    conf_data = load_config(conf_path)
    logger.info('Conf data: %s', conf_data)
    sync_run(conf_data)

if __name__ == '__main__':
    main()
