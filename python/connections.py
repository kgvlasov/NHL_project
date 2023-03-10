from airflow.hooks.clickhouse_hook import ClickhouseHook

def get_clickhouse_client():
    clickhouse_conn_id = 'clickhouse'
    ch_hook = ClickhouseHook(clickhouse_conn_id)
    print(ch_hook)
    try:
        client_ch = ch_hook.get_conn()
        return client_ch
    except:
        raise Exception('Trouble with connection: ' + clickhouse_conn_id)
