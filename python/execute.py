from python import connections
def execute_clickhouse(sql):
    clickhouse_client = connections.get_clickhouse_client()
    print(clickhouse_client)
    result = clickhouse_client.execute(sql)
