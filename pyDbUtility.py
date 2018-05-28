from subprocess import Popen, PIPE
import argparse
import os
import time


sql_plus_fullpath = r"C:\GitHub\pyDbUtility\instantclient_12_1\sqlplus"


def run_sql_query(sql_command, connect_string):
    session = Popen([sql_plus_fullpath, '-S', connect_string], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(sql_command)
    return session.communicate()


def get_oracle_active_session(schema_name):
    list = []
    queryResult, errorMessage = run_sql_query("SELECT s.sid, s.serial# FROM v$session s, v$process p WHERE s.username = '{0}' AND p.addr(+) = s.paddr;".format(schema_name), conn_str)
    queryResult = queryResult.strip()
    print queryResult
    if queryResult.strip() != "no rows selected":
        queryResult = queryResult.split('\n')
        for line in queryResult:
            if line.strip().startswith("SID") or line.strip().startswith("---"):
                pass
            else:
                list.append({line.strip().split('\t')[0]: line.strip().split('\t')[1]})
    return list


def delete_oralce_schema(schema_name):
    list = get_oracle_active_session(schema_name)
    if list.__len__()>0:
        for element in list:
            run_sql_query("ALTER SYSTEM KILL SESSION '{0}, {1}';".format(element.keys()[0], element.values()[0]), conn_str)
    run_sql_query("DROP USER " + schema_name + " CASCADE;", conn_str)


def get_conn_str_from_jenkins():
    db_server_address = os.getenv("dbServerAddress")
    db_service_name = os.getenv("servicename")
    db_port = os.getenv("dbPort")
    db_username = os.getenv("dbaUser")
    db_password = os.getenv("dbaPassword")
    if os.getenv("dbType") == "oracle":
        db_connection_string = "{0}/{1}@{2}:{3}/{4}".format(db_username, db_password, db_server_address, db_port, db_service_name)
    else:
        # connection string for sql - to be implemented
        db_connection_string = None
    return db_connection_string

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Utility for running simple database query')
    parser.add_argument('-j', '--jenkins', action='store_true', help='Run with Jenkins job, parameters will get from environment variables')
    parser.add_argument("-c", '--connStr', help="Connection string that used for connecting db")
    parser.add_argument("-q", '--query', help="Query to be run")
    parser.add_argument("-p", '--path', help="Full path of sqlplus execution file, oracle only")
    parser.add_argument("-d", '--deleteSchema', help="Delete specific schema from Oracle db")
    args = parser.parse_args()
    if args.path is not None:
        sql_plus_fullpath = args.path
    if args.jenkins is True:
        # Get connection string from Jenkins
        conn_str = get_conn_str_from_jenkins()
    else:
        # Get connection string from args
        conn_str = args.connStr
    if args.query is not None:
        queryResult, errorMessage = run_sql_query(args.query, conn_str)
        print queryResult
    if args.deleteSchema is not None:
        delete_oralce_schema(args.deleteSchema)

    # sqlCommand = 'select * from dba_directories;'

