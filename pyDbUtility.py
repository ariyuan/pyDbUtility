from subprocess import Popen, PIPE
import argparse
import os


sql_plus_fullpath = r"C:\GitHub\pyDbUtility\instantclient_12_1\sqlplus"


def run_sql_query(sql_command, connect_string):
    session = Popen([sql_plus_fullpath, '-S', connect_string], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(sql_command)
    return session.communicate()


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
    args = parser.parse_args()
    if args.path is not None:
        sql_plus_fullpath = args.path
    if args.jenkins is True:
        # Get connection string from Jenkins
        conn_str = get_conn_str_from_jenkins()
    else:
        # Get connection string from args
        conn_str = args.connStr
    queryResult, errorMessage = run_sql_query(args.query, conn_str)

    # sqlCommand = 'select * from dba_directories;'
    print queryResult
