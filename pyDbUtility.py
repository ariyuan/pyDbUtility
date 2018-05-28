from subprocess import Popen, PIPE
import argparse
import os
import json
import xlrd
from Utilities import FileUtility


sql_plus_fullpath = r"C:\GitHub\pyDbUtility\instantclient_12_1\sqlplus"
impdp_fullpath = r"C:\GitHub\pyDbUtility\instantclient_12_1\impdp"
db_config = None
dumps_to_restore = []


def read_db_configuration():
    global db_config
    db_config = json.load(open("dbconfig.json"))
    print db_config['oracle']


def run_sql_query(sql_command, connect_string):
    session = Popen([sql_plus_fullpath, '-S', connect_string], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(sql_command)
    return session.communicate()


def get_dumps_to_restore():
    global dumps_to_restore
    sheet = xlrd.open_workbook('DumpList.xls').sheet_by_index(0)
    for i in range(sheet.nrows):
        if i != 0:
            print sheet.row_values(i)
            dumps_to_restore.append(sheet.row_values(i))


def parse_dump_information():
    pass


def copy_dump_to_db_filesystem():
    pass


def restore_dump(connect_string):
    for ele in dumps_to_restore:
        if ele[0] == "Y":
            if ele[1].lower() == "Oracle".lower():
                my_env = os.environ.copy()
                my_env["ORACLE_HOME"] = os.path.dirname(impdp_fullpath)
                remap_schema = "remap_schema={0}:{1}".format(ele[4], ele[5])
                remap_tablespace = "remap_tablespace={0}:{1}".format(ele[3], db_config['oracle']['tablespace'])
                directory = "directory={0}".format(db_config['oracle']['directory_in_db'])
                dumpfile = "dumpfile={0}".format(ele[2])
                session = Popen([impdp_fullpath, connect_string, remap_schema, remap_tablespace, directory, dumpfile], stdout=PIPE, env=my_env)
                print session.stdout.readline()


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
    parser.add_argument('-r', '--restore', action='store_true',
                        help='Restore dumps from DumpList.xls')
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
    # Debug
    read_db_configuration()
    get_dumps_to_restore()
    restore_dump(conn_str)
    # End of Debug
    queryResult, errorMessage = run_sql_query(args.query, conn_str)

    # sqlCommand = 'select * from dba_directories;'
    print queryResult
