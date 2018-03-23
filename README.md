This is a small utility for execution sql commands

Usage:
python pyDbUtility.py -c username/password@oracle_hostname:1521/sid -q "drop user "User_to_drop" CASCADE;" -p "C:\GitHub\pyDbUtility\instantclient_12_1\sqlplus"

Jenkins Integration:
Pass following paramer to script from Jenkins
dbServerAddress
servicename
dbPort
dbaUser
dbaPassword
dbType
