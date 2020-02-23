## This file creates clone of production objects to enable CI/CD pipeline on top of those.
## Ensure you have installed snowflake connector for python
## Also ensure you have the config file - Snowflake_CICD.config in same repoistory 
import snowflake.connector
import sys
pwd = str(sys.argv[1])
# Gets the version
ctx = snowflake.connector.connect(
    user='',
    password=pwd,
    account=''
    )
cs = ctx.cursor()
cp = ctx.cursor()

with open("Snowflake_CICD.config", 'r') as fp:
    for line in fp:
        if line.startswith("clone"):
            try:
                table = line.split("-")[1].strip()[1:-1]
                # print(table)
                cs.execute("use role CICD_ROLE")
                cs.execute("use warehouse CICD_WH")
                print("drop table if exists DEV.EDW" + table)
                cs.execute("drop table if exists DEV.EDW." + table)
                print("create table EDWDEV.EDW_PROD." + table + " clone PRO.EDW." + table)
                cs.execute("create table EDWDEV.EDW_PROD." + table + " clone PRO.PRO." + table)
            except snowflake.connector.errors.ProgrammingError as e:
                print("ABORT:- clone step failed")
                raise Exception("ABORT:- clone step failed")
            print(table + " cloned from PRO to DEV")
        elif line.startswith("oneshot"):
            try:
                filename = line.split("-")[1].strip()[1:-1]
                cs.execute("use role CICD_ROLE")
                cs.execute("use warehouse CICD_WH")
                cs.execute("use database DEV")
                with open(filename, 'r') as add_column_file:
                    lines = (line.rstrip() for line in add_column_file)  # All lines including the blank ones
                    lines = (line for line in lines if line)
                    for line in lines:
                        print(line)
                        cs.execute(line)
            except snowflake.connector.errors.ProgrammingError as e:
                print("ABORT: oneshot execution failed - build failed")
                raise Exception("ABORT: oneshot execution failed - build failed")
            print("oneshot- "+filename+" executed with success")
        elif line.startswith("source_code"):
            try:
                sourcecode1 = line.split("-")[1].strip()[1:-1]
                cs.execute("use role CICD_ROLE")
                cs.execute("use warehouse CIDCD_WH")
                cs.execute("use database DEV")
                fd = open(sourcecode1, 'r')
                sqlfile = fd.read()
                fd.close()
                sqlcmd = sqlfile.split(';')
                for command in sqlcmd:
                    cs.execute(command)
                    print(command)
                    one_row = cs.fetchone()
                    print(one_row)
            except snowflake.connector.errors.ProgrammingError as e:
                print(sourcecode1 + "-ABORTED. build failed")
                raise Exception("ABORT: source file execution failed - build failed")
            print(sourcecode1 + "-executed successfully")
            cs.close()
            cp.close()
ctx.close()
