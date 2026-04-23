#!/usr/bin/python

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
import os
import glob
import re
import pandas as pd
import numpy as np
import subprocess

from py.utils import verifyDir
from .config import POSGRES_Config

class SQLManager():
    def __init__(self, config_file=None):
        
        posgres_obj = POSGRES_Config()
        self.config_file = dict((name.replace("POSGRES_", "").lower(), getattr(posgres_obj, name)) for name in dir(posgres_obj) if not name.startswith('__'))
        
        self.POSGRES_PSQLPASSWORD = 123456
        self.avoid_db = ['postgres', 'template1', 'template0']
        
    def db_connection(self, db_name=None, log=False):
        #Define our connection string
        if db_name is not None:
          conn_string = f"host={self.config_file['host']} user={self.config_file['user']} password={self.config_file['password']} database={db_name}"
          self.config_file["database"] = db_name
        else:
          conn_string = f"host={self.config_file['host']} user={self.config_file['user']} password={self.config_file['password']}"
          self.config_file["database"] = 'postgres'

        # print the connection string we will use to connect
        if log:
          print ("Connecting to database\n ->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(**self.config_file)
        #conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
        
        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        if log:
          print( "Connected!\n")
        
        return conn
    
    
    def createDB(self, db_name):
        conn = self.db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
        cursor.execute(query)
        print("Creating", db_name, ". . .")

        cursor.close()
        conn.close()
        print("Conn closed", conn.closed)
    
    
    def createDBs(self, list_dbs):
        conn = self.db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        list_current_dbs = self.listDBs()
        
        for db_name in list_dbs:
          if db_name not in list_current_dbs:
              query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
              cursor.execute(query)
              print("Creating", db_name, ". . .")

        cursor.close()
        conn.close()
        print("Conn closed", conn.closed)


    def deleteDBs(self, list_dbs):
        conn = self.db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        list_current_dbs = self.listDBs()
        
        for db_name in list_dbs:
          #if (db_name,) in list_current_dbs:
          if db_name in list_current_dbs:
              query = sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name))
              cursor.execute(query)
              print("Deleting", db_name, ". . .")

        cursor.close()
        conn.close()
        print("Conn closed", conn.closed)


    def listDBs(self):
        conn = self.db_connection()
        cursor = conn.cursor()

        query = "SELECT datname FROM pg_database;"
        cursor.execute(query)

        list_database = cursor.fetchall()
        cursor.close()
        conn.close()
        print("Conn closed", conn.closed)
        
        list_database = [db[0] for db in list_database]
        
        list_database = [db for db in list_database if db not in self.avoid_db]
        
        return list_database
        

    def listDBtables(self, list_dbs, log=False):
        tables_dict = {}

        for db_name in list_dbs:
            conn = self.db_connection(db_name=db_name, log=log)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            #query = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            cursor.execute(query)

            table_names = [record[0] for record in cursor.fetchall()]
            
            tables_dict[db_name] = table_names
            
            cursor.close()
            conn.close()

        return tables_dict
          
    
    def getTableSize(self, list_dbs):
        for db_name in list_dbs:
          conn = self.db_connection(db_name=db_name)
          conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
          cursor = conn.cursor()

          query = """SELECT relname, 
                          num_elements, 
                          pg_size_pretty(size_bytes) AS size_Mb,  
                          size_bytes 
                          FROM ( 
                            SELECT pg_catalog.pg_namespace.nspname AS schema_name, 
                                   relname, 
                                   pg_relation_size(pg_catalog.pg_class.oid) AS size_bytes, 
                                   reltuples::bigint AS num_elements 
                                   FROM pg_catalog.pg_class 
                                JOIN pg_catalog.pg_namespace 
                                ON relnamespace = pg_catalog.pg_namespace.oid  
                       ) t  
                  WHERE schema_name NOT LIKE 'pg_%' and schema_name='public' 
                  ORDER BY size_bytes DESC;"""
          
          cursor.execute(query)
          colnames = [desc[0] for desc in cursor.description]
          query_results = cursor.fetchall()
          df = pd.DataFrame(query_results, columns=colnames)
          print("db:", db_name, "tables:")
          print(df)

          cursor.close()
          conn.close()

    def getDBsSize(self, list_dbs=None):
        conn = self.db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        db_dict = {"db_name":[], "size": []}

        if not list_dbs:
            list_dbs = self.listDBs()

        for db_name in list_dbs:
            query = f"select pg_size_pretty( pg_database_size('{db_name}') );"
            cursor.execute(query)
            row = cursor.fetchone()
            db_dict["db_name"].append(db_name)
            db_dict["size"].append(str(row[0]))
            print("db:", db_name, "size:", str(row[0]))

        cursor.close()
        conn.close()
        
        return db_dict
    
    
    def get_db_names(self, path):
        path = path.replace("//", "/")
        current_year = path.split("/")[-3]

        files_path = glob.glob(path)
        files_path = np.sort(files_path).tolist()

        files_name = []
        for file_l in files_path:
            file_ = file_l.split("/")[-1].lower()
            file_ = file_.replace(".sql.gz", "")
            file_ = file_.replace(".zip", "")
            file_ = file_.replace(".sql", "")
            file_ = file_.replace("crimebb-", "")
            file_ = file_.replace("crimebb_", "")
            file_ = re.sub(r'[_-]*[0-9]{4}-[0-9]{2}-[0-9]{2}[_-]*', '', file_, flags=re.IGNORECASE)
            files_name.append(f"crimebb_{current_year}_{file_}")
        #files_name = [file.replace("../data/sql/crimebb-","").replace(f"{date}.sql","") for file in files_path]
    #    files_name = [(file_.split("/"))[-1].replace("crimebb-","").replace(f".sql","") for file_ in files_path]
        
        zip_iterator = zip(files_name, files_path)
        files_dict = dict(zip_iterator)
        
        #files_path = [f"{Path.cwd().as_posix()}/{file_}" for file_ in files_path]
        
        return files_dict
    
    
    def user_exists(self, username):
        conn = self.db_connection()
        cursor = conn.cursor()

        query = "SELECT 1 FROM pg_roles WHERE rolname = %s;"
        cursor.execute(query, (username,))
        exists = cursor.fetchone() is not None

        cursor.close()
        conn.close()

        return exists


    def cmd_create_user(self, username="crimebb", password="crimebb", db_name="postgres", time_to_sleep=5):
        passwd = self.POSGRES_PSQLPASSWORD

        if self.user_exists(username):
            print(f"User '{username}' already exists. Skipping creation.\n")
            return

        print(f"Creating user '{username}' ...")

        commands = [
            f"CREATE USER {username} WITH PASSWORD '{password}';",
            f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {username};",
            f"ALTER ROLE {username} SUPERUSER;",
        ]

        for cmd in commands:
            command_os = f'su -l postgres -c "psql -c \\"{cmd}\\""'
            print("Executing command ... ", command_os)
            result = subprocess.run(
                command_os,
                input=f"{passwd}\n",
                shell=True,
                text=True,
                capture_output=True
            )
            if result.returncode != 0:
                print(f"cmd_create_user error: {result.stderr}")
            time.sleep(time_to_sleep)

        print(f"User '{username}' created and configured.\n")


    def cmd_dir_permission(self, passwd="", time_to_sleep=5):
        command_os = "sudo -S setfacl -R -m u:postgres:rwx /media/felipe"
        result = subprocess.run(
            command_os,
            input=f"{passwd}\n",
            shell=True,
            text=True,
            capture_output=True
        )
        if result.returncode != 0:
            print(f"cmd_dir_permission error: {result.stderr}")
        time.sleep(time_to_sleep)
    
    
    def cmd_db_restore(self, db_name, db_path, time_to_sleep=30):
        passwd = self.POSGRES_PSQLPASSWORD
        print(f"Backuping ... {db_name}")
        
        #input_path=f"{os.getcwd()}/{db_path}/"
        
        command_psql = f"psql -d {db_name} -f {db_path}"
        command_os = f'su -l postgres -c "{command_psql}"'
        
        #"su -l postgres" #can be any command but don't forget -S as it enables input from stdin
        #os.system(f"psql -d {db_name} -a -f {dict_dbs[db_name]}")

        result = subprocess.run(
            command_os,
            input=f"{passwd}\n",
            shell=True,
            text=True,
            capture_output=True
        )
        if result.returncode != 0:
            print(f"cmd_db_restore error: {result.stderr}")

        db_size = self.getDBsSize(list_dbs=[db_name])
        size_str = db_size["size"][0] if db_size["size"] else "unknown"
        print(f"RESTORED DB {db_name} FINISHED. Size: {size_str}\n")
        #os.system("")
        
    
    def cmd_table_to_csv(self, db_name, table_name, csv_path, time_to_sleep=10):
        passwd = self.POSGRES_PSQLPASSWORD
        print(f"Table to CSV ... {db_name}-{table_name}")

        #out_path=f"{os.getcwd()}/{csv_path}/"
        verifyDir(csv_path)
        out_name = f"{csv_path}{table_name}.csv".replace("//", "/")

        #psql -d antichat-2021-01-10 -c "\copy boards to filename.csv csv header"
        command_psql = rf"\copy {table_name} to {out_name} csv header;"

        command_os = f'su -l postgres -c "psql -d {db_name} -c \'{command_psql}\'"'
        print("Excecuting command ... ", command_os)

        result = subprocess.run(
            command_os,
            input=f"{passwd}\n",
            shell=True,
            text=True,
            capture_output=True
        )
        if result.returncode != 0:
            print(f"cmd_table_to_csv error: {result.stderr}")
        else:
            print(f"DB to CSV {db_name} FINISHED.\n")
        time.sleep(time_to_sleep)
        
    
    def convert_to_gb(self, row):
        if row["str_size"].lower()=="kb":
            return row["num_size"]
        elif row["str_size"].lower()=="mb":
            return row["num_size"]*1000
        elif row["str_size"].lower()=="gb":
            return row["num_size"]*1000000
        
        
    def plot_db_sizes(self, db_dict):
        db_df = pd.DataFrame(data=db_dict)
        db_df["num_size"] = db_df["size"].apply(lambda x: int(x.split(" ")[0]))
        db_df["str_size"] = db_df["size"].apply(lambda x: x.split(" ")[1])
        
        db_df["val_size"] = db_df.apply(lambda row: self.convert_to_gb(row), axis=1)
        
        db_df.set_index("db_name", inplace=True)
        db_df.sort_values(by="val_size", ascending=False, inplace=True)
        
        return db_df
