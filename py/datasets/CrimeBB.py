#!/usr/bin/python

import pandas as pd
import copy
import time
from .CrimeBB_v1 import CrimeBB_v1
from .CrimeBB_v2 import CrimeBB_v2

import gc

class CrimeBB():
    
    def __init__(self, data_path, year, chunk_size=1000000, database = "CrimeBB", log=False):
        self.YEAR = year
        self.chunk_size = chunk_size
        self.log = log
        
        self.CSV_SUMMARY = f"{data_path}/summary_from_sql/"
        self.CSV_PROCESSED = f"{data_path}/processed_from_csv/"

        self.members_df = None
        self.members_path = f"{self.CSV_SUMMARY}members.csv"
        
        self.boards_df = None
        self.boards_path = f"{self.CSV_SUMMARY}boards.csv"
        
        self.sites_df = None
        self.sites_path = f"{self.CSV_SUMMARY}sites.csv"
        
        self.threads_df = None
        self.threads_path = f"{self.CSV_SUMMARY}threads.csv"
        
        self.posts_df = None
        self.posts_path = f"{self.CSV_SUMMARY}posts.csv"
        
        self.process_time = 0

        if self.YEAR in [2018, 2019]:
            self.version_mode = "v1"
            self.sep = "\\"
            self.crimeBB_processer = CrimeBB_v1()
        
        elif self.YEAR in [2021, 2023]:
            self.version_mode = "v2"
            self.sep = "\\"
            self.crimeBB_processer = CrimeBB_v2()
        else:
            self.version_mode = "v2"
            self.sep = "\\"
            self.crimeBB_processer = CrimeBB_v2()
    
    # Decide how to read a file
    def read_file(self, df_path, read_direct=False):
        if read_direct:
            df = pd.read_csv(df_path, sep=self.sep, low_memory=False, encoding='utf-8', on_bad_lines='skip', lineterminator='\n') # return DataFrame
        else:
            df = pd.read_csv(df_path, sep=self.sep, low_memory=False, iterator=True, encoding='utf-8', on_bad_lines='skip', lineterminator='\n') # Return iterator
        return df
   
    # Read members
    def read_members(self, read_direct=False):
        members_reader = self.read_file(self.members_path, read_direct=read_direct)
        return members_reader
    
    # Read boards
    def read_boards(self, read_direct=False):
        boards_reader = self.read_file(self.boards_path, read_direct=read_direct)
        return boards_reader
    
    # Read sites
    def read_sites(self, read_direct=False):
        sites_reader = self.read_file(self.sites_path, read_direct=read_direct)
        return sites_reader

    # Read threads
    def read_threads(self, read_direct=False):
        threads_reader = self.read_file(self.threads_path, read_direct=read_direct)
        return threads_reader
        
    # Read posts
    def read_posts(self, read_direct=False):
        posts_reader = self.read_file(self.posts_path, read_direct=read_direct)
        return posts_reader
    
    # Read and process members
    def read_and_process_members(self, read_direct=True, chunk_size=1000000):
        start = time.time()
        current_reader = self.read_members(read_direct=read_direct)

        if read_direct:
            print(f"Reading directly ...")
            current_df = copy.deepcopy(current_reader)
            final_df = self.crimeBB_processer.process_members(current_df)

        else:
            print(f"Reading by iterator ...")
            final_df = pd.DataFrame()

            count=0
            len_readed=chunk_size
            while len_readed>=chunk_size:
                current_df = current_reader.get_chunk(chunk_size).copy()
                current_df.drop_duplicates(inplace=True)

                len_readed = current_df.shape[0]
                
                if self.log:
                    print("count:", count, "readed:", len_readed, "chunck size:", chunk_size)

                _df = self.crimeBB_processer.process_members(current_df)
            
                final_df = pd.concat([final_df, _df], ignore_index=True)

                del current_df
                del _df
                gc.collect()
                count+=1
        
        final_df.to_csv(f"{self.CSV_PROCESSED}members.csv", sep=self.sep, index=False)

        self.members_df = copy.deepcopy(final_df)

        del final_df
        gc.collect()
        
        end = time.time()
        self.process_time += (end-start)
    
    # Read and process boards
    def read_and_process_boards(self, read_direct=True, chunk_size=1000000):
        start = time.time()
        current_reader = self.read_boards(read_direct=read_direct)

        if read_direct:
            print(f"Reading directly ...")
            current_df = copy.deepcopy(current_reader)
            final_df = self.crimeBB_processer.process_boards(current_df)

        else:
            print(f"Reading by iterator ...")
            final_df = pd.DataFrame()

            count=0
            len_readed=chunk_size
            while len_readed>=chunk_size:
                current_df = current_reader.get_chunk(chunk_size).copy()
                current_df.drop_duplicates(inplace=True)

                len_readed = current_df.shape[0]
                
                if self.log:
                    print("count:", count, "readed:", len_readed, "chunck size:", chunk_size)

                _df = self.crimeBB_processer.process_boards(current_df)
            
                final_df = pd.concat([final_df, _df], ignore_index=True)

                del current_df
                del _df
                gc.collect()
                count+=1
        
        if self.version_mode == "v1" and self.sites_df is not None:
            final_df = copy.deepcopy(pd.merge(final_df, self.sites_df, how="left", on="site_id"))
            final_df= final_df[["board_id", "site_id", "site_name", "board_title", "board_url"]].copy()
            final_df.drop_duplicates(inplace=True)
        
        if self.version_mode == "v2":
            website_df = final_df[["site_id", "site_name"]].copy()
            website_df.to_csv(self.sites_path, sep=self.sep, index=False)
        
        final_df.to_csv(f"{self.CSV_PROCESSED}boards.csv", sep=self.sep, index=False)

        self.boards_df = copy.deepcopy(final_df)

        del final_df
        gc.collect()
        
        end = time.time()
        self.process_time += (end-start)

    # Read and process sites
    def read_and_process_sites(self, read_direct=True, chunk_size=1000000):
        start = time.time()
        current_reader = self.read_sites(read_direct=read_direct)

        if read_direct:
            print(f"Reading directly ...")
            current_df = copy.deepcopy(current_reader)
            final_df = self.crimeBB_processer.process_sites(current_df)

        else:
            print(f"Reading by iterator ...")
            final_df = pd.DataFrame()

            count=0
            len_readed=chunk_size
            while len_readed>=chunk_size:
                current_df = current_reader.get_chunk(chunk_size).copy()
                current_df.drop_duplicates(inplace=True)

                len_readed = current_df.shape[0]
                
                if self.log:
                    print("count:", count, "readed:", len_readed, "chunck size:", chunk_size)

                _df = self.crimeBB_processer.process_sites(current_df)
            
                final_df = pd.concat([final_df, _df], ignore_index=True)

                del current_df
                del _df
                gc.collect()
                count+=1
        
        final_df.to_csv(f"{self.CSV_PROCESSED}sites.csv", sep=self.sep, index=False)

        self.sites_df = copy.deepcopy(final_df)

        del final_df
        gc.collect()
        
        end = time.time()
        self.process_time += (end-start)

    # Read and process threads
    def read_and_process_threads(self, read_direct=True, chunk_size=1000000):
        start = time.time()
        current_reader = self.read_threads(read_direct=read_direct)

        if read_direct:
            print(f"Reading directly ...")
            current_df = copy.deepcopy(current_reader)
            final_df = self.crimeBB_processer.process_threads(current_df)

        else:
            print(f"Reading by iterator ...")
            final_df = pd.DataFrame()

            count=0
            len_readed=chunk_size
            while len_readed>=chunk_size:
                current_df = current_reader.get_chunk(chunk_size).copy()
                current_df.drop_duplicates(inplace=True)

                len_readed = current_df.shape[0]
                
                if self.log:
                    print("count:", count, "readed:", len_readed, "chunck size:", chunk_size)

                _df = self.crimeBB_processer.process_threads(current_df)
            
                final_df = pd.concat([final_df, _df], ignore_index=True)

                del current_df
                del _df
                gc.collect()
                count+=1
        
        final_df.to_csv(f"{self.CSV_PROCESSED}threads.csv", sep=self.sep, index=False)

        self.threads_df = copy.deepcopy(final_df)

        del final_df
        gc.collect()
        
        end = time.time()
        self.process_time += (end-start)

    # Read and process posts
    def read_and_process_posts(self, read_direct=False, chunk_size=1000000):
        start = time.time()
        current_reader = self.read_posts(read_direct=read_direct)

        if read_direct:
            print(f"Reading directly ...")
            current_df = copy.deepcopy(current_reader)
            final_df = self.crimeBB_processer.process_posts(current_df)

        else:
            print(f"Reading by iterator ...")
            final_df = pd.DataFrame()

            count=0
            len_readed=chunk_size
            while len_readed>=chunk_size:
                current_df = current_reader.get_chunk(chunk_size)#.copy()
                current_df.drop_duplicates(inplace=True)

                len_readed = current_df.shape[0]
                
                if self.log:
                    print("count:", count, "readed:", len_readed, "chunck size:", chunk_size)

                _df = self.crimeBB_processer.process_posts(current_df)
            
                final_df = pd.concat([final_df, _df], ignore_index=True)

                del current_df
                del _df
                gc.collect()
                count+=1
        
        if self.version_mode == "v1" and self.threads_df is not None:
            final_df = copy.deepcopy(pd.merge(final_df, self.threads_df[["site_id", "board_id", "thread_id"]], how="left", on=["site_id", "thread_id"]))
            final_df.drop_duplicates(inplace=True)

            final_df = final_df[["post_id", "site_id", "board_id", "thread_id", "user_id", "username", "user_reputation", "content", "post_data_creation"]].copy()
            final_df.drop_duplicates(inplace=True)

        final_df.to_csv(f"{self.CSV_PROCESSED}posts.csv", sep=self.sep, index=False)

        self.posts_df = copy.deepcopy(final_df)

        del final_df
        gc.collect()
        
        end = time.time()
        self.process_time += (end-start)

    # Summarizing and create file
    def summarize(self):
        start = time.time()
        posts_threads_df = pd.merge(copy.deepcopy(self.posts_df), copy.deepcopy(self.threads_df[["site_id", "board_id", "thread_id", "thread_title"]].drop_duplicates()), on=["site_id", "board_id", "thread_id"], how="left")
        
        posts_threads_boards_df = pd.merge(posts_threads_df, copy.deepcopy(self.boards_df[["site_id", "site_name", "board_id", "board_title"]].drop_duplicates()), on=["site_id", "board_id"], how="left")
        
        crimebb_df = posts_threads_boards_df[['post_id', 'site_id', 'board_id', 'thread_id', 'user_id', 
                                     'site_name', 'board_title', 'thread_title', 'username', 'content', 
                                     'user_reputation', 'post_data_creation']].copy()
        
        end = time.time()
        self.process_time += (end-start)
        
        del posts_threads_boards_df
        del posts_threads_df
        gc.collect()
        
        return crimebb_df

    # return processing time
    def get_process_time(self):
        return self.process_time

    # return members
    def get_members(self):
        return self.members_df

    # return sites
    def get_sites(self):
        return self.sites_df

    # return boards
    def get_boards(self):
        return self.boards_df

    # return threads
    def get_threads(self):
        return self.threads_df
    
    # return posts
    def get_posts(self):
        return self.posts_df
