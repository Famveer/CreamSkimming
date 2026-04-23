#!/usr/bin/python

class CrimeBB_v2():
    
    def __init__(self):
        pass

    # Process members
    def process_members(self, members_df):
        members_df.drop_duplicates(inplace=True)
        
        members_df = members_df[["id", "username", "site_id"]].copy().drop_duplicates()
        members_df.rename(columns={"id":"user_id"}, inplace=True)
        members_df.drop_duplicates(inplace=True)

        return members_df

    # Process sites
    def process_sites(self, website_df):
        
        if "site_id" not in website_df.columns:
            website_df.rename(columns={"id":"site_id"}, inplace=True)
    
        website_df.drop_duplicates(inplace=True)
        website_df.sort_values(by="site_id", inplace=True)
        
        return website_df
    
    # Process boards
    def process_boards(self, boards_df):
        boards_df["url"] = boards_df["url"].apply(lambda x: x.replace("antichat.com", "forum.antichat.ru"))
        boards_df["site_name"] = boards_df["url"].apply(lambda x: (x.replace("https://", "")).split("/")[0] if "https" in x else (x.replace("http://", "")).split("/")[0] )
        boards_df.drop_duplicates(inplace=True)

        #boards_df.drop(columns=["db_created_on", "db_updated_on"], inplace=True)
        #boards_df.drop_duplicates(inplace=True)
        
        boards_df = boards_df[["id", "site_id", "site_name", "name", "url"]].copy().drop_duplicates()
        boards_df.rename(columns={"id":"board_id", 
                                  "name":"board_title", 
                                  "url":"board_url"}, inplace=True)
        boards_df.drop_duplicates(inplace=True)

        #boards_df = boards_df[["board_id", "site_id", "site_name", "board_title", "board_url"]].copy()
        #boards_df.drop_duplicates(inplace=True)
        
        return boards_df
    
    # Process Threads
    def process_threads(self, threads_df):
        threads_df["url"] = threads_df["url"].apply(lambda x: x.replace("antichat.com", "forum.antichat.ru"))
        threads_df.drop_duplicates(inplace=True)

        #threads_df.drop(columns=["label", "last_post_on", "is_forward_direction", "db_created_on", "db_updated_on", "created_on"], inplace=True)
        #threads_df.drop_duplicates(inplace=True)
        
        threads_df = threads_df[["id", "site_id", "board_id", "creator_id", "creator", "name", "url"]].copy().drop_duplicates()
        threads_df.rename(columns={"creator":"username", 
                                   "id":"thread_id", 
                                   "creator_id":"user_id", 
                                   "name":"thread_title", 
                                   "url":"thread_url"}, inplace=True)
        threads_df.drop_duplicates(inplace=True)

        #threads_df = threads_df[["thread_id", "site_id", "board_id", "user_id", "username", "thread_title", "thread_url"]].copy()
        #threads_df.drop_duplicates(inplace=True)
        
        return threads_df
    
    # Process posts
    def process_posts(self, posts_df):
    
        posts_df.drop_duplicates(inplace=True)

        posts_df = posts_df[["id", "site_id", "board_id", "thread_id", "creator_id", "creator", "creator_reputation", "content", "created_on"]].copy().drop_duplicates()
        posts_df.rename(columns={"creator":"username", 
                                    "id":"post_id", 
                                    "creator_id":"user_id", 
                                    "creator_reputation":"user_reputation", 
                                    "created_on": "post_data_creation"}, inplace=True)
        posts_df.drop_duplicates(inplace=True)

        return  posts_df
