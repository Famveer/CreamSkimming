#!/usr/bin/python

class CrimeBB_v1():
    
    def __init__(self):
        pass

    # Process members
    def process_members(self, members_df):
        members_df.drop_duplicates(inplace=True)
        
        members_df = members_df[["IdMember", "Username", "Site"]].copy().drop_duplicates()
        members_df.rename(columns={"IdMember":"user_id", 
                                   "Username":"username", 
                                   "Site":"site_id"}, inplace=True)
        members_df.drop_duplicates(inplace=True)
        
        members_df = members_df[["user_id", "username", "site_id"]].copy()
        members_df.drop_duplicates(inplace=True)

        return members_df

    # Process sites
    def process_sites(self, website_df):
        website_df.drop(columns=["NumMembers", "NumForums", "parsed", "Name", "LastParse"], inplace=True)
        website_df.drop_duplicates(inplace=True)
        
        website_df.rename(columns={"URL":"site_name", 
                                   "IdSite":"site_id"}, inplace=True)
        website_df.drop_duplicates(inplace=True)
        
        website_df["site_name"] = website_df["site_name"].apply(lambda x: (x.replace("https://", "")) if "https" in x else (x.replace("http://", "")) )
        website_df.sort_values(by="site_id", inplace=True)

        return website_df
    
    # Process boards
    def process_boards(self, boards_df):
        boards_df.drop_duplicates(inplace=True)
        
        boards_df.drop(columns=["NumThreads", "parsed", "NumPages", "LastParse"], inplace=True)
        boards_df.drop_duplicates(inplace=True)
        
        boards_df.rename(columns={"IdForum":"board_id", 
                                  "Title":"board_title", 
                                  "URL":"board_url", 
                                  "Site": "site_id"}, inplace=True)
        boards_df.drop_duplicates(inplace=True)
        
        return boards_df

    # Process threads
    def process_threads(self, threads_df):
        threads_df.drop_duplicates(inplace=True)
        
        threads_df.drop(columns=["LastParse", "parsed", "NumPages", "Direction", "NumPosts"], inplace=True)
        threads_df.drop_duplicates(inplace=True)
        
        threads_df.rename(columns={"IdThread":"thread_id", 
                                   "Site":"site_id", 
                                   "Forum":"board_id", 
                                   "Author":"user_id", 
                                   "AuthorName":"username", 
                                   "Heading":"thread_title", 
                                   "URL":"thread_url"}, inplace=True)
        threads_df.drop_duplicates(inplace=True)
        
        threads_df = threads_df[["thread_id", "site_id", "board_id", "user_id", "username", "thread_title", "thread_url"]].copy()
        threads_df.drop_duplicates(inplace=True)

        return threads_df

    # Process Posts
    def process_posts(self, posts_df):
        posts_df.drop_duplicates(inplace=True)

        posts_df.drop(columns=["Likes", "parsed", "LastParse", "CitedPost", "AuthorNumPosts"], inplace=True)
        posts_df.drop_duplicates(inplace=True)

        posts_df.rename(columns={"IdPost": "post_id", 
                                "Author":"user_id", 
                                "Thread":"thread_id", 
                                "Timestamp":"post_data_creation", 
                                "Content":"content", "site":"site_id", 
                                "AuthorReputation":"user_reputation", 
                                "Site":"site_id", 
                                "AuthorName":"username"}, inplace=True)
        posts_df.drop_duplicates(inplace=True)

        return posts_df
