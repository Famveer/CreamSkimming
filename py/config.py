import os
from pydantic_settings import BaseSettings
import ast
from dotenv import load_dotenv
load_dotenv()

class Config(BaseSettings):

    USER_PASSWD: str = os.getenv('USER_PASSWD')
    DATA_PATH: str = os.getenv('DATA_PATH')
    MODEL_PATH: str = os.getenv('MODEL_PATH')
    
    MODEL_TASK_NAME: str = os.getenv('MODEL_TASK_NAME', "RandomForest")
    
    YEAR: int = int(os.getenv('YEAR', "2019"))
    
    PROCESS_TXT: bool = ast.literal_eval(os.getenv("PROCESS_TXT", "True"))
    CHUNK_SIZE: int = int(os.getenv('CHUNK_SIZE', "100000"))
    CSV_SEP: str = os.getenv("CSV_SEP", "\\")
    KEYWORD_SEP: str = os.getenv("KEYWORD_SEP", " FAMVEERFAMVEERFAMVEER ")
    TOPIC_SEARCH: str = os.getenv("TOPIC_SEARCH", "cve")
    ANY_FORMAT: bool = ast.literal_eval(os.getenv("ANY_FORMAT", "True"))
    DELETE_DBS: bool = ast.literal_eval(os.getenv("DELETE_DBS", "True"))
    
    RANDOM_STATE: int = int(os.getenv('RANDOM_STATE', "42"))
    TOP_K_FEATURES: int = int(os.getenv('TOP_K_FEATURES', "15"))
