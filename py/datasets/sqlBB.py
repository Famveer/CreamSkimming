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
from pathlib import Path

from py.utils import verifyDir
from py.database.config import POSGRES_Config

'''
sudo setfacl -R -m u:postgres:rwx /media/felipe
su -l postgres
psql
CREATE USER crimebb WITH PASSWORD 'crimebb';
GRANT ALL PRIVILEGES ON DATABASE postgres TO crimebb;
ALTER ROLE crimebb SUPERUSER;
'''

class sqlBB():
    def __init__(self, config_file=None):
        pass
