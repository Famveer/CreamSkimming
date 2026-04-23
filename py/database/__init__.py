from .dbmanager import *

'''
sudo setfacl -R -m u:postgres:rwx /media/felipe
su -l postgres
psql
CREATE USER crimebb WITH PASSWORD 'crimebb';
GRANT ALL PRIVILEGES ON DATABASE postgres TO crimebb;
ALTER ROLE crimebb SUPERUSER;
'''
