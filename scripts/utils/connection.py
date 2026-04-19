import psycopg2
import sqlalchemy



def connection(HostDB, DBName, DBUser, DBPassword, DBPort):
    con = psycopg2.connect(
        host = HostDB,
        dbname = DBName,
        user = DBUser,
        password = DBPassword,
        port = DBPort
    )

    return con
