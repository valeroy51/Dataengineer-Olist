from scripts.utils.connection import connection

from scripts.utils.logger import getLog

logger = getLog(__name__)

def createDB(DBName):
    try:
        logger.info(f"[DATABASE] Checking Database : {DBName}")
        
        con = connection("postgres")

        con.autocommit=True
        
        cur = con.cursor()
        
        cur.execute(f"""
                    select 1 from pg_database where datname = '{DBName}'
                    """)
        
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(f"""
                        create database {DBName}
                        """)
            logger.info(f"[DATABASE] Create Database : {DBName}")
        else:
            logger.info(f"[DATABASE] Database already exists : {DBName} ")
        
        cur.close()
        con.close()
        
    except Exception as e:
        logger.error(f"[DATABASE] [ERROR] Error when creating Database : {e}")
        raise

def createSchemaTable(SchemaName, DBName):
    con = connection(DBName)
    
    cur = con.cursor()
    
    try:
        logger.info(f"[DATABASE] Creating Schema and Table in Database : {DBName}, Schema : {SchemaName}")
        
        logger.info(f"[DATABASE] Creating Schema : {SchemaName}")
        cur.execute(f"""
                    create schema if not exists {SchemaName};
                    """)
        
        logger.info(f"[DATABASE] Creating Table : dimgeolocation")
        cur.execute(f"""
                    create table if not exists {SchemaName}.dimgeolocation(
                        zipCode int primary key,
                        latitude double precision,
                        longitude double precision,
                        city varchar(50),
                        state varchar(50)
                        );
                        """)

        logger.info(f"[DATABASE] Creating Table : dimsellers")
        cur.execute(f"""
                    create table if not exists {SchemaName}.dimsellers(
                        sellerId varchar(50) primary key,
                        zipCode int,
                        city varchar(50),
                        state varchar(50)
                    );
                    """)

        logger.info(f"[DATABASE] Creating Table : dimcustomers")
        cur.execute(f"""
                    create table if not exists {SchemaName}.dimcustomers(
                        customerId varchar(50) primary key,
                        uniqueId varchar(50),
                        zipCode int,
                        city varchar(50),
                        state varchar(50)
                    );
                    """)

        logger.info(f"[DATABASE] Creating Table : dimproducts")
        cur.execute(f"""
                    create table if not exists {SchemaName}.dimproducts(
                        productId varchar(50) primary key,
                        categoryName varchar(100),
                        nameLength int,
                        descriptionLength int,
                        photosQty int,
                        weightG int,
                        lengthCm int,
                        heightCm int,
                        widthCm int
                    );
                    """)

        logger.info(f"[DATABASE] Creating Table : dimpurchasedate")
        cur.execute(f"""
                    create table if not exists {SchemaName}.dimpurchasedate(
                        dateId int primary key,
                        datePurchase timestamp,
                        year int,
                        month int,
                        day int
                    );
                    """)

        logger.info(f"[DATABASE] Creating Table : factpayments")
        cur.execute(f"""
                    create table if not exists {SchemaName}.factpayments(
                        orderId varchar(50),
                        sequential int,
                        type varchar(30),
                        installments int,
                        value double precision,
                        primary key(orderId, sequential)
                    );
                    """)

        logger.info(f"[DATABASE] Creating Table : factreviews")
        cur.execute(f"""
                    create table if not exists {SchemaName}.factreviews(
                        factId serial primary key,
                        reviewId varchar(50),
                        orderId varchar(50),
                        score int,
                        commentTitle text,
                        commentMessage text,
                        creationDate timestamp,
                        answertimestamp timestamp
                    );
                    """)
        
        logger.info(f"[DATABASE] Creating Table : factsales")
        cur.execute(f"""
                    create table if not exists {SchemaName}.factsales(
                        orderId varchar(50),
                        itemId int,
                        customerId varchar(50),
                        sellerId varchar(50),
                        productId varchar(50),
                        dateId int,
                        status varchar(30),
                        price double precision,
                        freightValue double precision,
                        totalPrice double precision,
                        primary key(orderId, itemId),
                        foreign key(customerId) references {SchemaName}.dimcustomers(customerId),
                        foreign key(sellerId) references {SchemaName}.dimsellers(sellerId),
                        foreign key(productId) references {SchemaName}.dimproducts(productId),
                        foreign key(dateId) references {SchemaName}.dimpurchasedate(dateId)
                    );
                    """)
        
        con.commit()
        logger.info(f"[DATABASE] All table created successfully")
        
    except Exception as e:
        con.rollback()
        logger.error(f"[DATABASE] [ERROR] Error when creating Schema and Table in Database {DBName} : {e}")
        raise
    
    finally:
        cur.close()
        con.close()