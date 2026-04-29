from scripts.utils.connection import connection

def createDB(DBName):
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
        print("Database telah dibuat")
    else:
        print("Database sudah ada")
    
    cur.close()
    con.close()

def createSchemaTable(SchemaName, DBName):
    con = connection(DBName)
    
    cur = con.cursor()
    
    cur.execute(f"""
                create schema if not exists {SchemaName};
                """)
    
    cur.execute(f"""
                create table if not exists {SchemaName}.dimgeolocation(
                    zipCode int primary key,
                    latitude double precision,
                    longitude double precision,
                    city varchar(50),
                    state varchar(50)
                    );
                    """)

    cur.execute(f"""
                create table if not exists {SchemaName}.dimsellers(
                    sellerId varchar(50) primary key,
                    zipCode int,
                    city varchar(50),
                    state varchar(50)
                );
                """)

    cur.execute(f"""
                create table if not exists {SchemaName}.dimcustomers(
                    customerId varchar(50) primary key,
                    uniqueId varchar(50),
                    zipCode int,
                    city varchar(50),
                    state varchar(50)
                );
                """)

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

    cur.execute(f"""
                create table if not exists {SchemaName}.dimpurchasedate(
                    dateId int primary key,
                    datePurchase timestamp,
                    year int,
                    month int,
                    day int
                );
                """)

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
                    primary key(orderId, itemId),
                    foreign key(customerId) references {SchemaName}.dimcustomers(customerId),
                    foreign key(sellerId) references {SchemaName}.dimsellers(sellerId),
                    foreign key(productId) references {SchemaName}.dimproducts(productId),
                    foreign key(dateId) references {SchemaName}.dimpurchasedate(dateId)
                );
                """)
    
    con.commit()
    
    cur.close()
    con.close()
        
    return print("table sudah di buat")