import psycopg2
from scripts.utils.connection import connection

def createDB(HostDB, DBName, DBUser, DBPassword, DBPort):
    con = connection(HostDB, "postgres", DBUser, DBPassword, DBPort)
    
    con.autocommit=True
    
    cur = con.cursor()
    
    cur.execute(f"""
                select 1 from pg_database where datname = {DBName}
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

def CreateSchema(SchemaName, HostDB, DBName, DBUser, DBPassword, DBPort):
    con = connection(HostDB, DBName, DBUser, DBPassword, DBPort)
    
    cur = con.cursor()
    
    cur.execute(f"""
                create schema if not exists {SchemaName};
                """)
    
    cur.execute(f"""
                create table if not exists {SchemaName}.geolocation(
                    geoId serial primary key,
                    zipCode int,
                    latitude float,
                    longtitude float,
                    city varchar(50),
                    state varchar(10)
                    );
                    """)

    cur.execute(f"""
                create table if not exists {SchemaName}.sellers(
                    sellerId varchar(50) primary key,
                    zipCode int,
                    city varchar(50),
                    state varchar(10)
                );
                """)

    cur.execute(f"""
                create table if not exists {SchemaName}.customers(
                    customerId varchar(50) primary key,
                    customerUniqueId varchar(50),
                    zipCode int,
                    city varchar(50),
                    state varchar(10)
                );
                """)

    cur.execute(f"""
                create table if not exists {SchemaName}.products(
                    productId varchar(50) primary key,
                    categoryName varchar(30),
                    nameLength int,
                    descriptionLength int,
                    photosQty int,
                    weightCm int,
                    lengthCm int,
                    heightCm int,
                    widthCm int
                );
                """)

    cur.execute(f"""
                create table if not exists {SchemaName}.orders(
                    orderId varchar(50) primary key,
                    customerId varchar(50),
                    orderStatus varchar(25),
                    purchaseDate timestamp,
                    approvedDate timestamp,
                    deliveredCarrier timestamp,
                    deliveredCustomer timestamp,
                    estimatedDelivery timestamp,
                    foreign key(customerId) references customers(customerId)
                );
                """)
    
    cur.execute(f"""
                create table if not exists {SchemaName}.reviews(
                    revieworderId varchar(50) primary key,
                    orderId varchar(50),
                    reviewScore int,
                    commentTitle text,
                    commentMessage text,
                    reviewCreation timestamp,
                    reviewAnswer timestamp,
                    foreign key(orderId) references orders(orderId)
                );
                """)
    
    cur.execute(f"""
                create table if not exists {SchemaName}.payments(
                    paymentId serial primary key,
                    orderId varchar(50),
                    orderStatus varchar(25),
                    sequential int,
                    type varchar(25),
                    installments int,
                    paymentValue float,
                    foreign key(orderId) references orders(orderId)
                );
                """)
    
    cur.execute(f"""
                create table if not exists {SchemaName}.orderItems(
                    orderItemId int,
                    orderId varchar(50),
                    productId varchar(50),
                    sellerId varchar(50),
                    limitDate timestamp,
                    price float,
                    freightValue float,
                    foreign key(orderId) references orders(orderId),
                    foreign key(productId) references products(productId),
                    foreign key(sellerId) references sellers(sellerId)
                );
                """)
    
    return print("table sudah di buat")