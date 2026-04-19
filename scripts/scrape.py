import kaggle
import psycopg2
import zipfile
import os

def scrapeKaggle(datasetpath):
    kaggle.api.authenticate()
    
    savePath="./data/raw"
    
    kaggle.api.dataset_download_files(datasetpath, path=savePath, unzip=False)
    
    zipPath=datasetpath.split("/")[-1]+".zip"
    name=os.path.join(savePath,zipPath)
    
    return name

def createDB(HostDB, DBName, DBUser, DBPassword, DBPort):
    con = psycopg2.connect(
        host = HostDB,
        dbname = "postgres",
        user = DBUser,
        password = DBPassword,
        port = DBPort
    )
    
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
    con = psycopg2.connect(
        host = HostDB,
        dbname = DBName,
        user = DBUser,
        password = DBPassword,
        port = DBPort
    )
    
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
    
def uploadData(dataname, schemaName, HostDB, DBName, DBUser, DBPassword, DBPort):
    con = psycopg2.connect(
        host = HostDB,
        dbname = DBName,
        user = DBUser,
        password = DBPassword,
        port = DBPort
    )
    rawPath = f"/.data/raw/{dataname}"
    bronzePath = "/.data/bronze"
    
    with zipfile.ZipFile(rawPath,"r") as zip:
        zip.extractall(bronzePath)
    
    cur = con.cursor()
    
    cur.execute
    

    cur.execute(f"""
                set search_Path to {schemaName}, public;
                """)
    
    for file in os.listdir(bronze_path):

        if file.endswith(".csv"):

            file_path = os.path.join(bronze_path, file)

            df = pd.read_csv(file_path)

            table_name = file.replace(".csv","")

            columns = list(df.columns)

            values = [tuple(row) for row in df.values]

            query = f"""
                INSERT INTO {table_name} ({",".join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
            """

            try:
                execute_values(cur, query, values)
                con.commit()

                print(f"{file} berhasil dimasukkan")

            except Exception as e:
                con.rollback()
                print(f"{file} gagal: {e}")

    cur.close()
    con.close()