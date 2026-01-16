class SetupHandler:
    def __init__(self, env):
        Conf = Config()
        self.landing_dir = Conf.base_data_dir + "/raw"
        self.checkpoint_dir = Conf.base_checkpoint_dir + "/checkpoints"
        self.catalog = env
        self.db_name = Conf.db_name
        self.initailized = False

    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating database {self.catalog}.{self.db_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Done")

    def create_bronze_tbl(self):
        if self.initialized:
            print(f"Creating kafka multiplex bronze table")
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.bronze(
                key string, 
                value string, 
                topic string, 
                partition long, 
                offset long, 
                timestamp long,                  
                year_month string,                  
                load_time timestamp,
                source_file string)
                PARTITIONED BY (topic, year_month)
                """)
            print("Done")
        else:
            raise ReferenceError("Database not initialized. Cannot create table in default database.")

    def create_silver_customers_tbl(self):
        if self.initialized:
            print(f"Creating silver customers table")
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.silver_customers(
                customer_id STRING,
                email STRING,
                first_name STRING,
                last_name STRING,
                gender STRING,
                street STRING,
                city STRING,
                country_code STRING,
                row_status STRING,
                row_time timestamp)
                """)
            print("Done")
        else:
            raise ReferenceError("Database not initialized. Cannot create table in default database.")

    def create_silver_orders_tbl(self):
        if self.initialized:
            print(f"Creating silver orders table")
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.silver_orders(
                order_id STRING,
                order_timestamp Timestamp,
                customer_id STRING,
                quantity BIGINT,
                total BIGINT,
                books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>
                """)
            print("Done")
        else:
            raise ReferenceError("Database not initialized. Cannot create table in default database.")

    def create_silver_books_tbl(self):
        if self.initialized:
            print(f"Creating silver books table")
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.silver_books(
                book_id STRING,
                title STRING,
                author STRING,
                price DOUBLE,
                current BOOLEAN,
                effective_date TIMESTAMP,
                end_date TIMESTAMP
                """)
            print("Done")
        else:
            raise ReferenceError("Database not initialized. Cannot create table in default database.")


        




            
