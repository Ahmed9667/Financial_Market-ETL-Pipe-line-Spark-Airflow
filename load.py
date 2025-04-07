def run_loading():
    try:
        #import necessary libraries
        from pyspark.sql import SparkSession
        from pyspark.sql import DataFrameWriter
        from pyspark.sql.functions import col
        import psycopg2
        spark = SparkSession.builder.appName('finance')\
                .master("local[*]") \
                .config('spark.jars', '/home/ahly9667/airflow/dags/postgresql-42.7.3.jar')\
                .getOrCreate()
        
        df = spark.read.csv(r'/home/ahly9667/airflow/dags/weekly_adjusted_IBM.csv',header=True, inferSchema=True)
        df.show(5)

        df.printSchema()
        print("✅ Row count:", df.count())

        #check null values
        for i in df.columns:
            null = df.filter(df[i].isNull()).count()
            print(null)

        #rename columns
        df = df.withColumnRenamed('timestamp','date')
        df = df.withColumnRenamed('dividend amount','dividend_amount')
        df = df.withColumnRenamed('adjusted close','adjusted_close')

        # Cast to match PostgreSQL schema
        df = df.withColumn("date", col("date").cast("date")) \
               .withColumn("open", col("open").cast("float")) \
               .withColumn("high", col("high").cast("float")) \
               .withColumn("low", col("low").cast("float")) \
               .withColumn("close", col("close").cast("float")) \
               .withColumn("adjusted_close", col("adjusted_close").cast("float")) \
               .withColumn("volume", col("volume").cast("bigint")) \
               .withColumn("dividend_amount", col("dividend_amount").cast("float"))

        df.show(5)
        df.printSchema()



        # Define database connection parameters including the database name
        db_params = {
           'username':'postgres',
           'password':'ahly9667',
           'host':'localhost',
           'port':'5432',
           'database':'vantage'}


    # Connect to the new created database alayta_bank
        def db_connected():
            connection = psycopg2.connect(user = 'postgres', 
                                        host= 'localhost',
                                        password = 'ahly9667',
                                        port = 5432,
                                        database ='vantage')

            return connection

        conn = db_connected()
        

        #create a function to create related tables of schema
        def create_table():
            conn = db_connected()
            cursor= conn.cursor()
            query = """
                        drop table if exists stock;

                        create table stock(
                        date date,
                        open float,
                        high float,
                        low float,
                        close float,
                        adjusted_close float,
                        volume bigint,
                        dividend_amount float)
                        
                        """
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()

        create_table()

        #load data into tables
        my_url = "jdbc:postgresql://localhost:5432/vantage"
        my_properties = {'user' : 'postgres',
                    'password' : 'ahly9667',
                    'driver' : 'org.postgresql.Driver',
                    'currentSchema': 'public'}

        df.write.jdbc( url= my_url , table = 'stock' , mode ='append' ,properties= my_properties)
        print("✅ Data successfully written to PostgreSQL")
        df_check = spark.read.jdbc(url=my_url, table='stock', properties=my_properties)
        print("✅ Inserted row count:", df_check.count())
        df_check.show(5)


    except Exception as e:
        print("Data loading Failed!", e)

