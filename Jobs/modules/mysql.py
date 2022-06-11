from sqlalchemy import create_engine

class Interface_MySQL:
    
    def __init__(self, user, password, host, dbname):
        try:
            self.user = user
            self.password = password
            self.host = host
            self.dbname = dbname
        except Exception as e:
            print("Error: ", str(e))
        
    def create_engine(self):
        try:
            cnx = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.dbname}')
            return cnx
        except Exception as e:
            print("Error: ", str(e))
        
class Conector_mysql:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
    
    def envia_mysql(self, dfs, table):
        self.dfs = dfs
        self.table = table
        self.dfs.write.format("jdbc")\
                .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
                .option('driver', 'com.mysql.cj.jdbc.Driver')\
                .option("numPartitions", "10") \
                .option("user",self.user)\
                .option("password", self.password)\
                .option("dbtable", self.db + "." + self.table)\
                .mode("append").save()
    
    def ler_mysql(self, table, spark_conection):
        self.table = table
        self.spark_conection = spark_conection
        self.df = self.spark_conection.read.format("jdbc")\
            .option('url', f'jdbc:mysql://{self.host}/{self.db}')\
            .option('driver', 'com.mysql.cj.jdbc.Driver')\
            .option("user",self.user)\
            .option("password", self.password)\
            .option("dbtable", self.db + "." + self.table).load()
        return self.df