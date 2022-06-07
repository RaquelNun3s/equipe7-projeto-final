class Conector_mongo():
    '''
    Essa classe tem por objetivo realizar operações entre o pyspark e o mongodb atlas.
        user = o nome do seu projeto do mongodb atlas
        password = sua senha do cluster criado no mongodb atlas
        db = a database que será utilizada
        
    '''
    def __init__(self, user, password, db):
        self.user = user
        self.password = password
        self.db = db
  
    def inserir_mongo(self, df, collection):
        '''
        Esse método tem por objetivo inserir todos os dados de uma dataframe spark no mongodb atlas
            df = a dataframe do spark que deseja realizar a inserção
            collection = o nome da collection que deseja inserir os dados
        '''
        self.collection=collection
        self.df = df
        mongo_ip = f"mongodb://{self.user}:{self.password}@ac-5uquupr-shard-00-00.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-01.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-02.bjjkitq.mongodb.net:27017/?ssl=true&replicaSet=atlas-dzh8bl-shard-0&authSource=admin&retryWrites=true/{self.db}."
        self.df.write.format('com.mongodb.spark.sql.DefaultSource')\
            .option('spark.mongodb.output.database', self.db)\
            .option('spark.mongodb.output.collection', self.collection)\
            .option('uri', mongo_ip + self.collection)\
            .mode('Overwrite')\
            .option('maxBatchSize', "80000000").save()
    
    def ler_mongo(self, spark_session, collection):
        '''
        Esse método tem por objetivo ler os dados de uma collection do mongodb atlas, retornando uma dataframe
        do pyspark
            spark_session = o nome da sua SparkSession
            collection = O nome da collection que deseja extrair os dados
        '''
        self.collection = collection
        self.spark_session = spark_session
        mongo_ip = f"mongodb://{self.user}:{self.password}@ac-5uquupr-shard-00-00.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-01.bjjkitq.mongodb.net:27017,ac-5uquupr-shard-00-02.bjjkitq.mongodb.net:27017/?ssl=true&replicaSet=atlas-dzh8bl-shard-0&authSource=admin&retryWrites=true/{self.db}."
        self.df = ( self.spark_session.read.format('com.mongodb.spark.sql.DefaultSource')
                   .option('spark.mongodb.input.database', self.db)
                   .option('spark.mongodb.input.collection', self.collection)
                   .option('uri', mongo_ip + self.collection).load()) 
        return self.df
