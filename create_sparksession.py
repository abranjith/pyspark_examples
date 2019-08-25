from pyspark.sql import SparkSession

def create_sparksession(app_name, master="local", hive_suppport=True):
    """ Creates spark session

    Parameters
    ----------
    app_name    :   str
        Name of the application
    master  :   str,    optional (default is local)
        What to use as master url. Examples - 
            local - to run from local
            local[4] - run locally with 4 worker nodes (better to match with number of cores in the system)
            yarn - to specify yarn as the resource manager
            spark://master:7077 - for standalone cluster
        Check documentation for details - https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    hive_suppport : bool,   optional (default is True)
        Creates spark session with support for hive
    
    Returns
    -------
    SparkSession :   SparkSession
"""
    session = SparkSession.builder.master(master).appName(app_name)
    if hive_suppport:
        session = session.enableHiveSupport()
    session = session.getOrCreate()
    return session

def get_sparkconfig(session):
    """ Returns config information used in the SparkSession

    Parameters
    ----------
    session :   SparkSession

    Returns
    -------
    dict    :   Dictionary representing spark session configuration
    """
    conf = session.sparkContext.getConf().getAll()
    return conf

if __name__ == "__main__":
    #spark session
    spark_session = create_sparksession("MyApp")
    #spark context
    spark_context = spark_session.sparkContext
    #current configuration used in spark session
    conf = get_sparkconfig(spark_session)
    print(conf)