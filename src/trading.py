from pyspark.sql import SparkSession
from log_param import logger


def read_csv_file(spark: SparkSession, file_path):
    """
    Read the input file.

    The function reads the csv file present in the input file path and loads into a dataframe.
    The dataframe is returned from the function.

    Parameters
    ----------
    spark : Spark session
        Spark session.
    file_path : str
        The file path of the csv.

    Return
    ------
    dataframe
        The dataframe from the read csv file.
    """
    logger.info("Starting read_csv_file function")
    # start spark code
    df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
    logger.info(f"Reading CSV File {file_path}")
    logger.info("Ending read_csv_file function")
    return df_raw


def data_manipulation(df1, df2, country_list):
    """
    Manipulate the input data.

    The function reads two dataframes and does data manipulation. The data manipulation includes filter data,
    dropping column, joining data and renaming column name. The dataframe is returned from the function.

    Parameters
    ----------
    df1 : dataframe
        Input dataframe 1.
    df2 : dataframe
        Input dataframe 2.
    country_list : list
        The list of country names used for filtering.

    Return
    ------
    dataframe
        The dataframe returned after data manipulation.
    """
    logger.info("Starting data_manipulation function")
    fil_df_1 = df1.filter((df1.country == country_list[0]) | (df1.country == country_list[1]))
    logger.info(f"Filtering countries {country_list} from dataframe")
    final_df_1 = fil_df_1.drop('first_name').drop('last_name')
    final_df_2 = df2.drop('cc_n')
    join_df = final_df_1.join(final_df_2, ['id'], "inner")
    fin_df = join_df.withColumnRenamed("btc_a", "bitcoin_address").withColumnRenamed("cc_t", "credit_card_type") \
        .withColumnRenamed("id", "client_identifier")
    logger.info("Ending data_manipulation function")
    return fin_df


def write_to_specific_path(df, spark, hdfs_path, filename):
    """
    Write dataframe into csv file.

    The function reads the input dataframe and writes it into a csv file. The csv file is created within the path from
    input parameter hdfs_path and named as input parameter file_name.
    The result is returned from the function.

    Parameters
    ----------
    df : dataframe
        Input dataframe.
    spark : SparkSession
        Spark session.
    hdfs_path : str
        The path for creating the file.
    filename :  str
        The name for creating the file.

    Return
    ------
    boolean
        The file path after creating the file in the mentioned path.
    """
    sc = spark.sparkContext
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    filesystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    df.coalesce(1).write.option("header", True).option("delimiter", ",").option("compression", "none")\
        .csv(hdfs_path, mode='overwrite')
    fs = filesystem.get(configuration())
    file = fs.globStatus(path("%s/part*" % hdfs_path))[0].getPath().getName()
    full_path = "%s/%s" % (hdfs_path, filename)
    result = fs.rename(path("%s/%s" % (hdfs_path, file)), path(full_path))
    return result
