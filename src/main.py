"""
# Title : main program
# Description : Reads csv files and data manipulation is performed.
The final dataframe is the written into a csv file in client location.
# Author : Niveditha
# Date : 22-December-2023
# Version : 1.0
"""

# import modules
import os.path
from pyspark.sql import SparkSession
from log_param import logger, path_in_1, path_in_2, country
import sys
from datetime import datetime
import trading


# current time variable to be used for logging purpose
dt_string = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
# change it to your app name
AppName = "MyPySparkApp"

# Input parameters
country_list = country
path_1 = path_in_1
path_2 = path_in_2
output_file_path = '../client_data/'
output_file_name = 'client_file.csv'


# Function to check if two files exists within given path
def check_file_exists():
    """
    Validate the input file.

    The function validates whether the files and their directories are present. A boolean is returned from the
    function based on the check.

    Return
    ------
    Boolean
        A boolean value is returned.
    """
    logger.info("Function check_file_exists")
    if os.path.isfile(path_1) & os.path.isfile(path_2):
        logger.info("Both the files exist")
        return True


# Function to initiate a spark session
def start_spark_session():
    """
    Create a spark session.

    The function initiate and creates a spark session. The SparkSession is returned from the function.

    Return
    ------
    SparkSession
        SparkSession is returned.
    """
    logger.info("Inside start_spark_session function")
    # start spark session
    spark: SparkSession = SparkSession.builder.\
        appName(start_spark_session.__name__ + "_" + str(dt_string)).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")
    return spark


def main():
    """
    Start the main flow.

    The function executes other functions like reading csv files, data manipulation.
     The final dataframe is written to a csv file in client location.

    Return
    ------
    None
        None is returned.
    """
    # start spark code
    logger.info("Main function starts")
    # validate whether the files exist
    if not check_file_exists():
        raise FileNotFoundError("The csv files were not found or the directory is missing")

    # Read the csv files from the path
    df_1 = trading.read_csv_file(start_spark_session(), path_1)
    df_2 = trading.read_csv_file(start_spark_session(), path_2)

    # This is combines function for the funcs df_1_upd, df_2_upd, joining_data
    joined_data = trading.data_manipulation(df_1, df_2, country_list)
    # joined_data.show(truncate=False)

    # Write the dataframe into csv file
    trading.write_to_specific_path(joined_data, start_spark_session(), output_file_path, output_file_name)

    logger.info("Ending spark application")
    # end spark session including all the existing ones
    SparkSession.builder.getOrCreate().stop()
    logger.info("Main function ends")
    return None


# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
