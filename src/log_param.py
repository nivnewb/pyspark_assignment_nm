import logging.handlers
import os
import argparse

file_name = '../log/pyspark_application.log'
# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
should_roll_over = os.path.isfile(file_name)
handler = logging.handlers.RotatingFileHandler(file_name, backupCount=2)
if should_roll_over:
    handler.doRollover()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Arguments passing
parser = argparse.ArgumentParser()
parser.add_argument("--path1", help="path info for file 1")
parser.add_argument("--path2", help="path info for file 2")
parser.add_argument("--countries", help="two countries are required; format as <country1,country2>")
args = parser.parse_args()
if args.path1 and args.path2 and args.countries:
    path_in_1 = args.path1
    path_in_2 = args.path2
    country = args.countries.split(sep=',')
