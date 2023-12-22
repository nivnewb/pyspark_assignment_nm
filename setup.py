from setuptools import setup, find_packages

setup(
    name='ABNAmro_Python',
    version='1.0',
    description='Project to convert two csv files into dataframe and then filter and join the dataframe. '
                'The final dataframe is written as a csv file',
    author='Niveditha',
    packages=find_packages(where='src', exclude=['tests'])
)
