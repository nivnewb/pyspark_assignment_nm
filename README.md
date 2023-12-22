# pyspark_assignment_nm
The program reads two csv files from the input directory (csv_files) and loads them into a dataframe. Data manipulation is performed on the resulting dataframes. Data manipulation includes filtering the column country, dropping personal identifiable columns, renaming columns for easier readability and then inner join the two dataframes. The final dataframe is then written into a csv file within the client directory (client_data).

Input parameters required:
The program requires three input parameters namely path1, path2 and countries. They are as below.
--path1 "../csv_files/dataset_one.csv"
--path2 "../csv_files/dataset_two.csv"
--countries "Netherlands,United Kingdom"
