CSE 512 - DDS : Assignment 4
Submitted by: Sumit Rawat
ASU ID: 1216225348

Approach used:
There are three functions that perform actions namely:

- Driver
It is the main function and is called "equijoin". It class the Mapper and Reducer classes and also specifies the output datatypes of the Mapper and Reducer. 

- Mapper
Mapper processes the input file. It fetches the columns for each tuple in the file by splitting the String with ", " and then maps each row to the value in their respective 2nd columns (join column).
The output of the mapper is the different rows (values) mapped to a unique vaue of their join column (key).

- Reducer
The reducer processes the key and value pairs from the mapper by splitting the value by "\n"(end line character). Then it iterates over the list of values (rows) in two loops and joins 2 rows if they have different table names (i.e. the first column in each row). The iterations are made in a way to avoid duplicate combinations. The reducer then outputs the result of the equijoin. 

To execute - 
sudo -u <username> <path_of_hadoop> jar <name_of_jar> <class_with_main_function> <HDFSinputFile> <HDFSoutputFile>
Ex: sudo -u hduser /usr/local/hadoop/bin/hadoop jar equijoin.jar equijoin hdfs://localhost:54310/input/sample.txt hdfs://localhost:54310/output

Here <name_of_jar> is equijoin.jar and <class_with_main_function> is equijoin
