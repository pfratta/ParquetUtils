# ParquetUtils
A set of tools to manage Parquet files. 

## ParquetStats

The ParquetStats tool takes as input: 
*  the HDFS folder containing Parquet files. Url form, such as `hdfs://localhost:9000/foo/bar` and absolute path form, such as `/foo/bar`, are both valid
*  the configuration file that specifies the IP address and port of namenode (optional).

It gives as output the following descriptive statistics: the total size of the directory, standard deviation, average size, minimum size, maximum size and number of files. File sizes are reported in MByte.

## Syntax

Usage: ParquetStats (-i, --input path_to_HDFS_folder) [-c, --config path_to_configuration_file]

The brackets denote optional elements, while parentheses the mandatory ones. You can either use the long or the short version of the two options. If the `-c` option is omitted, the configuration file is searched in the classpath; otherwise, an error message will be displayed to the user. 

Here an example of tool usage:
```
ParquetStats -i /home/parquet_dataset -c /etc/hadoop/conf
```
The previous command will connect to the namenode, will read .parquet files in `parquet_dataset` folder and will compute the statistics. Moreover, the tool allows to manage logging level at runtime by setting the `-Dlog4j=log_level` system property as VM arguments. There are several `log_level` as you can see in the log4j library documentation. 

## ParquetRepart

The ParquetRepart tool takes as input:
*  the HDFS folder containing N<sub>T</sub> Parquet files. Url form, such as `hdfs://localhost:9000/foo/bar` and absolute path form, such as `/foo/bar`, are both valid
*  the number N<sub>R</sub> of .parquet file we want to get in output or the minimum file size
*  a flag indicating the overwrite mode.

It gives as output a new set of N<sub>R</sub> Parquet files, stored in a new folder (generally, N<sub>R</sub> ≤ N<sub>T</sub>).

## Syntax

Usage: ParquetRepart (-i, --input path_to_HDFS_folder) (-o, --output path_to_output_dir] [-p, --partitions number_of_parquet_files | -s, --size min_size_of_parquet_file] [-f, --flagoverwrite]
