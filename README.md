# pythonUtilities
Simple python utilities to make dev job easier

### s3_download_orc_csv.py
This python utility has functions that let developers connect to a terminal (similar to Putty) and run commands. 
In this code I have one function ssh_connect_pwd, where you provide a password to connect to the host whereas in function ssh_connect_ppk
you need to provide a ppk file to connect to the host. Once connected you can pass your commands which need to be executed to function 
ssh_command. In this code I have connected to an EC2 machine, executed an aws cp command on the host and then downloaded all the files.
The files are in ORC format and are downloaded to the local directory. As a bonus step I use pyspark to convert the ORC files to CSV.

### Restoretable.ipynb
This python utility can be used to restore data in a versioned s3 bucket for Hive tables. Let's say you have created external partitioned hive tables where data is stored on a versioned s3 bucket. Now one of the users deletes data directly in the s3 bucket, and you need to restore the data from the versions. You just need to provide a connection to your metastore, and name of the table you want to restore and the script does the rest.
