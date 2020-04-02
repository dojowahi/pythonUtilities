from pyspark import SparkContext, SQLContext
import paramiko
import os
from datetime import datetime

def ssh_connect_pwd(host, user, pwd):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=user, password=pwd)
        return ssh

    except Exception as e:
        print('Connection Failed')
        print(e)


def ssh_connect_ppk(host, user, key):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=user, key_filename=key)
        return ssh

    except Exception as e:
        print('Connection Failed')
        print(e)


def ssh_close(ssh):
    ssh.close()


def ssh_command(ssh, command):
    # command = input("Command:")
    ssh.invoke_shell()
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.readlines()


def ftp_file(ssh, remote_path, local_path):
    list_cmd = 'ls ' + remote_path
    file_list = ssh_command(ssh, list_cmd)
    try:
        ftp_client = ssh.open_sftp()
        for file in file_list:
            file = file.strip()
            remote_file = remote_path + file
            local_file = local_path + file
            ftp_client.get(remote_file, local_file)
            print("Download completed for:{}".format(file))
    except Exception as e:
        print('FTP Connection Failed')
        print(e)
    finally:
        ftp_client.close()
        ssh.exec_command('rm -rf ' + remote_path)


def orc_to_csv(local_path):
    sc = SparkContext("local", "SQL App")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format('orc').load(local_path)
    pandas_df = df.toPandas()
    csv_dir = local_path + 'csvFile/'
    if not os.path.isdir(csv_dir):
        os.makedirs(csv_dir)

    suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_nm = 'panda_'+suffix+'.csv'
    csv_file_loc = csv_dir + csv_file_nm

    pandas_df.to_csv(csv_file_loc, index=False)


if __name__ == '__main__':
    user = input("Username:")
    pwd = input("Password:")
    host = input("Target Hostname:")
    print("Connecting to terminal with password")
    pwd_ssh = ssh_connect_pwd(host, user, pwd)
    remote_path = '/tmp/maura_files/'
    local_path = 'C:/Users/orcfiles/'
    if not os.path.isdir(local_path):
        os.makedirs(local_path)
    bucket_path = 's3://bucket-data/orc_files/'
    aws_cmd = 'aws s3 cp --recursive ' + bucket_path + ' ' + remote_path
    aws_output = ssh_command(pwd_ssh, aws_cmd)
    ssh_close(pwd_ssh)
    print("Connecting to terminal with ppk file")
    ppk_ssh = ssh_connect_pwd(host, 'hadoop', 'C:/Users/my_ppk_key/mera_key.ppk')
    ftp_file(ppk_ssh, remote_path, local_path)
    ssh_close(ppk_ssh)

    print("Converting downloaded orc files to csv")
    orc_to_csv(local_path)



