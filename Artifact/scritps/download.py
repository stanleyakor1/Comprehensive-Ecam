import urllib.request
import tarfile
import gzip
import os
import subprocess

def download_data(args):
    try:
        url, filename = args
        urllib.request.urlretrieve(url, filename)
        return True
    except Exception as e:
        return str(e)


def untar_file(filename, exdir):
        with tarfile.open(filename, 'r') as tar:
            tar.extractall(exdir)

def uncompress_all_gz_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".gz"):
            gz_file_path = os.path.join(folder_path, filename)
            output_file_path = os.path.splitext(gz_file_path)[0]  # Remove .gz extension
            # Run the gunzip command using subprocess
            subprocess.run(["gunzip", gz_file_path])

def remove_compressed_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".gz") or filename.endswith('.tar'):
            gz_file_path = os.path.join(folder_path, filename)
            # Run the rm command using subprocess
            subprocess.run(["rm", gz_file_path])