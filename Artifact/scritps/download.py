import urllib.request
import tarfile
import gzip
import os

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

def ungzip_files_in_folder(folder_path):
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith('.gz'):
                input_path = os.path.join(root, filename)
                output_path = os.path.splitext(input_path)[0]  # Remove .gz extension
                with gzip.open(input_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
                    f_out.write(f_in.read())
                print(f"Uncompressed {filename} to {output_path}")

def remove_tar_and_gz_files(folder_path):
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith('.tar') or filename.endswith('.gz'):
                file_path = os.path.join(root, filename)
                os.remove(file_path)
                print(f"Removed {filename}")