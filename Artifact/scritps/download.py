import urllib.request
import tarfile

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
