import urllib.request

def download_data(args):
    try:
        url, filename = args
        urllib.request.urlretrieve(url, filename)
        return True
    except Exception as e:
        return str(e)
