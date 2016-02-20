from io import TextIOWrapper
from datetime import datetime
import urllib.request as request
import urllib.parse as parse
import json

_DECODER = json.JSONDecoder()

_DEFAULT_CHUNK_SIZE = 4096
_MB = (1024 * 1024)
_LARGEST_JSON_OBJECT_ACCEPTED = 16 * _MB  # default to 16 megabytes
_DEFAULT_TIMEOUT = 20 # seconds

_VERBOSE = False

def _get_file_size(stream):
    if type(stream) == TextIOWrapper:
        file_size = os.stat(stream.name).st_size
    else:
        file_size = stream.info().get('Content-Length')
        try:
            file_size = int(file_size)
        except (ValueError, TypeError):
            file_size = -1
    return file_size

def _open_stream(url, timeout):
    parse_result = parse.urlparse(url)
    if parse_result.scheme == "file":
        return open(parse_result.path,'r')
    else:
        return request.urlopen(url, timeout=timeout)

last_time = datetime.now()
last_bytes = 0
def format_progress(downloaded, total):
    global last_time, last_bytes
    from math import log2, floor
    if(downloaded < last_bytes): last_bytes = 0
    suffixes = ['bytes', 'kB', 'MB', 'GB', 'TB']

    now = datetime.now()
    dl_speed = 10**6*(downloaded-last_bytes)/(now-last_time).microseconds
    last_bytes = downloaded
    last_time = now

    try:
        suf_idx = floor(log2(total)/10)
        suffix = suffixes[suf_idx]
        downloaded /= 2**(suf_idx*10)
        total /= 2**(suf_idx*10)

        suf_idx = floor(log2(dl_speed)/10)
        dl_suffix = suffixes[suf_idx]+'/s'
        dl_speed /= 2**(suf_idx*10)

        progress = downloaded/total * 100 if total > 0 else -1
        s = "({:.2f}{}/{:.2f}{}),({:.2f}{}) {:.2f}%".format(downloaded, suffix, total, suffix,
                                                            dl_speed, dl_suffix, progress)
        return s
    except ValueError:
        return ""
        



def json_list_parser(url,
                     timeout = _DEFAULT_TIMEOUT,
                     chunk_size=_DEFAULT_CHUNK_SIZE,
                     max_size=_LARGEST_JSON_OBJECT_ACCEPTED):
    """
    Read an input file, and yield up each JSON object parsed from the file.
    Allocates minimal memory so should be suitable for large input files.
    """
    stream = _open_stream(url, timeout)
    file_size = _get_file_size(stream)

    bytes_read = 0
    def read(num_chars):
        nonlocal bytes_read
        try:
            s = stream.read(num_chars)
        except TimeoutError:
            stream.close()
            raise TimeoutError()
        bytes_read += len(s)
        if type(s) == bytes:
            s = s.decode('utf8')
        return s.lstrip()

    #Seek to the opening bracket of the data list
    while True:
        c = read(1)
        if c == '[': break

    buf = ''
    while True:
        temp = read(chunk_size)
        if not temp:
            break

        # The decoder is confused by leading white space before an object.
        # So, strip any leading white space if any.
        buf += temp
        while True:
            try:
                x, i = _DECODER.raw_decode(buf)
                if type(x) == dict:
                    buf = buf[i:].lstrip()
                    if len(buf) < chunk_size:
                        temp = read(chunk_size)
                        buf += temp
                    # Look for a following comma, indicating there is another
                    # entry in the list.
                    i = buf.find(',')
                    yield (bytes_read,file_size),x
                    if i == -1:
                        raise StopIteration()
                    buf = buf[i+1:].lstrip()

            except ValueError:
                if len(buf) >= max_size:
                    raise ValueError("either bad input or too-large JSON object.")
                break
    buf = buf.strip()
    if buf:
        if len(buf) > 70:
            buf = buf[:70] + '...'
        raise ValueError('Leftover stuff from input: "{}"\nLast object:\n{}'.format(buf,X))

if __name__=="__main__":
    import os
    import sys
    import re
    url_regex = re.compile(
                r'^(?:http|ftp)s?://' # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                r'localhost|' #localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                r'(?::\d+)?' # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if url_regex.fullmatch(sys.argv[1]) is not None:
        f = request.urlopen(sys.argv[1], timeout=20)
        get_file_size(f)
        print("Downloading plan json\n\tFile Size: {}Bytes".format(file_size))
    else:
        f = open(sys.argv[1],'r')
        get_file_size(f)
        print("Opening plan json\n\tFile Size: {}Bytes".format(file_size))
    parser = json_list_parser(f, file_size)

    for (bytes_read,file_size),obj in parser:
        print("\nREAD ({}/{}) BYTES".format(bytes_read,file_size))
        print(obj)
        input()

