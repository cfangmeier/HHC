from io import TextIOWrapper
from datetime import datetime
import json

_DECODER = json.JSONDecoder()

_DEFAULT_CHUNK_SIZE = 4096
_MB = (1024 * 1024)
_LARGEST_JSON_OBJECT_ACCEPTED = 16 * _MB  # default to 16 megabytes

def get_file_size(input_file):
    if type(input_file) == TextIOWrapper:
        file_size = os.stat(input_file.name).st_size
    else:
        file_size = input_file.info().get('Content-Length')
        try:
            file_size = int(file_size)
        except ValueError:
            file_size = -1
    return file_size

last_time = datetime.now()
last_bytes = 0

def format_progress(downloaded, total):
    global last_time, last_bytes
    from math import log2, floor
    suffixes = ['', 'kB', 'MB', 'GB', 'TB']

    now = datetime.now()
    dl_speed = 10**6*(downloaded-last_bytes)/(now-last_time).microseconds
    last_bytes = downloaded
    last_time = now

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


def json_list_parser(input_file,
                     chunk_size=_DEFAULT_CHUNK_SIZE,
                     max_size=_LARGEST_JSON_OBJECT_ACCEPTED):
    """
    Read an input file, and yield up each JSON object parsed from the file.
    Allocates minimal memory so should be suitable for large input files.
    """
    file_size = get_file_size(input_file)

    bytes_read = 0
    def read(num_chars):
        nonlocal bytes_read
        s = input_file.read(num_chars)
        bytes_read += len(s)
        if type(s) == bytes:
            s = s.decode('utf8')
        return s

    #Seek to the opening bracket of the data list
    while True:
        c = read(1)
        if c == '[': break

    buf = ''
    while True:
        temp = read(chunk_size)
        # print(bytes_read)
        # if (not temp) and (bytes_read >= file_size):
        #     break
        if not temp:
            break

        # Accumulate more input to the buffer.
        #
        # The decoder is confused by leading white space before an object.
        # So, strip any leading white space if any.
        buf = (buf + temp).lstrip()
        while True:
            try:
                # Try to decode a JSON object.
                # print("DECODING WITH BUFFER")
                # print("*"*80)
                # print(buf)
                # print("*"*80)
                # input()
                x, i = _DECODER.raw_decode(buf)
                # print("JSON:\n{}".format(buf[:i]))
                # If we got back a dict, we got a whole JSON object.  Yield it.
                if type(x) == dict:
                    # First, chop out the JSON from the buffer.
                    # Also strip any leading white space if any.
                    buf = buf[i:].lstrip()
                    if len(buf) < chunk_size:
                        temp = read(chunk_size)
                        buf = (buf + temp).lstrip()
                    # print("NEXT BUFFER:\n{}\n".format(buf[:100]))
                    # Look for a following comma, indicating there is another
                    # entry in the list.
                    i = buf.find(',')
                    # print("i:\n{}".format(i))
                    yield (bytes_read,file_size),x
                    if i == -1:
                        # print(buf)
                        raise StopIteration()
                    buf = buf[i+1:].lstrip()

            except ValueError:
                # Either the input is garbage or we got a partial JSON object.
                # If it's a partial, maybe appending more input will finish it,
                # so catch the error and keep handling input lines.

                # Note that if you feed in a huge file full of garbage, this will grow
                # very large.  Blow up before reading an excessive amount of data.

                if len(buf) >= max_size:
                    raise ValueError("either bad input or too-large JSON object.")
                break
    buf = buf.strip()
    if buf:
        if len(buf) > 70:
            buf = buf[:70] + '...'
        raise ValueError('leftover stuff from input: "{}"\nLast object:\n{}'.format(buf,X))

if __name__=="__main__":
    import os
    import sys
    import re
    import urllib.request as request
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

