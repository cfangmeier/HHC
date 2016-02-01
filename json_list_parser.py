import json

_DECODER = json.JSONDecoder()

_DEFAULT_CHUNK_SIZE = 4096
_MB = (1024 * 1024)
_LARGEST_JSON_OBJECT_ACCEPTED = 16 * _MB  # default to 16 megabytes

def json_list_parser(input_file,
                     chunk_size=_DEFAULT_CHUNK_SIZE,
                     max_size=_LARGEST_JSON_OBJECT_ACCEPTED):
    """
    Read an input file, and yield up each JSON object parsed from the file.
    Allocates minimal memory so should be suitable for large input files.
    """
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
                x, i = _DECODER.raw_decode(buf)
                # If we got back a dict, we got a whole JSON object.  Yield it.
                if type(x) == dict:
                    # First, chop out the JSON from the buffer.
                    # Also strip any leading white space if any.
                    buf = buf[i:].lstrip()
                    if len(buf) < chunk_size:
                        temp = read(chunk_size)
                        buf = (buf + temp).lstrip()
                    # Look for a following comma, indicating there is another
                    # entry in the list.
                    i = buf.find(',')
                    yield bytes_read,x
                    if i == -1:
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

