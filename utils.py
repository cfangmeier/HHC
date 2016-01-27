import json

from models import Issuer, Plan, Individual, Facility, Address

_DECODER = json.JSONDecoder()

_DEFAULT_CHUNK_SIZE = 4096
_MB = (1024 * 1024)
_LARGEST_JSON_OBJECT_ACCEPTED = 16 * _MB  # default to 16 megabytes

def json_objects_from_file(input_file,
                           chunk_size=_DEFAULT_CHUNK_SIZE,
                           max_size=_LARGEST_JSON_OBJECT_ACCEPTED):
    """
    Read an input file, and yield up each JSON object parsed from the file.
    Allocates minimal memory so should be suitable for large input files.
    """
    def read(bytes):
        return input_file.read(bytes).decode('utf8')

    X = None
    #Seek to the opening bracket of the data list
    while True:
        c = read(1)
        if c == '[': break

    buf = ''
    reading_object = False
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
                    X = x
                    yield x
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

def parse_addresses(addresses_raw):
    addresses = []
    for address_raw in addresses_raw:
        address = Address(address=address_raw['address'],
                          city=address_raw['city'],
                          state=address_raw['state'],
                          zip=address_raw['zip'],
                          phone=address_raw['phone'])
        addresses.append(address)
    return addresses


def parse_plans(plans_raw):
    plans = []
    for plan_raw in plans_raw:
        plan = Plan(network_tier=plan_raw['network_tier'],
                    plan_id     =plan_raw['plan_id'],
                    plan_id_type=plan_raw['plan_id_type'])
        plans.append(plan)
    return plans


def parse_name(name_raw):
    first = name_raw.get('first','')
    middle = name_raw.get('middle','')
    last = name_raw.get('last','')
    return ' '.join([first,middle,last])


def parse_individual_provider(provider_raw):
    individual = Individual(accepting      =provider_raw.get('accepting', []),
                            gender         =provider_raw.get('gender'),
                            languages      =provider_raw.get('languages', []),
                            last_updated_on=provider_raw.get('last_updated_on'),
                            npi            =provider_raw.get('npi'),
                            speciality     =provider_raw.get('speciality'),
                            name           =parse_name(provider_raw.get('name',{})),
                            addresses      =parse_addresses(provider_raw.get('addresses',[])),
                            plans          =parse_plans(provider_raw.get('plans',[])))
    return individual


def parse_facility_provider(provider_raw):
    facility = Facility(facility_name  =provider_raw['facility_name'],
                        facility_type  =provider_raw['facility_type'],
                        last_updated_on=provider_raw['last_updated_on'],
                        npi            =provider_raw['npi'],
                        addresses      =parse_addresses(provider_raw['addresses']),
                        plans          =parse_plans(provider_raw['plans']))
    return facility

def parse_providers(providers_raw):
    providers = []
    for provider_raw in providers_raw:
        provider_type = provider_raw['type']
        if provider_type=='INDIVIDUAL':
            providers.append(parse_individual_provider(provider_raw))
        elif provider_type=='FACILITY':
            providers.append(parse_facility_provider(provider_raw))
        else:
            logging.error("unknown provider type {}".format(provider_type))
    return providers

CSV_FIELDS = [
              'ISSUER_NAME',
              'PROVIDER_NAME',
              'PROVIDER_TYPE',
              'PLAN_ID'
             ]
def save_to_csv(issuer):
    out_file = open(out_filename,'w')
    csv_writer = csv.DictWriter(out_file, CSV_FIELDS)
    csv_writer.writeheader()
    logging.info("Writing issuer \"{}\" data to csv".format(issuer.issuer_name))
    for provider in issuer.providers:
        for plan in provider.plans:
            if type(provider) == Facility:
                name = provider.facility_name
                type_ = "Facility"
            else:
                name = provider.name
                type_ = "Individual"
            all_fields = {'ISSUER_NAME':issuer.issuer_name,
                          'PROVIDER_NAME': name,
                          'PROVIDER_TYPE': type_,
                          'PLAN_ID': plan.plan_id,
                          }
            csv_writer.writerow(all_fields)
    out_file.close()
