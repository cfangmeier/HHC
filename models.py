from enum import IntEnum
from datetime import date


def unique(xs):
    if type(xs) is not list: return []
    else: return list(set([x.lower() for x in xs]))

class Accepting(IntEnum):
    not_accepting = 0
    accepting = 1
    accepting_some_locations = 2
    no_data = -1

class ProviderType(IntEnum):
    no_type = 0
    facility = 1
    individual = 2

def decode_accepting(accepting_string):
    str_map = {'accepting':Accepting.accepting,
               'not accepting':Accepting.not_accepting,
               'accepting in some locations': Accepting.accepting_some_locations,
               }
    try:
        return str_map[accepting_string.lower()]
    except (KeyError, AttributeError):
        return Accepting.no_data

def decode_date(date_str):
    try:
        year, month, day = map(int, date_str.split('-'))
        return date(year=year, month=month, day=day)
    except KeyError:
        return None

def decode_provider_type(prov_dict):
    str_map = {'individual':ProviderType.individual,
               'facility':ProviderType.facility,
               }
    return str_map[prov_dict['type'].lower()]

def decode_individual_name(name_dict):
    return '|'.join([name_dict.get('first','N/A'),
                     name_dict.get('last', 'N/A')])


class Issuer:
    def __init__(self):
        self.id_issuer = -1
        self.name = ""
        self.marketplace_category = ""
        self.url_submitted = ""
        self.state = ""
        self.plans = []

class Plan:
    def __init__(self):
        self.idx_plan = -1
        self.id_plan = -1
        self.issuer = None
        self.plan_id_type = ""
        self.marketing_name = ""
        self.summary_url = ""

def build_plan_from_dict(issuer, plan_dict):
    try:
        p = Plan()
        p.id_plan        = plan_dict['plan_id']
        p.issuer         = issuer
        p.plan_id_type   = plan_dict['plan_id_type']
        p.marketing_name = plan_dict['marketing_name']
        p.summary_url    = plan_dict['summary_url']
    except KeyError as e:
        raise ValueError("Missing field \"{}\" in plan json with parsed dict \n{}\n".format(e,plan_dict))
    return p

class Address():
    def __init__(self):
        self.provider = None
        self.address = None
        self.city = None
        self.state = None
        self.zip = None
        self.phone = None

def add_address(provider, addr_dict):
    from healthcare_cms_pull import CONFIG
    addr = Address()
    addr.provider=provider
    addr.zip=addr_dict.get('zip',None)
    if CONFIG['FULL_ADDRESS']:
        addr.address=addr_dict.get('address',None)
        addr.city=addr_dict.get('city',None)
        addr.state=addr_dict.get('state',None)
        addr.phone=addr_dict.get('phone',None)
    provider.addresses.append(addr)

class Provider:
    def __init__(self):
        self.idx_provider = -1
        self.npi = -1
        self.name = ""
        self.type = ProviderType.no_type
        self.last_updated_on = None
        self.accepting = Accepting.no_data
        self.languages = []
        self.specialties = []
        self.facility_types = []
        self.plans = []
        self.addresses = []

class FacilityType():
    def __init__(self, facility_type, idx_facility_type=None):
        self.idx_facility_type = idx_facility_type
        self.facility_type = facility_type

class Language():
    def __init__(self, language, idx_language=None):
        self.idx_language = idx_language
        self.language = language

class Specialty():
    def __init__(self, specialty, idx_specialty=None):
        self.idx_specialty = idx_specialty
        self.specialty = specialty

def build_provider_from_dict(issuer, plans, prov_dict, config):
    prov = Provider()
    prov.type = ProviderType.individual
    prov.npi = prov_dict.get('npi',None)
    if prov.npi is not None: prov.npi = int(prov.npi)
    prov.last_updated_on = decode_date(prov_dict.get('last_updated_on',{}))
    prov.accepting = decode_accepting(prov_dict.get('accepting',None))
    prov.languages = [Language(language) for language in unique(prov_dict.get('languages', []))]
    prov.specialties = [Specialty(specialty) for specialty in unique(prov_dict.get('specialty', [])+
                                                                     prov_dict.get('speciality',[]))]
    prov.facility_types=[FacilityType(facility_type) for facility_type in unique(prov_dict.get('facility_type',[]))]
    for plan_dict in prov_dict.get('plans', []):
        plan = plans[plan_dict['plan_id']]
        prov.plans.append(plan)
    for addr_dict in prov_dict.get('addresses', []):
        add_address(prov, addr_dict, config)

    type_ = prov_dict.get('type','').lower()
    if type_ == 'individual':
        prov.type = ProviderType.individual
        prov.name = decode_individual_name(prov_dict.get('name',{}))
    if type_ == 'facility':
        prov.type = ProviderType.facility
        prov.name = prov_dict.get('facility_name',"N/A")
    return prov

