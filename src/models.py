from enum import IntEnum
from datetime import date


def unique(xs):
    if type(xs) is not list: return []
    else: return list(set([x.lower() for x in xs if x is not None]))

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


class IssuerGroup:
    def __init__(self):
        self.idx_issuer_group = -1
        self.url_submitted = ""
        self.url_status = ""
        self.issuers = []
        self.plan_urls = []
        self.provider_urls = []
        self.formulary_urls = []

class Issuer:
    def __init__(self):
        self.issuer_group = None
        self.id_issuer = -1
        self.name = None
        self.marketplace_category = None
        self.state = None
        self.plans = []

class Plan:
    def __init__(self):
        self.idx_plan = -1
        self.id_plan = -1
        self.issuer = None
        self.plan_id_type = ""
        self.marketing_name = ""
        self.summary_url = ""

def build_plan_from_dict(issuers, plan_dict, fake=False):
    try:
        p = Plan()
        fake_issuer = None
        p.id_plan        = plan_dict['plan_id']
        p.plan_id_type   = plan_dict['plan_id_type']
        if fake:
            p.marketing_name = "N/A"
            p.summary_url    = "N/A"
        else:
            p.marketing_name = plan_dict['marketing_name']
            p.summary_url    = plan_dict['summary_url']
        id_issuer = int(p.id_plan[:5])
        try:
            p.issuer         = issuers[id_issuer]
        except:
            fake_issuer = Issuer()
            fake_issuer.id_issuer = id_issuer
            p.issuer = fake_issuer
    except KeyError as e:
        raise ValueError("Missing field \"{}\" in plan json with parsed dict \n{}\n".format(e,plan_dict))
    return p, fake_issuer

class Address():
    def __init__(self):
        self.provider = None
        self.address = None
        self.city = None
        self.state = None
        self.zip = None
        self.phone = None

def add_address(provider, addr_dict):
    addr = Address()
    addr.provider=provider
    addr.zip=addr_dict.get('zip',None)
    if 'address' in addr_dict:
        addr.address = addr_dict['address'].lower()
    if 'city' in addr_dict:
        addr.city = addr_dict['city'].lower()
    if 'state' in addr_dict:
        addr.state = addr_dict['state'].lower()
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

class ProviderPlan:
    def __init__(self):
        self.provider = None
        self.plan = None
        self.network_tier = None

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

def build_provider_from_dict(issuers, plans, prov_dict):
    prov = Provider()
    prov.type = ProviderType.individual
    prov.npi = prov_dict.get('npi',None)
    if prov.npi is not None:
        try:
            prov.npi = int(prov.npi)
        except ValueError:
            prov.npi = None
    prov.last_updated_on = decode_date(prov_dict.get('last_updated_on',{}))
    prov.accepting = decode_accepting(prov_dict.get('accepting',None))
    prov.languages = [Language(language) for language in unique(prov_dict.get('languages', []))]
    prov.specialties = [Specialty(specialty)
                            for specialty in unique(prov_dict.get('specialty', [])+
                                                    prov_dict.get('speciality',[]))]
    prov.facility_types=[FacilityType(facility_type)
                             for facility_type in unique(prov_dict.get('facility_type',[]))]

    fake_plans = {}
    fake_issuers = {}
    for plan_dict in prov_dict.get('plans', []):
        id_plan = plan_dict['plan_id']
        if id_plan not in plans:
            plan, fake_issuer = build_plan_from_dict(issuers,plan_dict, fake=True)
            fake_plans[id_plan] = plan
            if fake_issuer:
                fake_issuers[fake_issuer.id_issuer] = fake_issuer
        else:
            plan = plans[id_plan]
        prov_plan = ProviderPlan()
        prov_plan.plan = plan
        prov_plan.provider = prov
        prov_plan.network_tier = plan_dict.get('network_tier',None)
        prov.plans.append(prov_plan)
    for addr_dict in prov_dict.get('addresses', []):
        add_address(prov, addr_dict)

    type_ = prov_dict.get('type','').lower()
    if type_ == 'individual':
        prov.type = ProviderType.individual
        prov.name = decode_individual_name(prov_dict.get('name',{}))
    if type_ == 'facility':
        prov.type = ProviderType.facility
        prov.name = prov_dict.get('facility_name',"N/A")
    return prov, fake_plans, fake_issuers

class Drug:
    def __init__(self):
        self.idx_drug = -1
        self.rxnorm_id = -1
        self.drug_name = ""
        self.plans = []


class DrugPlan:
    def __init__(self):
        self.drug = None
        self.plan = None
        self.drug_tier = None
        self.prior_authorization = None
        self.step_therapy = None
        self.quantity_limit = None

def build_drug_from_dict(issuers, plans, drug_dict):
    drug = Drug()
    drug.rxnorm_id = drug_dict['rxnorm_id']
    drug.drug_name = drug_dict['drug_name']

    fake_plans = {}
    fake_issuers = {}
    for plan_dict in drug_dict.get('plans', []):
        id_plan = plan_dict['plan_id']
        if id_plan not in plans:
            plan, fake_issuer = build_plan_from_dict(issuers,plan_dict, fake=True)
            fake_plans[id_plan] = plan
            if fake_issuer:
                fake_issuers[fake_issuer.id_issuer] = fake_issuer
        else:
            plan = plans[id_plan]
        drug_plan = DrugPlan()
        drug_plan.drug = drug
        drug_plan.plan = plan
        drug_plan.prior_authorization = plan_dict.get('prior_authorization',None)
        drug_plan.drug_tier = plan_dict.get('drug_tier',None)
        drug_plan.step_therapy = plan_dict.get('step_therapy',None)
        drug_plan.quantity_limit = plan_dict.get('quantity_limit',None)
        drug.plans.append(drug_plan)
    return drug, fake_plans, fake_issuers
