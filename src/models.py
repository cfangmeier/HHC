from enum import IntEnum
from datetime import date


def unique(xs):
    if xs is None:
        return set()
    xs = [x.lower() for x in xs if type(x) == str]
    return set(xs)


class Accepting(IntEnum):
    no_data = -1
    not_accepting = 0
    accepting = 1
    accepting_some_locations = 2

    @classmethod
    def lookup(cls, accept_str):
        if accept_str is None:
            return cls.no_data
        map_ = {'accepting': cls.accepting,
                'not accepting': cls.not_accepting,
                'accepting in some locations': cls.accepting_some_locations}
        return map_.get(accept_str.lower(), cls.no_data)


class ProviderType(IntEnum):
    no_type = -1
    facility = 1
    individual = 2

    @classmethod
    def lookup(cls, provider_str):
        if provider_str is None:
            return cls.no_data
        map_ = {'facility': cls.facility,
                'individual': cls.individual}
        return map_.get(provider_str.lower(), cls.no_type)


class URLType(IntEnum):
    prov = 0
    plan = 1
    drug = 2

    @classmethod
    def get_name(cls, type_):
        type_map = {cls.plan: "Plan",
                    cls.prov: "Provider",
                    cls.drug: "Drug"}
        return type_map.get(type_)


class IssuerGroup:
    def __init__(self):
        self.idx_issuer_group = None
        self.index_url = None
        self.index_status = None
        self.issuers = []
        self.data_urls = []


class IssuerGroupURL:
    def __init__(self, idx, url, url_type, status=""):
        self.idx_issuer_group = idx
        self.url = url
        self.url_type = url_type
        self.status = status


class Issuer:
    def __init__(self):
        self.idx_issuer_group = None
        self.id_issuer = None
        self.name = None
        self.state = None


class Plan:
    def __init__(self, plan_dict=None):
        if not plan_dict:
            self.id_plan = None
            self.id_plan_type = None
            self.marketing_name = None
            self.summary_url = None
        else:
            self.id_plan = plan_dict.get('plan_id')
            self.plan_id_type = plan_dict.get('plan_id_type')
            self.marketing_name = plan_dict.get('marketing_name')
            self.summary_url = plan_dict.get('summary_url')


class Address():
    def __init__(self, addr_dict=None):
        def f(s):
            if s is not None:
                return s.lower()
            return s

        if addr_dict is None:
            self.address = None
            self.city = None
            self.state = None
            self.zip_ = None
            self.phone = None
        else:
            self.address = f(addr_dict.get('address'))
            self.city = f(addr_dict.get('city'))
            self.state = f(addr_dict.get('state'))
            self.zip_ = addr_dict.get('zip')
            self.phone = addr_dict.get('phone')


class Provider:
    def __init__(self, prov_dict=None):
        if prov_dict is None:
            self.npi = -1
            self.type_ = ProviderType.no_type   # ProviderType Enum
            self.name = ""
            self.last_updated_on = None         # datetime object
            self.accepting = Accepting.no_data  # Accepting enum
            self.languages = []                 # Language strings
            self.specialties = []               # Specialty strings
            self.facility_types = []            # FacilityType strings
            self.plans = []                     # ProviderPlan objects
            self.addresses = []                 # Address objects
        else:
            try:
                self.npi = int(prov_dict['npi'])
            except:
                self.npi = None
            self.type_ = ProviderType.lookup(prov_dict.get('type'))
            if self.type_ == ProviderType.individual:
                name_dict = prov_dict.get('name', {})
                self.name = '|'.join([name_dict.get('first', 'N/A'),
                                      name_dict.get('last', 'N/A')])
            elif self.type_ == ProviderType.facility:
                self.name = prov_dict.get('facility_name', "N/A")
            else:
                self.name = None
            try:
                date_str = prov_dict['last_updated_on']
                year, month, day = map(int, date_str.split('-'))
                self.last_updated_on = date(year=year, month=month, day=day)
            except KeyError:
                self.last_updated_on = None
            self.accepting = Accepting.lookup(prov_dict.get('accepting'))
            self.languages = unique(prov_dict.get('languages', []))
            self.specialties = set.union(unique(prov_dict.get('specialty')),
                                         unique(prov_dict.get('speciality')))
            self.facility_types = unique(prov_dict.get('facility_type', []))
            self.plans = [ProviderPlan(provplan_dict)
                          for provplan_dict in prov_dict.get('plans', [])]
            self.addresses = [Address(addr_dict)
                              for addr_dict in prov_dict.get('addresses', [])]


class ProviderPlan:
    def __init__(self, provplan_dict=None):
        if provplan_dict is None:
            self.id_plan = None
            self.id_plan_type = None
            self.network_tier = None
        else:
            self.id_plan = provplan_dict.get('plan_id')
            self.id_plan_type = provplan_dict.get('plan_id_type')
            self.network_tier = provplan_dict.get('network_tier')


class Drug:
    def __init__(self, drug_dict=None):
        if drug_dict is None:
            self.rxnorm_id = None
            self.name = None
            self.plans = None
        else:
            self.rxnorm_id = drug_dict.get('rxnorm_id')
            self.name = drug_dict.get('drug_name')
            self.plans = [DrugPlan(drugplan_dict)
                          for drugplan_dict in drug_dict.get('plans', [])]


class DrugPlan:
    def __init__(self, drugplan_dict=None):
        if drugplan_dict is None:
            self.id_plan = None
            self.drug_tier = None
            self.prior_authorization = None
            self.step_therapy = None
            self.quantity_limit = None
        else:
            self.id_plan = drugplan_dict.get('plan_id')
            self.prior_authorization = drugplan_dict.get('prior_authorization')
            self.drug_tier = drugplan_dict.get('drug_tier')
            self.step_therapy = drugplan_dict.get('step_therapy')
            self.quantity_limit = drugplan_dict.get('quantity_limit')
