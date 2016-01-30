from collections import namedtuple as nt

Issuer = nt('Issuer', ['id_issuer',
                       'name',
                       'marketplace_category',
                       'url_submitted',
                       'state',
                       'plans'])

Plan = nt('Plan', ['idx_plan',
                   'id_plan',
                   'id_issuer',
                   'plan_id_type',
                   'marketing_name',
                   'summary_url'])
IDX_PLAN = 1
def build_plan_from_dict(issuer, plan_dict):
    global IDX_PLAN
    p = Plan(idx_plan=IDX_PLAN,
             id_plan=plan_dict['plan_id'],
             id_issuer=issuer.id_issuer,
             plan_id_type=plan_dict['plan_id_type'],
             marketing_name=plan_dict['marketing_name'],
             summary_url=plan_dict['summary_url'])
    IDX_PLAN+=1
    return p

Individual = nt('Individual', ['npi',
                               'name_first',
                               'name_last',
                               'last_updated_on',
                               'accepting',
                               'languages',
                               'specialties',
                               'plans',
                               'addresses'])
Facility = nt('Facility', ['npi',
                           'facility_name',
                           'last_updated_on',
                           'facility_types',
                           'plans',
                           'addresses'])

def build_provider_from_dict(issuer, plans, prov_dict):
    npi = prov_dict['npi']
    unique = lambda xs: list(set([x.lower() for x in xs]))
    if prov_dict['type'] == 'INDIVIDUAL':
        try:
            specialty = prov_dict['specialty']
        except KeyError:
            specialty = prov_dict['speciality']
        specialty = unique(specialty)
        prov = Individual(npi = npi,
                          name_first=prov_dict['name']['first'],
                          name_last=prov_dict['name']['last'],
                          last_updated_on=prov_dict['last_updated_on'],
                          accepting=prov_dict['accepting'],
                          languages=unique(prov_dict.get('languages',[])),
                          specialties=specialty,
                          plans=[],
                          addresses=[])
    else:
        prov = Facility(npi=npi,
                        facility_name=prov_dict['facility_name'],
                        last_updated_on=prov_dict['last_updated_on'],
                        facility_types=unique(prov_dict['facility_type']),
                        plans=[],
                        addresses=[])
    for plan_dict in prov_dict['plans']:
        plan = plans[(plan_dict['plan_id'],issuer.id_issuer)]
        prov.plans.append(plan)
    for addr_dict in prov_dict['addresses']:
        addr = build_address_from_dict(prov, addr_dict)
        prov.addresses.append(addr)
    return prov

Address = nt('Address', ['npi',
                         'address',
                         'city',
                         'state',
                         'zip',
                         'phone'])
def build_address_from_dict(provider, addr_dict):
    return Address(npi=provider.npi,
                   address=addr_dict['address'],
                   city=addr_dict['city'],
                   state=addr_dict['state'],
                   zip=addr_dict['zip'],
                   phone=addr_dict['phone'])

