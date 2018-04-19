# -*- coding: utf-8 -*-
"""
Your task in this exercise has two steps:
- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.
    You have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

SAMPLE_FILE = "MyrtleBeach.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Rd", "Rd.", "Ave", "Ave.", "Hwy", "Hwy.",
            "E", "E.", "W", "W.", "S", "S.", "N", "N."]

# UPDATE THIS VARIABLE
mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Hwy": "Highway",
            "Hwy.": "Highway",
            "Rd": "Road",
            "Rd.": "Road",
            "E": "East",
            "E.": "East",
            "W": "West",
            "W.": "West",
            "N": "North",
            "N.": "North",
            "S": "South",
            "S.": "South"
            }


def update_name(name, mapping):
    # YOUR CODE HERE
    s = street_type_re.search(name)
    if s:
        name = name.replace(s.group(0), mapping[s.group(0)])

    return name

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):

    #osm_file = open(osmfile, "r") -- Problem 1 -- Encoding
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()

    # output street types into a txtfile
    with open('street_types.txt', 'wb') as out:
        for street_type in sorted(street_types):
            out.write(street_type + '\n')
    return street_types


def test():
    st_types = audit(SAMPLE_FILE)
    #pprint.pprint(st_types)
    
    #for st_type, ways in st_types.iteritems():
    for st_type, ways in st_types.items():
        for name in ways:
            better_name = update_name(name, mapping)
            #print name, "=>", better_name
            print("{0} => {1}".format(name, better_name))
            if name == "Connector Rd":
                assert better_name == "Connector Road"
            if name == "N Kings Hwy":
                assert better_name == "N Kings Highway"


if __name__ == '__main__':
    test()