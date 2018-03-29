import audit

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

OSM_PATH = "map.osm"

NODES_PATH = "myrtle_nodes.csv"
NODE_TAGS_PATH = "myrtle_nodes_tags_f4.csv"
WAYS_PATH = "myrtle_ways_f4.csv"
WAY_NODES_PATH = "myrtle_ways_nodes_f4.csv"
WAY_TAGS_PATH = "myrtle_ways_tags_f4.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema

NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']

WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

def build_node_tag(node_id, key_k, value_v, tag_type):
  # Below checks to see if there's a colon in the tag_type and appropriately splits it into 'tag_type' or
  # key_k as appropriate
  # Otherwise it sets 'tag_type' to "regular" and keeps key_k as it is
  '''
  If colon exists:
    k = after colon characters
    v = characters before the colon
  If no colon exists:
    k = full tag (the tag_type)
    v = 'regular' (the value)
  '''
  # Note: key_k and tag_type arguments both come in as tag_type, aka tag.attrib['k'], not sure why I did this
  # might need to review it

  return_dict = {}
  full_string = tag_type

  addr_street_check = tag_type

  # Auditing for colons, and street name overabbreviations
  if(LOWER_COLON.search(tag_type)):   
    index = tag_type.index(':')
    tag_type = full_string[:index]
    key_k = full_string[index+1:]

    if(addr_street_check == "addr:street"):
      return_dict['value'] = audit.update_name(value_v, audit.mapping)      

    elif(addr_street_check == "addr:postcode"):
      return_dict['value'] = audit.update_zip(value_v)
    
    else:
      return_dict['value'] = value_v

  else:
    tag_type = 'regular'
    return_dict['value'] = value_v

  return_dict['id'] = node_id
  return_dict['type'] = tag_type
  return_dict['key'] = key_k

  # Note: return_dict['value'] hits one of the boolean clauses in order to get set to value_v
  # return_dict is now in the format of the following:
  # {'value': 'North Lincoln Ave', 'type': 'addr', 'id': '2406124091', 'key': 'street'}  

  return return_dict


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    tags = []  # Handle secondary tags the same way for both node and way elements
    node_final_dict = {}

    way_attribs = {}
    way_nodes = []

    if element.tag == 'node':
        for i in node_attr_fields:
            if element.attrib[i]:
                node_attribs[i] = element.attrib[i]

        # Note: node_attribs has all the attributes, we will add this to below to make dict {'node' : note_attribs}

        node_final_dict['node'] = node_attribs
        
        for tag in element.iter("tag"):          
          node_id = node_attribs['id'] 
          tag_type = tag.attrib['k'] 

          if(not PROBLEMCHARS.search(tag.attrib['k'])):
            node_tag = build_node_tag(node_id, tag.attrib['k'], tag.attrib['v'], tag_type)
            tags.append(node_tag)
         
        # Note: tags now is a list with a dict filled with tags, gets returned fit to the following code as given

        return {'node': node_attribs, 'node_tags': tags}


    # Code below builds the way attributes dictionary key and value:
    elif element.tag == 'way':
        for j in way_attr_fields:
            if element.attrib[j]:
                way_attribs[j] = element.attrib[j]

        # Resets the position to 0 in each main elif element.tag = "way" loop        
        position = 0

        for node_tag in element.iter("nd"):
            way_id = way_attribs['id']
            way_node_num = node_tag.attrib['ref']

            temp_way_dict = build_way_tag(way_id, way_node_num, position)
            way_nodes.append(temp_way_dict)
            position+=1

        for tag in element.iter("tag"):
          way_id = way_attribs['id']
          tag_type = tag.attrib['k']
          way_tag = build_node_tag(way_id, tag_type, tag.attrib['v'], tag_type)
          tags.append(way_tag)

        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# Description: build_way_tag function takes the parameters to create the way_node entry
# and returns a dictionary with the keys of the paraments
# 'id', 'node_id', and 'position'

def build_way_tag(way_id, way_node_num, position):
  return_dict = {}
  return_dict['id'] = way_id
  return_dict['node_id'] = way_node_num
  return_dict['position'] = position

  return return_dict


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if(elem.tag == "node" or elem.tag == "way"):
            for tag in elem.iter("tag"):
#                print(tag.attrib['k'])
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)

            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
