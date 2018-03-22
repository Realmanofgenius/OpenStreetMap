# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 16:50:38 2018

@author: sunda
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Your task is to use the iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
Fill out the count_tags function. It should return a dictionary with the 
tag name as the key and number of times this tag can be encountered in 
the map as value.
Note that your code will be tested with a different data file than the 'example.osm'
"""
import xml.etree.cElementTree as ET
import pprint

## The mappaerser.py quiz asked to fill out the count_tags function
## count_tags function works as intended
def count_tags(filename):
        tags = {}
        tree = ET.iterparse(filename)

        ''' Tree is a bunch of tuples, the 2nd value of the tuple is the element. use .tag to get element name '''

        for i, j in tree:
        	'''print j.tag'''
	        tags = addtag(j.tag, tags)
        
        return tags

def addtag(tag_name, tags):
	if tag_name in tags:
		count = tags[tag_name]
		count += 1
		tags[tag_name] = count
	else:
		tags[tag_name] = 1

	return tags


def test():
    tags = count_tags('map.osm')
    pprint.pprint(tags)

'''
    assert tags == {'bounds': 1,
                     'member': 3,
                     'nd': 4,
                     'node': 20,
                     'osm': 1,
                     'relation': 1,
                     'tag': 7,
                     'way': 1}
'''
    

if __name__ == "__main__":
    test()