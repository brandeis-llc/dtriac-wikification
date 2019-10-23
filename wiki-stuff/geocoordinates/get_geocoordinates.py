# get_geocoordinates.py

import argparse
import glob
import json
import os
import time

from bs4 import BeautifulSoup

"""
Input: kml file of testing operations and DBPedia file with infobox data

Output: Each operation associated with subevents, geocoordinates, and other metadata about testing events
"""

"""
Sample command line:

python3 get_geocoordinates.py \
--geo_path ./geocoordinates/ \
--infobox_kb ../data/infobox_properties_en.ttl \
--write_path ./operations_geocoordinates_metadata.json

"""


def get_geo_data(geo_path):
	os.chdir(geo_path)

	operations_metadata = {}

	for file in glob.glob("*.kml"):

		infile = open(file, "r")
		contents = infile.read()
		soup = BeautifulSoup(contents, 'xml')
		placemarks = soup.find_all('Placemark')

		title = "operation_" + file.replace('.kml', '').lower()
		print(title)
		operations_metadata[title] = {"subevents_with_geocoordinates": {}}

		for pm in placemarks:
			name = str(pm.find_all('name')[0])
			coord = str(pm.find_all('coordinates')[0])
			operations_metadata[title]['subevents_with_geocoordinates'][name] = coord

	return operations_metadata


def retrieve_infobox_data(infobox_kb_path, operations_metadata):

	titles = list(operations_metadata.keys())

	with open(infobox_kb_path, 'r') as infile:
		for line in infile:
			for title in titles:
				if 'resource/'+title in line.lower():

					try:

						s = line.split()[0].split('/')[-1][:-1].split('_')[0] \
							+ '_' + line.split()[0].split('/')[-1][:-1].split('_')[1]

						s = s.lower()

						p = line.split()[1].replace('<http://dbpedia.org/property/', '')

						if p[-1] == '>':
							p = p[:-1]

						p = p.replace('"', '')

						o = line.split('> ')[2]

						if '^^' in o:
							attr = o.split('^^')[0]
						else:
							attr = o.split('/')[-1]

							if attr[-1] == '>':
								attr = attr[:-1]

						attr = attr.replace('"', '')

						if s in operations_metadata:
							operations_metadata[s][p] = attr

					except IndexError:
						print(line)

	for title, value in operations_metadata.items():
		print(title)
		print(value)
		print()

	return operations_metadata


def write_to_disk(path, operations_metadata):

	with open(path, 'w') as outfile:
		json.dump(operations_metadata, outfile, indent=4)


def main():
	print()
	print("Getting geocoordinates and metadata for Operations of interest")
	print()

	parser = argparse.ArgumentParser(description='Compiling info for Operations of interest.')
	parser.add_argument('--geo_path', required=True, type=os.path.abspath,
						help='path to the geocoordinates directory')
	parser.add_argument('--infobox_kb', required=True, type=os.path.abspath,
						help='path to the wiki infoboxes KB')
	parser.add_argument('--write_path', required=True, type=os.path.abspath,
						help='path to write the JSON output file')

	opt = parser.parse_args()
	print()
	print(opt)
	print()

	operations_metadata = get_geo_data(opt.geo_path)

	toc = time.time()
	operations_metadata = retrieve_infobox_data(opt.infobox_kb, operations_metadata)
	tic = time.time()
	print("Time needed to retrieve infobox:", tic-toc)

	write_to_disk(opt.write_path, operations_metadata)

	print("Written to path:", opt.write_path)
	print()


if __name__ == '__main__':
	main()