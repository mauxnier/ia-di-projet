import os
import glob
from lxml import etree

# Define a function to parse an XML file and convert it to a list of dictionaries
def parse_xml_file(xml_file):
    flow_data = []
    tree = etree.parse(xml_file)
    root = tree.getroot()

    for flow_element in root:
        flow_dict = {}
        for child in flow_element:
            flow_dict[child.tag] = child.text
        flow_data.append(flow_dict)

    return flow_data

# Specify the directory containing the XML files
xml_files_dir = 'TRAIN_ENSIBS'

# Get a list of all XML files in the directory
xml_files = glob.glob(os.path.join(xml_files_dir, '*.xml'))

# Initialize an empty list to store all flow data
all_flow_data = []

# Iterate through each XML file and parse it
for xml_file in xml_files:
    flow_data = parse_xml_file(xml_file)
    all_flow_data.extend(flow_data)

# Now, all_flow_data contains a list of dictionaries, where each dictionary corresponds to a single flow
# You can access and manipulate the data as needed
print("Longueur: ", len(all_flow_data))
# print("Premier élément: ", all_flow_data[0])