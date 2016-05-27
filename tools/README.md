# Solr Stats Analyzer

[solr_stats.py](tools/solr_stats.py) is a tool that analyzes Navigator metadata stored
in Solr cores. It is built on top of a light-weight python API wrapper for Solr 4.x
with some Navigator-specific abstractions added.

To run this tool, you must have a python environment setup. The dependencies can be
installed using `pip install -r requirements.txt` from within the tools directory.

The tool must be configured to reference Navigator by create a configuration file
containing:

    `name,host,port,user,password`

e.g., `customer1,foo.cloudera.com,1234,user,password`

Once the configuration file is created, run the tool with the following command format:

    `solr_stats.py <config_file_path> <output_xlsx_path>`

Note that the output path must have an `.xlsx` extension for the Excel writer to work
properly.