# Solr Stats Analyzer

[solr_stats.py](solr_stats.py) is a tool that analyzes Navigator metadata stored
in Solr cores. It is built on top of a light-weight python API wrapper for Solr 4.x
with some Navigator-specific abstractions added.

To run this tool, you must have a python environment setup. The dependencies can be
installed using `pip install -r requirements.txt` from within the tools directory.
Any missing modules can be fixed by install [Anaconda](https://www.continuum.io/downloads)

The tool must be configured to reference Navigator by create a configuration file
containing:

    `name,host,port,user,password`

e.g., `customer1,foo.cloudera.com,1234,user,password`

Once the configuration file is created, run the tool with the following command format:

    `solr_stats.py <config_file_path> <output_xlsx_path>`

Note that the output path must have an `.xlsx` extension for the Excel writer to work
properly.

# Navigator Report, Optimizer and Google Fusion Data Generation

[gen_reports.py](gen_reports.py) is a script that generates Navigator metadata
 analysis from [solr_stats.py](solr_stats.py), and input files required by Optimizer and 
 Sigma from [QueryExtraction.java](../examples/src/main/java/com/cloudera/nav/sdk/examples/extraction/QueryExtraction.java)
 
 Dependencies:
 
 Maven 3 Click [here](https://maven.apache.org/install.html) for Installation
 
 Compile the SDK project in the navigator-sdk folder, type `mvn clean install`
 
 Python Modules can be installed using `pip install -r requirements.txt` from within the tools directory.
 
 Update [query_extraction.sh](query_extraction.sh) permission by `chmod +x query_extraction.sh` from within the tools directory.
 
 The script requires a configuration file. The [query-extraction-sample.conf](../examples/src/main/resources/query-extraction-sample.conf) file contains all the options that's currently supported.
 
 To run the script in the tools directory:
 
    `python gen_reports.py <config_file_path>`

After the script finished running, the files will be under `output_directory` set by the config file.