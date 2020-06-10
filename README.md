# UCSF MyBPLab 2.0 Data Extraction
Code in this repository downloads UCSF MyBPLab 2.0 data from Sage's [Synapse](https://synapse.org) platform and merges, cleans, and formats data for further analysis. 

## Installing Python and Required Python Packages
In order to run this code you will first need to download and install [Python 3.X.X](https://www.python.org/downloads/). Any version of Python 3 will work, but on Windows it is important to make sure the box "Add to path" in the window that emerges when you open the .exe file **before** clicking "Install". After installing Python 3, you will need to download this repository, via the green download box at the top of this github page and extract data if zipped. Then you will need to open Command Prompt on Windows (Terminal window on mac), and navigate to the directory (folder) where the code has been downloaded (example of how to navigate directories via Command Prompt [here](https://www.youtube.com/watch?v=MBBWVgE0ewk)). Next, you will need to install required Python packages via running the following command in terminal:

    pip install -r requirements.txt 
    
On some windows machines instead use the following command:

    py -m pip install -r requirements.txt

## Running the code to Download and Format Data
To download, format, and output data simply run the following command to run the [get_my_bp_lab_data.py](get_my_bp_lab_data.py) program:

    python get_my_bp_lab_data.py
    
On some windows machines instead use the following command:

    py get_my_bp_lab_data.py

The program will prompt you to enter your Synapse credentials to access MyBPLab data. Note that the first time the program runs it may take several hours to download data from Synapse, due to known limitations in the Synapse API. After the first run, the program takes ~10 minutes to run. Data will be downloaded to a ```data_results``` folder and will contain several sub-folders:
* bodymap_data
* check_in_and_ep_data
* cog_task_data
* intervention_task_data


Formatted data CSV files will be in the appropriate sub-folder above. Note: the Synapse API throws several errors when downloading Synapse tables such as the one that follows. These should not cause alarm, and simply indicate that Synapse's memory mangement is not optimal:

    [WARNING] get_my_bp_lab_data.py:44: DtypeWarning: Columns (8,12,30,31,32,33,34,35,36,37,91) have mixed types.Specify dtype option on import or set low_memory=False.

*Originally developed by David Feldman (davidelanfeldman@gmail.com)*

