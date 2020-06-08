# UCSF MyBPLab 2.0 Data Extraction
Code in this repository downloads UCSF MyBPLab 2.0 data from Sage's [Synapse](synapse.org) platform and merges, cleans, and formats data for further analysis. 

## Installing Python and Required Python Packages
In order to run this code you will first need to download and install [Python 3.X.X](https://www.python.org/downloads/). After installing Python 3, you will need to download this repository, via the link above. Then you will need to open a Command Prompt window on Windows (Terminal window on mac), and navigate to the directory (folder) where the code has been downloaded (example of how to move directories [here](https://www.youtube.com/watch?v=MBBWVgE0ewk). Next, you will need to install required Python packages via running the following command in command prompt/terminal:
    pip install -r requirements.txt

## Downloading and Formatting Data
To download format, and output data simply run the following command:
   python get_my_bp_lab_data.py

*Originally developed by David Feldman (davidelanfeldman@gmail.com)*

