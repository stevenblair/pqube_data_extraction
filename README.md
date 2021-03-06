# PQube Data Extraction

This repository contains code for importing power quality monitoring data from PQube 2 devices, and converting the data into a consistent and efficient format using PyTables. It was used for analysing monitoring data captured during the [C2C project](http://www.enwl.co.uk/c2c), led by Electricity North West Limited.

The code is designed to import PQube data from multiple different batches over time into a common PyTables file (or files for the detailed harmonic data). It currently supports extracting the 5-minute sampled data, but could be extended to extract the 1-minute sampled data.

An example of the output data generated by the `batch-import.py` script is available here: https://strathcloud.sharefile.eu/share?cmd=d&id=s54bf049e5e142b0a#/view/s54bf049e5e142b0a?_k=jxa57e. This data can also be viewed graphically at http://c2c.eee.strath.ac.uk/.