## Interoperable Watershed Network – Configuration for SOS Data Ingestion

This document describes the contents of important files for ingestion of observation data into an Interoperable Watershed Network (IWN) 52N SOS-based data appliance. The IWN is EPA's pilot portal for discovery of and access to continuous water quality sensor data collected by agencies around the United States. The IWN consists of three components:
* Distributed "data appliances" that agencies use to make their data and associated metadata available through standardized web services;
* A centralized catalog that harvests metadata from registered data appliances; and
* A discovery tool that leverages the data appliance and catalog APIs to deliver search and access capabilities.
The IWN-ingest.py tool described below supports the transfer of data into the data appliances' Open Geospatial Consortium(OGC) Sensor Observation Service (SOS) database.

The material in this repository is made available under the [MIT License](https://opensource.org/licenses/MIT).

## IWN-ingest.py

IWN-ingest.py contains the main body of the IWN ingestion script. When invoked, the user should specify the CSV-formatted file containing observation data to be ingested, and the accompanying JSON-fomatted station configuration file:

 IWN-ingest _observations_.csv _station_.json    or

 IWN-ingest _observations_.csv input/_station_.json

The ingestion code checks the SOS database to identify the most recent available observation for a given parameter and station, and only uploads observations that are more recent. Two typical use cases have been identified in the IWN pilot project: batch and near real-time.

### Batch (manual) operation

For batch operations – the occasional ingestions of long-term, typically historical and lengthy records – direct use of the ingestion program is suggested:

 ~/anaconda/python IWN-ingest _historical_.csv input/_station_.json (Linux example)

### Near real-time

For continuous near real-time operations, scheduled invocation of a .sh (Linux) or .bat/.vbs (Windows) script is suggested. The following Linux script pulls an observation file from another site, creates a truncated data file with only the most recent data points, and ingests it:

wget [http://someplace/posted.csv](http://someplace/posted.csv)
head posted.csv &gt; nrt.csv
tail -10 posted.csv &gt;&gt; nrt.csv
~/anaconda/python IWN-ingest _nrt_.csv input/_station_.json

## dataconvert.py

dataconvert.py is called by IWN-ingest.py to parse the CSV-formatted file potentially containing observations for multiple parameters and stations into &quot;chunks&quot; containing observations for a single parameter and station.

## Station configuration file (_station_.json)

The station configuration file is used to guide the parsing of CSV-formatted observations into chunks for ingestion. The file contains four elements:

- File type – indicates general format of the CSV file:
  - &quot;type&quot;:1 – data for a single station, date and time are in separate columns
  - &quot;type&quot;:2 – data for a single station, date and time are in a single column
  - &quot;type&quot;:3 – data for multiple stations, date and time are in a single column
- Columns – array that identifies which parameter in the metadata\_parameter file (see below) corresponds to each column in the CSV file:
  - &quot;columns&quot;:[&quot;date&quot;,&quot;time&quot;, &quot;pH&quot;] interprets the first two columns as the datetime stamp and the third column as pH measurements
  - &quot;columns&quot;:[&quot;&quot;,&quot;datetime&quot;, &quot;pH&quot;] skips the first column, presents the second column as the datetime stamp, and uses the third column for pH
  - Valid array values are &quot;&quot; (skip), &quot;date&quot;, &quot;time&quot;, &quot;datetime&quot;, or any match for &quot;parameter&quot; values entered in metadata\_parameter.csv
- Station – identifies the station entry in the metadata\_station file:
  - &quot;station&quot;: &quot;stationid&quot;
  - The station string must match a stationid value in the metadata\_station file.
- Header – specifies how many lines to ignore at the beginning of the file:
  - &quot;header&quot;: 3

The following example is shown to demonstrate formatting:

{

    "type":2,
    
    "columns":[
    
        "datetime",
        
        "water_level",
        
        "ph",
        
        "dissolved_oxygen",
        
        "specific_conductance",
        
        "temperature",
        
        "rainfall"],
        
    "station":"grssy0",
    
    "header":7
    
}

## Station metadata file (station\_metadata.csv)

The station metadata file station\_metadata.csv must be stored in the ./input subdirectory. It should contain the following elements in the indicated order for each station in CSV format, with the data starting on line 2 of the file:

- Station ID – a text field unique to the station containing no spaces. This field is used to (1) link an ingested .CSV file&#39;s contents to a station through the JSON station configuration file&#39;s &quot;station&quot; element, and (2) as part of the URNs used to uniquely identify stations, sensors, offerings, features, and result templates in IWN data appliances.
- Short Name – A text field containing a unique short descriptive name for the station.
- Long Name – A text field containing a unique long descriptive name for the station.
- Longitude – Longitude of station location in decimal degrees.
- Latitude – Latitude of station location in decimal degrees.
- Altitude – Altitude of station location in meters.
- Organization – A text field containing the name of the organization/office collecting the observations.
- Web URL – A URL linking to a web page specified by the organization, presumable relevant to the observations being collected.
- Contact – An e-mail address for a party responsible for the collected observations.
- Waterbody Type – A text field indicating the type of waterbody from which observations are collected. Examples include Estuary, Lake, River/Stream, Outfall
- Publisher – Organization publishing the data
- Data Status – Text field indicating QA/QC stats of the data. Examples include raw, provisional and final.
- URN org part – A text field with no spaces unique to the organization collecting the data, used in the SOS URNs specifying stations, sensors, offerings, features and result templates.
- URN suborg A text field with no spaces unique to the sub-organization (bureau, office, etc.) collecting the data, used in the SOS URNs specifying stations, sensors, offerings, features and result templates.

## Parameter metadata file (parameter\_metadata.csv)

The station metadata file station\_metadata.csv must be stored in the ./input subdirectory. It should contain the following elements in the indicated order for each parameter in CSV format, with the data starting on line 2 of the file:

- Parameter – a text field unique to the station containing no spaces. This field is used to (1) link an ingested .CSV file&#39;s contents to a parameter through the JSON station configuration file&#39;s &quot;column&quot; element, and (2) as part of the URNs used to uniquely identify stations, sensors, offerings, features, and result templates in IWN data appliances.
- Parameter Name – Pretty name for parameter used in SOS templates; currently (52N SOS 4.3.x) must be entered manually in database.
- Parameter Units – Units for the parameter
- Parameter Field Name – A text field containing no spaces that is used for naming the observable property element in SOS.
- Status – Text field indicating QA/QC stats of the data. Examples include raw, provisional and final.
Rendered
