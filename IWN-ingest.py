#*****************
# Import modules
#*****************
import urllib.request as ur
import urllib.parse
import xml.etree.ElementTree as ET 
from pprint import pprint
import csv
from datetime import datetime
import dateutil.parser
import dataconvert
import os
from glob import glob
import argparse
from xml.sax.saxutils import escape
#*****************
# Global variables
#*****************
VERBOSE = False
#url = "http://ec2-52-43-146-68.us-west-2.compute.amazonaws.com:8080/52b-sos-webapp-437/service"
#url = "http://45.55.86.218:8080/52n-sos-webapp/service"
#url = "http://localhost:8080/52n-sos-webapp/service"
url="http://ec2-35-166-3-66.us-west-2.compute.amazonaws.com:8080/52n-sos-webapp/service"
debugprint = True
station_meta_path="input/metadata_station.csv"
parameter_meta_path="input/metadata_parameter.csv"
station_meta_template="input/station_template.txt"
sensor_meta_template="input/sensor_template.txt"
result_meta_template="input/result_template.txt"
data_template="input/data_template.txt"
LOG_FILE="debug/log.txt"
#DATA_VALUES1="input/data_text.csv"
DATA_VALUES1 ="tempdata.csv"
DEFAULT_DATE = datetime(2999,1,1)

#station metadata file must have columns in the following order
metadata_headers=[
    'stationid',
    'shortName',
    'longName',
    'easting',
    'northing',
    'altitude',
    'organizationName',
    'organizationURL',
    'contact',
    'waterbodyType',
    'publisher',
    'status',
    'urn-org',
    'suborg']

#parameter metadata file must have columns in the following order
parameter_headers=[
    'parameter',
    'parameterName',
    'parameterUnit',
    'fieldName',
    'status',
    'comment']

#Shortcuts to information in GetCapabiities
offerkey = [
    ['id', "./ns5:identifier", 1],
    ['name',"./ns5:name", 1],
    ['procedure',"./ns5:procedure",1],
    ['proc_fmt',"./ns5:procedureDescriptionFormat",5],
    ['obsprop',"./ns5:observableProperty",5],
    ['respfmt',"./ns0:responseFormat",7],
    ['obstype',"./ns0:observationType",1],
    ['featureofinttype',"./ns0:featureOfInterestType",1],
    ['phenombegin',"./ns0:phenomenonTime/ns6:TimePeriod/ns6:beginPosition",1],
    ['phenomend',"./ns0:phenomenonTime/ns6:TimePeriod/ns6:endPosition",1],
    ['resultbegin',"./ns0:resultTime/ns6:TimePeriod/ns6:beginPosition",1],
    ['resultend',"./ns0:resultTime/ns6:TimePeriod/ns6:endPosition",1]]

#Shorthand for namespaces in xml
namespaces = {
    'ns0':"http://www.opengis.net/sos/2.0",
    'ns2':"http://www.opengis.net/ows/1.1",
    'ns3':"http://www.w3.org/1999/xlink",
    'ns4':"http://www.opengis.net/fes/2.0",
    'ns5':"http://www.opengis.net/swes/2.0",
    'ns6':"http://www.opengis.net/gml/3.2",
    'xsi':"http://www.w3.org/2001/XMLSchema-instance",
    'gda':"http://www.opengis.net/sosgda/1.0",
    'gml':"http://www.opengis.net/gml/3.2"}

#*****************
# Functions
#*****************

def log_entry(symbol, log_text):
    """
    Helper function to create log entry
    """
    with open(LOG_FILE,'a') as fo:
        fo.write("{} [{}] - {}\n".format(symbol, str(datetime.now()),log_text))

def create_offer_dict(noff):
    off_d = {}
    #cycle through every offering of interest (global variable) and create dict of individual offer
    for field in offerkey:
        for i in range(field[2]):
            off_d[field[0]] = [j.text for j in noff.findall(field[1],namespaces)]
    return off_d

def parse_capabilities(url):
    """
    Requests GetCapabilities and returns a list made up of a dictionary of each observation
    offering
    """
    #request GetCapabilities
    r = ur.urlopen(url+"?service=SOS&request=GetCapabilities")
    #create walkable tree of xml response
    tree = ET.parse(r)
    #write the capabilities xml file to capabilities.xml
    with open('debug/capabilities.xml','wb') as of:
        tree.write(of)
    root = tree.getroot()
    #create a list of all ObservationOffering
    offerings_l = root.findall('./ns0:contents/ns0:Contents/ns5:offering/ns0:ObservationOffering',namespaces)
    offer_list = []
    for noffer in offerings_l:
        offer_dict = create_offer_dict(noffer)
        offer_list.append(offer_dict)
    if debugprint:
        with open('debug/capabilities.txt','w') as of:
            pprint(offer_list,stream=of)
    return offer_list

def pull_capability_data(offer_list):
    #write the list of offerings to debug/offer.csv for review
    with open('debug/offer.csv','w') as fo:
        fo.write("id,org,suborg,stationid,status,parameter,phenombegin,phenomend,resbegin,resend\n")
        unique_offers = []
        for i in offer_list:
            try:
                resbegin = i['resultbegin'][0]
            except IndexError:
                resbegin = ""
            try:
                resend = i['resultend'][0]
            except IndexError:
                resend = ""
            try:
                phenombegin = i['phenombegin'][0]
            except IndexError:
                phenombegin = ""
            try:
                phenomend = i['phenomend'][0]
            except IndexError:
                phenomend = ""
            nid = i['id'][0]
            nid_sp = nid.split(":")
            org=nid_sp[3]
            suborg=nid_sp[4]
            stationid=nid_sp[5]
            status=nid_sp[6]
            try:
                parameter=nid_sp[7]
            except IndexError:
                parameter=""
            fo.write("{},{},{},{},{},{},{},{},{},{}\n".format(
                nid,org,suborg,stationid,status,parameter,phenombegin,phenomend,resbegin,resend))

            #compile unique offers. Date defaults to 1950-01-01
            try:
                unique_offers.append((stationid,parameter,dateutil.parser.parse(phenomend,default=datetime(1950,1,1))))
            except ValueError:
                unique_offers.append((stationid,parameter,datetime(1950,1,1)))
    return unique_offers
        
def read_station_meta(station_meta_path,metadata_headers):
    """
    Function to read station metadata File 
    """
    log_entry("-","Read metadata from {}".format(station_meta_path))
    stationdata_l = []
    with open(station_meta_path) as fr:
        stat_reader = csv.reader(fr)
        for i,row in enumerate(stat_reader):
            if i == 0:
                continue #skip header row
            else: 
                try:
                    dict_l = dict(zip(metadata_headers,escape(row))) #escape the string for xml
                except :
                    print("metadata - header mismatch") #if zip fails (likely because number of columns don't match)
                stationdata_l.append(dict_l)
    return stationdata_l

def read_parameter_meta(parameter_meta_path,parameter_headers):
    """
    Function to read parameter metadata File 
    """
    log_entry("-","read metadata from {}".format(parameter_meta_path))
    parameterdata_l = []
    with open(parameter_meta_path) as fr:
        par_reader = csv.reader(fr)
        for i,row in enumerate(par_reader):
            if i == 0:
                continue #skip header row
            else:
                #throw an error if number of columns don't match expected
                assert (len(parameter_headers) == len(row)),"Parameter Metadata Header and Row mismatch."
                dict_l = dict(zip(parameter_headers,row))
                parameterdata_l.append(dict_l)
    return parameterdata_l

def create_station_request(template, stationmeta, parammeta):
    """
    Purpose: Create the text of the xml for a station/sensor 
    Input: station meta data (as list of dictionaries)
    Output: text file used for station push
    """
    lookup={
            #'Sequence':stationmeta['Sequence'].lower(),
            'suborg':stationmeta['suborg'].lower(),
            'stationid':stationmeta['stationid'].lower(),
            'shortName':stationmeta['shortName'].lower(),
            'longName':stationmeta['longName'],
            'easting':stationmeta['easting'].lower(),
            'northing':stationmeta['northing'].lower(),
            'altitude':stationmeta['altitude'].lower(),
            'parameter':parammeta['parameter'].lower(),
            'parameterName':parammeta['parameterName'],
            'parameterUnit':parammeta['parameterUnit'],
            'fieldName':parammeta['fieldName'],
            'organizationName':stationmeta['organizationName'],
            'organizationURL':stationmeta['organizationURL'],
            'contact':stationmeta['contact'],
            'waterbodyType':stationmeta['waterbodyType'],
            'publisher':stationmeta['urn-org'].lower(),
            'status':parammeta['status'].lower(),
            'urn-org':stationmeta['urn-org'].lower()}
    #log_entry("-","Create template from {}".format(template))
    #open the station template file and read the template as a string
    with open(template,'r') as fi:
        station_meta_str = fi.read()
    
    #the template is coded with lookups for the KEYS from each list element in stationmeta.
    #Create the lookup dictionary for the template LookupError
    #lookup = {k:stationmeta[k].lower() for k in metadata_headers}
    
    #replace the placeholders with corresponding information from the metadata
    new_station_meta_str = station_meta_str.format(**lookup)
    # Hand fix for bug/? with .format call ...
    if new_station_meta_str.find('parameterUnit') != -1:
       new_station_meta_str = new_station_meta_str.replace('parameterUnit',parammeta['parameterUnit'])

    return new_station_meta_str


def push_template(station_str, url):
    """
    Purpose: Push station or sensor to server
    Input: Station xml, url
    Output: None
    """
    station_bytes = station_str.encode('utf-8')
    req = ur.Request(url)
    req.add_header('Content-Type','application/xml')
    req.add_header('charset','UTF-8')
    r = ur.urlopen(req,data=station_bytes)
    
    return r

def check_data(data_file,unique_offers):
    """
    Creates a list that has the same length as data points in data file 
    Each list entry is a tuple with the following:
    "ok" - station and sensor exist on server, just push result 
    "sensor" - station exists on server, push sensor and result 
    "station" - neither station or sensor exist on server. push station, sensor, and result
    """
    #log_entry("-", "Check offerings for missing stations and sensors")
    last_record = []
    #create list of stations already on server
    station_list = [s[0] for s in unique_offers]
    station_match_flag = False
    station_sensor_flag = False

    with open(data_file,'r') as fr:
        #open the data file 
        r = csv.reader(fr)
        for i, row in enumerate(r):
            if i == 0:
                continue #skip header row
            else:
                station = row[0]
                parameter = row[3]
                if VERBOSE:
                    log_entry(".","{} - Station: {} Parameter {}".format(i,station,parameter))
                #check if station is in offering
                #stops when it finds a match, because offering should be unique
                if station in station_list: #if the station is already on the server
                    append_value = ("sensor",(station,parameter))
                    for st, par, date in unique_offers: #check if the data result is already on the server
                        
                        if station == st:
                            if parameter == par:
                                
                                append_value = ("ok",date)
                                break
                else:
                    append_value = ("station_sensor",(station,parameter))
                last_record.append(append_value)
    return last_record

def get_unique_station_sensor(data_file,date_filter):
    """
    Input: data_file:csv file
    Output: a list of the unique station/sensor combinations
    """
    with open(data_file,'r') as fi:
        r = csv.reader(fi)
        # new_csv_list=[]
        # new_csv_list.append(["StationID", "Parameter","NumberOfObs","TimeObsString"])
        station_sensor_list = []
        for i, row in enumerate(r):
            #Do we want to process it?
            if i == 0:
                continue
            else:
                j = i-1
                stationid = row[0]
                parameter = row[3]
                if not date_filter[j]:
                    continue #or do something with it
                else:
                    station_sensor_list.append((stationid,parameter))
        unique_station_sensor = set(station_sensor_list)
    return unique_station_sensor

def check_dates(data_file,last_record):
    """
    Return a list of booleans that show True if the date of the entry is after the date already on server
    """
    date_filter=[]
    with open(data_file,'r') as fi:
        r = csv.reader(fi)
        for i, row in enumerate(r):
            #index offset 1 for last_record list
            j = i-1
            if i == 0:
                continue
            elif last_record[j][0] != 'ok':
                date_filter.append(True)
            else:
                date = row[1]
                time = row[2]
                date_time = dateutil.parser.parse("{} {}".format(date,time))
                #process the date filter
                if date_time > last_record[j][1].replace(tzinfo=None):
                    date_filter.append(True)
                else:
                    date_filter.append(False)
                 
    return date_filter


def accumulate_data(unique_station_sensor,data,date_filter):
    """
    Prep the data in a format easy to push to the server
    """
    rolled_up_data = {}
    with open(data,'r') as fi:
        r = csv.reader(fi)
        for i, row in enumerate(r):
            j = i-1
            if i == 0:
                continue
            else:
                if date_filter[j] and row[4] != "": #only process row if date_filter is true and value is not missing
                    data_id = (row[0],row[3])  #(stationid,parameter)
                    date_time = dateutil.parser.parse("{} {}".format(row[1],row[2]))
                    data_value = (datetime.isoformat(date_time),row[4])
                    #print(data_value)
          
                    #rolled_up_data.setdefault(data_id, []).append(data_value)
                    rolled_up_data.setdefault(data_id, {}).setdefault('values',[]).append(data_value)
    for k,v in rolled_up_data.items():
        count = len(v['values'])
        rolled_up_data[k]['count']=count
    return rolled_up_data

def create_data_request(data_template, k,v,status,urnorg,suborg):
    
    stationid = k[0]
    parameter = k[1]
    # status = "final"
    # suborg = "python"
    count = str(v['count'])
    values_str = ""
    for item in v['values']:
        values_str += item[0]
        values_str +=","
        values_str += item[1]
        values_str += ";"
    data = count + ";" + values_str


    with open(data_template,'r') as fi:
        data_meta_str = fi.read()
        #replace the placeholders with corresponding information from the metadata
        new_data_meta_str = data_meta_str.format(
                                            stationid=stationid,
                                            parameter=parameter,
                                            status=status,
                                            suborg=suborg,
                                            urnorg=urnorg,
                                            data=data)
    return new_data_meta_str

def push_new_templates(data,last_record,date_filter,stationmeta,parameta):
    alreadyprocessed = []
    with open(data,'r') as fi:
        for i,row in enumerate(fi):
            j = i-1
            if i == 0:
                continue
            elif not date_filter[j]:
                continue  #skip data that is already in the database (OR DO SOMETHING WITH IT?)
            elif last_record[j] in alreadyprocessed:
                continue
            else:
                #check templates that need to be uploaded
                status = last_record[j][0]
                if status == 'station_sensor':
                    # Get pertinent info
                    station = last_record[j][1][0]
                    parameter = last_record[j][1][1]
                    #
                    #PROCESS STATION TEMPLATE
                    #
                    station_record = None
                    for record in stationmeta:
                        
                        if record['stationid'].lower() == station.lower():
                            station_record = record
                            break
                    if station_record == None:
                        print("station metadata not found for {}".format(station))
                        continue #OR DO SOMETHING else
                    #Create STATION template
                    station_str = create_station_request(station_meta_template,station_record,parammeta[0])
                    response = push_template(station_str, url)
                    log_entry("+","Station {} template pushed with response {}".format(station,response.readlines()))
                    #
                    # PROCESS SENSOR TEMPLATE
                    #
                    sensor_record = None
                    sensor_param_record=None
                    for record in stationmeta:
                        if record['stationid'].lower() == station.lower():
                                sensor_record = record
                                break
                    for record in parammeta:
                        if record['parameter'] == parameter:
                                sensor_param_record = record
                                break
                    if sensor_record == None:
                        print("sensor metadata not found for {}".format(station))
                        continue #OR DO SOMETHING else
                    if sensor_param_record == None:
                        print("parameter metadata not found - {}".format(parameter))
                        continue
                    #Create sensor str
                    sensor_str = create_station_request(sensor_meta_template,sensor_record,sensor_param_record)
                    response = push_template(sensor_str, url)
                    log_entry("+","Sensor {} template pushed with response {}".format(parameter, response.readlines()))
                    #
                    # PROCESS RECORD TEMPLATE
                    # 
                    result_record = None
                    result_param_record=None
                    for record in stationmeta:
                        if record['stationid'].lower() == station.lower():
                                result_record = record
                                break
                    for record in parammeta:
                        if record['parameter'] == parameter:
                                result_param_record = record
                                break
                    if result_record == None:
                        print("sensor metadata not found for {}".format(station))
                        continue #OR DO SOMETHING else
                    if result_param_record == None:
                        print("parameter metadata not found -{}".format(parameter))
                        continue
                    #Create sensor str
                    result_str = create_station_request(result_meta_template,result_record,result_param_record)
                    response = push_template(result_str, url)
                    log_entry("+","Result {} {} template pushed with response {}".format(station, parameter,response.readlines()))
                    alreadyprocessed.append(last_record[j])

                elif status == 'sensor':
                    station = last_record[j][1][0]
                    parameter = last_record[j][1][1]
                    #lookup station parameter in metadata_headers
                    #
                    # PROCESS SENSOR TEMPLATE
                    #
                    sensor_record = None
                    sensor_param_record=None
                    for record in stationmeta:
                        if record['stationid'].lower() == station.lower():
                                sensor_record = record
                                break
                    for record in parammeta:
                        if record['parameter'] == parameter:
                                sensor_param_record = record
                                break
                    if sensor_record == None:
                        print("sensor metadata not found for {}".format(station))
                        continue #OR DO SOMETHING else
                    if sensor_param_record == None:
                        print("parameter metadata not found - {} ".format(parameter))
                        continue
                    #Create sensor str
                    sensor_str = create_station_request(sensor_meta_template,sensor_record,sensor_param_record)
                    response = push_template(sensor_str, url)
                    log_entry("+","Sensor {} template pushed with response {}".format(parameter, response.readlines()))
                    #
                    # PROCESS RECORD TEMPLATE
                    # 
                    result_record = None
                    result_param_record=None
                    for record in stationmeta:
                        if record['stationid'].lower() == station.lower():
                                result_record = record
                                break
                    for record in parammeta:
                        if record['parameter'] == parameter:
                                result_param_record = record
                                break
                    if result_record == None:
                        print("sensor metadata not found for {}".format(station))
                        continue #OR DO SOMETHING else
                    if result_param_record == None:
                        print("parameter metadata not found - {}".format(parameter))
                        continue
                    #Create sensor str
                    result_str = create_station_request(result_meta_template,result_record,result_param_record)
                    response = push_template(result_str, url)
                    log_entry("+","Result {} {} template pushed with response {}".format(station, parameter, response.readlines()))
                    alreadyprocessed.append(last_record[j])


if __name__ == "__main__":
    #Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("d", help="data file relative location")
    parser.add_argument("c", help="config file relative location")
    args = parser.parse_args()


    log_entry("*","*************")
    log_entry("*","Start Program")
    log_entry("*","*************")
    filelist = glob("temp/PART_*.csv")
    for nfile in filelist:
        os.remove(nfile)
    #dataconvert.parse("input/edmr_outfall.json","short.csv")
    dataconvert.parse(args.c,args.d)
    filelist = glob("temp/PART_*.csv")
    total_files = len(filelist)
    #loop over chopped-up data files in temp folder
    for i,nfile in enumerate(filelist):
        print("Processing file {} of {}".format(i+1,total_files))
        log_entry("-","Processing file {} of {}".format(i+1,total_files))

        #------------------------
        #Read metadata 
        #------------------------
        #Read station metadata csv file
        stationmeta = read_station_meta(station_meta_path, metadata_headers)
        #Read parameter metadata csv file
        parammeta = read_parameter_meta(parameter_meta_path,parameter_headers)
        
        #------------------------
        # Get existing information from server
        #------------------------
        #Request offering from server. Parse into a list of offerings.
        offer_dict = parse_capabilities(url)
        
        #Create a list of unique offerings (station, parameter, last measurement date/time)
        unique_offers = pull_capability_data(offer_dict)
        
        #------------------------
        # Check data for missing templates
        #------------------------
         
        
        #loop through the data file and determine what has to be done for each data point
        last_record = check_data(nfile,unique_offers)
        #create a list of which dates are before the offering date
        date_filter = check_dates(nfile, last_record) # could clean this up a bit
        #push any new templates that are needed
        push_new_templates(nfile,last_record,date_filter,stationmeta,parammeta)
        #------------------------
        #process data, format data and push
        #------------------------
        unique_station_sensor = get_unique_station_sensor(nfile,date_filter)
        rolled_up_data = accumulate_data(unique_station_sensor,nfile,date_filter)
        for k,v in rolled_up_data.items():
            for i in parammeta:
                if k[1] == i['parameter']:
                    status = i['status'].lower()
                    break
            for i in stationmeta:
                if k[0].lower() == i['stationid'].lower():
                    urnorg = i['urn-org'].lower()
                    suborg = i['suborg'].lower()
                    break
            #print(urnorg)
            #print(stationmeta)
            data_meta_str = create_data_request(data_template, k,v,status,urnorg,suborg)
            response = push_template(data_meta_str,url)
            log_entry("-","Results for {} pushed with response {}".format(k,response.readlines()))
    log_entry("*","*************")
    log_entry("*","End Program")
    log_entry("*","*************")    
    
