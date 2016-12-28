import csv
import json

CSVCHUNK = 1000

def parse(conf_file,data_file):
    """
    Read the type field and chose the associated parser
    """
    with open(conf_file,'r') as fi:
        conf_str = fi.read()
    config_f = json.loads(conf_str)
    if config_f['type']==1:
        parse1(conf_file,data_file,config_f)
    elif config_f['type']==2:
        parse2(conf_file,data_file,config_f)
    elif config_f['type']==3:
        #station varies, datetime together
        parse3(conf_file,data_file,config_f)

def parse1(conf_file,data_file,config):
    with open(data_file,'r') as fi:
        with open('temp/all_data.csv','w', newline="") as fo:
            # convert the input file to a consistent file with all data
            r = csv.reader(fi)
            w = csv.writer(fo)
            w.writerow(["station","date","time","parameter","value"])
            columns = config['columns']
            station = config['station']
            datecol = columns.index("date")
            timecol = columns.index("time")
            header = config['header']

            for i,ncol in enumerate(columns):
                
                if ncol == "date":
                    continue
                elif ncol == "time":
                    continue
                elif ncol == "":
                    continue
                else:
                    for j,nrow in enumerate(r):
                        if j < header:
                            continue
                        else:
                            newrow = []
                            newrow.append(station) #station
                            newrow.append(nrow[datecol]) #date
                            newrow.append(nrow[timecol]) #time
                            newrow.append(ncol) #parameter
                            newrow.append(nrow[i]) # value
                            w.writerow(newrow)
                    fi.seek(0)
    with open('temp/all_data.csv','r') as fi:
        r = csv.reader(fi)
        filecount = 1
        counter = 1
        eof = False
        next(fi)
        while eof == False:
            eof = True    
            filename = "temp/PART_{}.csv".format(filecount)
            filecount += 1
            with open(filename,'w',newline="") as fo:
                w = csv.writer(fo)
                w.writerow(["station","date","time","parameter","value"])
                
                for i,line in enumerate(r):
                    # if i < counter:
                    #     continue
                    # else:
                        #print(counter)
                    counter += 1
                    w.writerow(line)
                    eof = False
                    if counter % CSVCHUNK == 0:
                        early_break = True
                        break

def recorddate(date_time):
    cleandata = date_time.strip()
    rdate = cleandata.split(" ")[0]
    rtlist = cleandata.split(" ")[1:]
    rtime = " ".join(rtlist)
    return rdate, rtime


def parse2(conf_file,data_file,config):
    with open(data_file,'r') as fi:
        with open('temp/all_data.csv','w', newline="") as fo:
            # convert the input file to a consistent file with all data
            r = csv.reader(fi)
            w = csv.writer(fo)
            w.writerow(["station","date","time","parameter","value"])
            columns = config['columns']
            station = config['station']
            datetimecol = columns.index("datetime")

            header = config['header']

            for i,ncol in enumerate(columns):
                
                if ncol == "datetime":
                    continue
                elif ncol == "":
                    continue
                else:
                    for j,nrow in enumerate(r):
                        if j < header:
                            continue
                        else:
                            rdate,rtime = recorddate(nrow[datetimecol])
                            newrow = []
                            newrow.append(station) #station
                            newrow.append(rdate) #date
                            newrow.append(rtime) #time
                            newrow.append(ncol) #parameter
                            newrow.append(nrow[i]) # value
                            w.writerow(newrow)
                    fi.seek(0)
    with open('temp/all_data.csv','r') as fi:
        r = csv.reader(fi)
        filecount = 1
        counter = 1
        eof = False
        next(fi)
        while eof == False:
            eof = True    
            filename = "temp/PART_{}.csv".format(filecount)
            filecount += 1
            with open(filename,'w',newline="") as fo:
                w = csv.writer(fo)
                w.writerow(["station","date","time","parameter","value"])
                
                for i,line in enumerate(r):
                    # if i < counter:
                    #     continue
                    # else:
                        #print(counter)
                    counter += 1
                    w.writerow(line)
                    eof = False
                    if counter % CSVCHUNK == 0:
                        early_break = True
                        break
                
def parse3(conf_file,data_file,config):
    with open(data_file,'r') as fi:
        with open('temp/all_data.csv','w', newline="") as fo:
            # convert the input file to a consistent file with all data
            r = csv.reader(fi)
            w = csv.writer(fo)
            w.writerow(["station","date","time","parameter","value"])
            columns = config['columns']
            stationcol = columns.index("station")
            datetimecol = columns.index("datetime")

            header = config['header']

            for i,ncol in enumerate(columns):
                
                if ncol == "datetime":
                    continue
                elif ncol == "":
                    continue
                elif ncol == "station":
                    continue
                else:
                    for j,nrow in enumerate(r):
                        if j < header:
                            continue
                        else:
                            rdate,rtime = recorddate(nrow[datetimecol])
                            station = nrow[stationcol]
                            newrow = []
                            newrow.append(station) #station
                            newrow.append(rdate) #date
                            newrow.append(rtime) #time
                            newrow.append(ncol) #parameter
                            newrow.append(nrow[i]) # value
                            w.writerow(newrow)
                    fi.seek(0)
    with open('temp/all_data.csv','r') as fi:
        r = csv.reader(fi)
        filecount = 1
        counter = 1
        eof = False
        next(fi)
        while eof == False:
            eof = True    
            filename = "temp/PART_{}.csv".format(filecount)
            filecount += 1
            with open(filename,'w',newline="") as fo:
                w = csv.writer(fo)
                w.writerow(["station","date","time","parameter","value"])
                
                for i,line in enumerate(r):
                    # if i < counter:
                    #     continue
                    # else:
                        #print(counter)
                    counter += 1
                    w.writerow(line)
                    eof = False
                    if counter % CSVCHUNK == 0:
                        early_break = True
                        break
                

if __name__ == "__main__":
    #used only for testing
    parse("njdep.json","c:/data/njdep.csv")           

