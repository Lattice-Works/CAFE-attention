from datetime import timedelta
import datetime as dt
import pandas as pd
import numpy as np
import os

def logger(text,level=1):
    time = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    prefix = "༼ つ ◕_◕ ༽つ" if level==0 else "-- "
    print("%s %s: %s"%(prefix,time,text))

def extract_measurements(infile, metric, subdir):
    # get file
    if not os.path.exists(infile):
        logger("WARNING: No %s-measures found in %s"%(metric,subdir))
        return 9
    data = pd.read_csv(infile,header=None)

    if metric == "IBI":
        data.columns = ['offset_seconds','IBI']
        # read in BVP to get onset timestamp
        hlp = infile.split("/")[-1].split(".")[0].split("_")
        suffix = "" if len(hlp)==1 else "_%s"%hlp[1]
        BVPdata = pd.read_csv(os.path.join(subdir,"BVP%s.csv"%suffix),header=None)
        BVPonset = dt.datetime.fromtimestamp(BVPdata.iloc[0,0])
        # extract timestamps:
        # attention: onset = BVPonset + IBIonset (1st column in IBI)
        data['timestamp'] = [BVPonset + timedelta(seconds = x) for x in
                         data['offset_seconds']]
        data['timestamp'] = data['timestamp'] + timedelta(hours=2)
        data = data.drop(['offset_seconds'],axis=1).iloc[1:]
        return data.reset_index(drop=True)

    data.columns = ["%s_%i"%(metric,ind) for ind in range(len(data.columns))]
    # converting the first timestamp to a datetime field
    timestamp = list(data.iloc[0])[0]
    timestamp = dt.datetime.fromtimestamp(timestamp)
    # converting the samplerate from Hz to seconds
    samplerate = list(data.iloc[1])[0]
    samplerate_seconds = 1/samplerate
    # extract the measurements (starting from row 2 - the third row)
    measurements = pd.DataFrame(data.iloc[2:]).reset_index(drop=True)
    # add timestamps for each measurement
    offsets = measurements.index * timedelta(seconds=samplerate_seconds)
    # add offsets to starttime and add timedelta (for inconsistency between our timestamps and Seungs')
    measurements['timestamp'] = timestamp + offsets + timedelta(hours=2)


    if metric == "ACC":
        measurements['SVM'] = np.sqrt(measurements['ACC_0']**2 + \
            measurements['ACC_1']**2 + \
            measurements['ACC_2']**2)

    return measurements.reset_index(drop=True)
