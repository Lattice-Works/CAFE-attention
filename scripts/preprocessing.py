from datetime import datetime, timedelta
from collections import Counter
from utils import utils_preprocessing as utils
import pandas as pd
import numpy as np
import os

# set variables for input and output
basedir = "/Users/jokedurnez/Desktop/Heather Info for CAFE Physio Pilot/Preliminary Physio Wristband Data for Mollie/data/"
outdir = os.path.join(basedir,'derivatives/preprocessed/')

if not os.path.exists(outdir):
    os.mkdir(outdir)

group_info_file = os.path.join(basedir,"group_information.csv")
group_info = pd.read_csv(group_info_file,index_col="Subject ID")

#####################
# PREPROCESS PHYSIO #
#####################

utils.logger("\nAdding timestamps for physio all subjects...\n", level=0)

# loop over all physio folders and preprocess timestamps to datetimes for all measurements
for subject, row in group_info.iterrows():

    # reconstruct subject folder and make preprocessing folder (if not existing)
    if not os.path.exists(os.path.join(outdir,subject)):
        os.mkdir(os.path.join(outdir,subject))

    # physio suboutdir
    suboutdir = os.path.join(outdir,subject,'physio')
    if not os.path.exists(suboutdir):
        os.mkdir(suboutdir)

    # loop over ACC, EDA, BVP, TEMP, HR that have the same processing stream
    for metric in ['ACC', 'EDA', 'BVP', 'TEMP', 'HR', 'IBI']:
        infiles = [x for x in os.listdir(os.path.join(basedir,subject,'physio')) if x.startswith(metric)]
        measurements = pd.DataFrame({})

        # loop over potentially multiple files for the same metric
        for infile in infiles:
            subset_measurements = utils.extract_measurements(os.path.join(basedir,subject,'physio',infile), metric,os.path.join(basedir,subject,'physio'))
            if isinstance(subset_measurements,int):
                continue
            else:
                measurements = pd.concat([measurements,subset_measurements], ignore_index=True).sort_values(by='timestamp').reset_index(drop=True)

        if len(measurements)==0:
            utils.logger("WARNING: No measures found for %s"%(subject))
        else:
            measurements.to_csv(os.path.join(suboutdir,"%s.csv"%(metric)),index=False)

            # logging
            start = measurements['timestamp'][0].strftime("%H:%M:%S")
            end = measurements['timestamp'].iloc[len(measurements)-1].strftime("%H:%M:%S")
            utils.logger("LOG: subject %s: %s was measured from %s to %s"%(subject, metric, start, end))

#########################
# PREPROCESS CONDITIONS #
#########################

utils.logger("\n\nAdding timestamps for conditions all subjects...\n", level=0)

for subject, row in group_info.iterrows():

    # get subject info
    subject_info_file = os.path.join(basedir,subject,"subject_information.csv")
    subject_info = pd.read_csv(subject_info_file,index_col='file', parse_dates=['origin'])

    # reconstruct subject folder and make preprocessing folder (if not existing)
    if not os.path.exists(os.path.join(outdir,subject)):
        os.mkdir(os.path.join(outdir,subject))

    # conditions suboutdir
    suboutdir = os.path.join(outdir,subject,'conditions')
    if not os.path.exists(suboutdir):
        os.mkdir(suboutdir)

    # loop over attention and TV
    for condition in ['attention','TV']:
        confiles = [x for x in os.listdir(os.path.join(basedir,subject,'conditions')) if x.startswith(condition)]
        combined = pd.DataFrame({})

        for confile in confiles:
            metric = confile.split(".")[0]

            # reading in data
            data = pd.read_csv(os.path.join(basedir,subject,'conditions',confile))

            # grabbing starttime
            starttime = subject_info.loc[os.path.join('conditions',confile),'origin']

            # set timestamps
            data['timestamp_onset'] = [starttime + timedelta(milliseconds=x) for x in data['onset']]
            data['timestamp_offset'] = [starttime + timedelta(milliseconds=x) for x in data['offset']]

            # get rid of onsets/offsets (otherwise start is not clear)
            data = data.drop(['onset','offset'],axis=1)

            # check offset is not before onset
            inds = np.where(data['timestamp_offset'] < data['timestamp_onset'])[0]
            if len(inds)>0:
                utils.logger("WARNING: subject {subject}: in line(s) {lineno} of {confile}: the offset is before the onset.  Line ignored.".format(
                    subject = subject, lineno = ",".join([str(x) for x in inds]), confile = confile
                ))
                data = data.drop(index=inds,axis=0)

            combined = pd.concat([combined, data], ignore_index=True)

        # recode attention more generally (O-Shoes = O, etc)
        if condition == 'attention':
            combined['codegeneralised'] = combined['code'].apply(lambda y: str(y).split("-")[0].split("_")[0].upper())

        combined.to_csv(os.path.join(suboutdir,"%s.csv"%condition),index=False)

        # logging
        start = data['timestamp_onset'][0].strftime("%H:%M:%S")
        end = data['timestamp_offset'].iloc[len(data)-1].strftime("%H:%M:%S")
        utils.logger("LOG: subject %s: %s was measured from %s to %s"%(subject, metric, start, end))

#########################
# PREPROCESS VIDEO CUTS #
#########################

utils.logger("\n\nAdding timestamps for video cuts all subjects...\n", level=0)

for subject, row in group_info.iterrows():

    # get subject info
    subject_info_file = os.path.join(basedir,subject,"subject_information.csv")
    subject_info = pd.read_csv(subject_info_file,index_col='file', parse_dates=['origin'])

    # reconstruct subject folder and make preprocessing folder (if not existing)
    if not os.path.exists(os.path.join(outdir,subject)):
        os.mkdir(os.path.join(outdir,subject))

    # conditions suboutdir
    suboutdir = os.path.join(outdir,subject,'conditions')
    if not os.path.exists(suboutdir):
        os.mkdir(suboutdir)

    # adding datacuts: reading in TV file
    tvdata = pd.read_csv(os.path.join(suboutdir,"TV.csv"),index_col='code', parse_dates=['timestamp_onset', 'timestamp_offset'] )

    # datacuts files
    datacuts = {
        "TL": os.path.join(basedir,'extra/video_cuts','TumbleLeaf_First7.5_Cuts.csv'),
        "Psych": os.path.join(basedir,'extra/video_cuts','Psych_First7.5_Cuts.csv'),
        "WoF": None
    }

    for code in ['Adult', "Child"]:

        # select rows for this condition
        rows = tvdata[tvdata.index.str.startswith(code)]

        # select videos for condition and make sure there's only 1
        videos = list(dict(Counter(rows['video'])).keys())
        assert len(videos) < 2
        video = videos[0]

        # get correct cuts
        infile = datacuts[video]
        if not isinstance(infile,str):
            utils.logger("WARNING: video %s hasn't been decoded.  Cannot decode subject %s"%(video,subject))
            continue

        if video == "TL":
            infiles = [infile]+[os.path.join(basedir,'extra/video_cuts/',x) for x in ['TumbleLeaf_RF.csv','TumbleLeaf_RM.csv']]
            prefixes = ['','RF_','RM_']
        else:
            infiles = [infile]
            prefixes = ['']
        # read in cutdata and rename columns like other categorical files

        for infile, prefix in zip(infiles,prefixes):
            if infile not in [os.path.join(basedir,'extra/video_cuts/TumbleLeaf_RF.csv'),os.path.join(basedir,'extra/video_cuts/TumbleLeaf_RM.csv')]:
                cutdata = pd.read_csv(infile).drop("Unnamed: 4",axis=1).rename(columns = {"code01": "cut"})

                # split scene from cut
                cutdata['scene'] = cutdata['cut'].apply(lambda x: (x.split(" ")[1]))
                cutdata['scene_cut'] = cutdata['cut'].apply(lambda x: (x.split(" ")[3]))
            else:
                cutdata = pd.read_csv(infile,index_col=0).rename(columns = {"code": "cut"})

            cutdata['cut'] = cutdata['cut'].apply(lambda y: ("%s%s"%(prefix,y)))
            # deal with restroom breaks (recognised by a TV condition appearing twice)
            if len(rows) == 2:
                rows = rows.sort_values(by="timestamp_onset").reset_index()
                # get start of break and duration in ms from origin of cutfile
                breakduration = rows.iloc[1]['timestamp_onset'] - rows.iloc[0]['timestamp_offset']
                breakstart = rows.iloc[0]['timestamp_offset'] - rows.iloc[0]['timestamp_onset']
                breakdurationms = breakduration.seconds*1000. + breakduration.microseconds/1000.
                breakstartms = breakstart.seconds*1000. + breakstart.microseconds/1000.
                # find where in cutfile happened the break
                cutlocation = np.min(np.where(breakstartms<cutdata['offset'])[0])
                # keep a copy of the row that has to be split
                cutrow = cutdata.iloc[cutlocation]
                # change the endtime of the row to when the break happened
                cutdata.at[cutlocation,'offset'] = breakstartms
                # change the starttime and index of the row kept and append to dataset (and resort by onset)
                cutrow.at['onset'] = breakstartms
                cutrow.at['ordinal'] = "%sB"%cutrow['ordinal']
                cutdata = cutdata.append(cutrow,ignore_index=True).sort_values(by='onset').reset_index(drop=True)
                # all timepoints after the break: add breakduration
                cutdata.at[(cutlocation+1):, 'onset'] = cutdata['onset']+breakdurationms
                cutdata.at[(cutlocation+1):, 'offset'] = cutdata['offset']+breakdurationms

            # get starttime
            starttime = rows.iloc[0]['timestamp_onset']
            endtime = rows.iloc[len(rows)-1]['timestamp_offset']

            # add onset/offset to starttime to get timestamp
            cutdata['timestamp_onset'] = np.array([starttime + timedelta(milliseconds=x) for x in cutdata['onset']])
            cutdata['timestamp_offset'] = np.array([starttime + timedelta(milliseconds=x) for x in cutdata['offset']])
            cutdata = cutdata.drop(['onset','offset'], axis=1)

            # change last time to endtime
            idx = np.where(cutdata['timestamp_offset']>endtime)[0]
            if len(idx)>0:
                cutdata.at[idx,'timestamp_offset'] = endtime

            # remove rows after endtime
            idx = np.where(cutdata['timestamp_offset'] < cutdata['timestamp_onset'])[0]
            if len(idx)>0:
                cutdata = cutdata.drop(idx)

            cutdata.at[cutdata.timestamp_offset<=endtime,'code'] = code
            # cuts = pd.concat([cuts,cutdata],ignore_index=True)
            suffix = infile.split(".")[-2].split("_")[-1]
            cutdata.to_csv(os.path.join(suboutdir,'{code}_{suffix}.csv'.format(code=code,suffix=suffix)))
