from datetime import datetime, timedelta
from collections import Counter
from utils import utils
import pandas as pd
import numpy as np
import os

# set variables for input and output
basedir = "/Users/jokedurnez/Desktop/Heather Info for CAFE Physio Pilot/Preliminary Physio Wristband Data for Mollie/"
datadir = os.path.join(basedir, "Physio CSV data")
outdir = os.path.join(basedir, "Physio CSV data_preprocessed")
outfile = os.path.join(basedir, "Summary.csv" )
infosheetfile = os.path.join(basedir, "Physio AMP_Subject_Info_Sheet (1).xlsx")

if not os.path.exists(outdir):
    os.mkdir(outdir)

# get a list of all subjects in physio folder
folders = [x for x in os.listdir(datadir) if not x.startswith(".DS")]
subjects = dict(Counter([x[:10] for x in folders]))

##############
# PREPROCESS #
##############

utils.logger("\nAligning timestamps for all subjects...\n", level=0)

# loop over all physio folders and preprocess timestamps to datetimes for all measurements
for folder in folders:

    # reconstruct subject folder and make preprocessing folder (if not existing)
    subdir = os.path.join(datadir,folder)
    suboutdir = os.path.join(outdir,folder)
    if not os.path.exists(suboutdir):
        os.mkdir(suboutdir)

    # loop over ACC, EDA, BVP, TEMP, HR that have the same processing stream
    for metric in ['ACC', 'EDA', 'BVP', 'TEMP', 'HR', 'IBI']:
        measurements = utils.extract_measurements(metric,subdir)
        if isinstance(measurements,int):
            continue
        measurements.to_csv(os.path.join(
            suboutdir,"PHYSIO_%s.csv"%(metric)),index=False)
        # logging
        start = measurements['timestamp'][0].strftime("%H:%M:%S")
        end = measurements['timestamp'].iloc[len(measurements)-1].strftime("%H:%M:%S")
        utils.logger("LOG: subject %s: %s was measured from %s to %s"%(folder, metric, start, end))

##############
# SUMMARISE #
##############

utils.logger("\nCombining sessions and summarising by condition for all subjects... \n", level=0)

# variables from preprocessed files that we want to summarise
variable_of_interest = {
    "ACC": "SVM",
    "EDA": "EDA_0",
    "BVP": "BVP_0",
    "TEMP": "TEMP_0",
    "HR": "HR_0",
    "IBI": "IBI"
}

infosheet = pd.read_excel(infosheetfile,index_col='Subject ID')

summary = pd.DataFrame({})

# loop over subjects
for subject,nums in subjects.items():
    if subject == "WI_AMP_001":
        utils.logger("Not doing subject %s: not sure if this subject is to be used."%subject)
        continue

    # get subject folders and assert there's as many sessions per subject as folders
    subfolders = [x for x in folders if x.startswith(subject)]
    subcodes = utils.get_subcodes(subfolders)
    assert(len(subfolders)==nums)

    utils.logger("PREPROCESSING subject %s"%subject)

    # loop over all metrics
    for metric in ['ACC', 'EDA', 'TEMP', 'HR', 'IBI', 'BVP']:

        measures = pd.DataFrame({}) #dataset per subject per metric --> to combine sessions

        # loop over sessions (must be within metrics)
        for folder,subcode in zip(subfolders,subcodes):

            # get subject info and timing of conditions
            subjectinfo = infosheet.loc[subcode]
            times = utils.extract_times(subjectinfo, 1 if metric=="ACC" else 0)

            # read in preprocessed file
            preprocessed = pd.read_csv(os.path.join(
                outdir,folder,"PHYSIO_%s.csv"%(metric)),
                parse_dates = ['timestamp'])

            # add condition to preprocessed data
            preprocessed['condition'] = None
            for condition,values in times.items():
                conditiontimes = (preprocessed.timestamp < values['end']) & \
                    (preprocessed.timestamp >= values['start'])
                preprocessed.loc[conditiontimes,'condition']= condition

            # append session to subject dataset
            measures = pd.concat([measures,preprocessed])

        # group by condition and summarise --> note grouper depends on variable_of_interest object
        grouper = variable_of_interest[metric]
        grouped = measures[[grouper,'condition']] \
            .groupby('condition') \
            .aggregate(['mean','count','median','std'])
        grouped.columns = ['mean','count','median','std']

        # add metric, ID and condition to index
        grouped['metric'] = metric
        grouped['ID'] = subject
        grouped.reset_index().set_index(['ID','condition'])

        # add to summary dataset
        summary = pd.concat([summary,grouped])

# output full dataset
summary.to_csv(outfile)
