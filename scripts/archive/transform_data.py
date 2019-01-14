import os
import pandas as pd
from collections import Counter
import datetime as dt
from utils import datatransfer
from shutil import copyfile

basedir = "/Users/jokedurnez/Desktop/Heather Info for CAFE Physio Pilot/Preliminary Physio Wristband Data for Mollie/"

# create datafolder
datadir = os.path.join(basedir,"data")
if not os.path.exists(datadir):
        os.mkdir(datadir)

# get attention files
attentionfolder = os.path.join(basedir,"Datavyu_Attention_csv/Attention CSV files")
attentionfiles = [os.path.join(x) for x in os.listdir(attentionfolder)]

# get infosheet and list of subjects
infosheetfile = os.path.join(basedir,
                         "Physio AMP_Subject_Info_Sheet (1).xlsx")
infosheet = pd.read_excel(infosheetfile,index_col='Subject ID')
subjects = ["_".join(x.split("_")[:3]) for x in infosheet.index]
subjectscounter = dict(Counter(subjects))

group_information = pd.DataFrame({})
for subject in subjectscounter.keys():
    # create subject directory
    subdir = os.path.join(datadir,subject)
    if not os.path.exists(subdir):
            os.mkdir(subdir)

    conditionsfolder = os.path.join(subdir,"conditions")
    if not os.path.exists(conditionsfolder):
            os.mkdir(conditionsfolder)

    # get subject attention file
    subattentionfiles = [x for x in attentionfiles if x.startswith(subject)]

    # check multiplicity
    if subject == 'WI_AMP_001':
        print("Not restructuring subject %s: please do this manually.")
        continue
    elif subject == "WI_AMP_006":
        print("Subject %s has 3 rows, but only one attention file so we can use the first row and ignore the two following."%subject)
        subjectinfo = infosheet.loc['WI_AMP_006_A']
    elif subject == "WI_AMP_013":
        print("Subject %s has 2 rows, but only one attention file so we can use the first row and ignore the following."%subject)
        subjectinfo = infosheet.loc['WI_AMP_013_A']
    elif subject == "WI_AMP_020":
        subjecttable = pd.DataFrame({})
        for attentionfile,subcode in zip(subattentionfiles,['WI_AMP_020_A',"WI_AMP_020_B"]):
            suffix = subcode.split("_")[3]
            subjectinfo = infosheet.loc[subcode]
            subtab, TV, attention = datatransfer.get_attention_and_tv(subject, attentionfile,subjectinfo,attentionfolder,conditionsfolder,suffix="_%s"%suffix)
            TV.to_csv(os.path.join(conditionsfolder,"TV_%s.csv"%suffix),index=None)
            attention.to_csv(os.path.join(conditionsfolder,"attention_%s.csv"%suffix),index=None)
            subjecttable = pd.concat([subjecttable,subtab])
        subjecttable.to_csv(os.path.join(subdir,"subject_information.csv"),index=None)
        # lines for group summary
        subjectinfo = infosheet.loc['WI_AMP_020_A']
        groupcols = ["Female", "Age(months)", "Wristband", "HR data", "Date"]
        newrow = subjectinfo[groupcols]
        allnotes = [str(x) for x in infosheet['Note'][infosheet.index.str.startswith(subject)]]
        newrow['Note'] = ", ".join(allnotes)
        newrow['Subject ID'] = subject
        group_information = group_information.append(newrow)
        continue
    elif not subject in infosheet.index:
        print("Subject %s might have multiple entries and we need a specific path for this."%subject)
        continue
    else:
        subjectinfo = infosheet.loc[subject]
        assert len(subattentionfiles) == 1

    # get attention, tv and subjectinfo
    subjecttable, TV, attention = datatransfer.get_attention_and_tv(subject, subattentionfiles[0],subjectinfo,attentionfolder,conditionsfolder)
    # write to csv
    subjecttable.to_csv(os.path.join(subdir,"subject_information.csv"),index=None)
    TV.to_csv(os.path.join(conditionsfolder,"TV.csv"),index=None)
    attention.to_csv(os.path.join(conditionsfolder,"attention.csv"),index=None)

    # lines for group summary
    groupcols = ["Female", "Age(months)", "Wristband", "HR data", "Date"]
    newrow = subjectinfo[groupcols]
    allnotes = [str(x) for x in infosheet['Note'][infosheet.index.str.startswith(subject)]]
    newrow['Note'] = ", ".join(allnotes)
    newrow['Subject ID'] = subject
    group_information = group_information.append(newrow)

group_information.to_csv(os.path.join(datadir,"group_information.csv"),index=None)
# get Physio
# Note: for physio, we know the exact datetime, so no need to use the subjectinformation anymore !
# This makes it easier for split files...
physiofolder = os.path.join(basedir,"Physio CSV data")
physiosubfolders = os.listdir(physiofolder)

for physiosubfolder in physiosubfolders:
    subject = physiosubfolder[:10]

    subdir = os.path.join(datadir,subject)
    if not os.path.exists(subdir):
            os.mkdir(subdir)

    subphysiofolder = os.path.join(subdir,"physio")
    if not os.path.exists(subphysiofolder):
            os.mkdir(subphysiofolder)

    suffix = ""

    if subject == "WI_AMP_001":
        print("Not restructuring subject %s: please do this manually."%subject)
        continue
    if subject == "WI_AMP_013" or subject == "WI_AMP_006":
        suffix = "_"+physiosubfolder[11:12]
    if subject == "WI_AMP_020":
        suffix = "_"+physiosubfolder[10:11]
    fullfolder = os.path.join(physiofolder,physiosubfolder)
    # loop over all physio files for subject
    for fl in os.listdir(fullfolder):
        # get path of old file and new file
        newfilebase = ".".join(fl.split(".")[:-1])
        newfileext = fl.split(".")[-1]
        oldfile = os.path.join(physiofolder,physiosubfolder,fl)
        newfile =  os.path.join(subphysiofolder,"%s%s.%s"%(newfilebase,suffix,newfileext))
        copyfile(oldfile,newfile)

        # extract onset
        if not oldfile.endswith(".csv") or oldfile.endswith("tags.csv"):
            continue
        data = pd.read_csv(oldfile,header=None)
        onset = dt.datetime.fromtimestamp(data.iloc[0,0])

        # append new file to subjecttable
        subjecttablefile = os.path.join(subdir,"subject_information.csv")
        if os.path.exists(subjecttablefile):
            subjecttable = pd.read_csv(subjecttablefile)
        else:
            subjecttable = pd.DataFrame({})
        newrow = {
            "file": "physio/%s%s.%s"%(newfilebase,suffix,newfileext),
            "origin": onset
            }
        subjecttable = subjecttable.append(newrow,ignore_index=True)
    subjecttable.to_csv(subjecttablefile,index=None)
