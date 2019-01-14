from utils import utils_combine, utils_onsets
from datetime import datetime, timedelta
from collections import Counter
import pandas as pd
import numpy as np
import os

basedir = "/Users/jokedurnez/Desktop/Heather Info for CAFE Physio Pilot/Preliminary Physio Wristband Data for Mollie/data/"

outsummary = os.path.join(basedir,'derivatives/summary_conditions')
if not os.path.exists(outsummary):
    os.mkdir(outsummary)
outonset = os.path.join(basedir,'derivatives/summary_onsets')
if not os.path.exists(outonset):
    os.mkdir(outonset)
outphysio = os.path.join(basedir,'derivatives/summary_physio')
if not os.path.exists(outphysio):
    os.mkdir(outphysio)


conditions = [
    {
        'cat1': 'Child_Cuts',
        'cat1col': 'cut',
        'cat2': 'attention',
        'cat2col': 'codegeneralised',
        'cat2val': "TV" # used for onsetstats, not for grouping
    },
    {
        'cat1': 'Child_RF',
        'cat1col': 'cut',
        'cat2': 'attention',
        'cat2col': 'codegeneralised',
        'cat2val': "TV"
    },
    {
        'cat1': 'Child_RM',
        'cat1col': 'cut',
        'cat2': 'attention',
        'cat2col': 'codegeneralised',
        'cat2val': "TV"
    },
    {
        'cat1': 'TV',
        'cat1col': 'code',
        'cat2': 'attention',
        'cat2col': 'codegeneralised',
        'cat2val': "TV"
    }
]

################################
# Group categorical conditions
################################

# condition = conditions[1]
for condition in conditions:
    cat2 = condition['cat2']
    cat2col = condition['cat2col']
    cat2val = condition['cat2val']
    cat1 = condition['cat1']
    cat1col = condition['cat1col']
    all_subjects = utils_combine.combine_2_conditions_all(basedir, cat1, cat1col, cat2, cat2col)

    # subset = all_subjects[all_subjects.subject_ID==subid]
    # onsettable = utils_onsets.extract_onsets_subject(all_subjects, subid, cat1, cat1col, cat2, cat2col, cat2val)

    includetransition = True
    # for includetransition in [False, True]:
    suffix = "" if includetransition else "_trimmed"
    outfile = os.path.join(outsummary,"conditions_%s-%s_%s-%s%s.csv"%(cat1,cat1col,cat2,cat2col,suffix))
    grouped = utils_combine.group_2_conditions(all_subjects, cat1col, cat2col, includetransition)
    grouped.to_csv(outfile,index=False)

    onsettable = utils_onsets.extract_onsets(all_subjects, cat1, cat1col, cat2, cat2col, cat2val)
    outfile = os.path.join(outonset,"%s-%s_%s-%s-%s.csv"%(cat1,cat1col,cat2,cat2col,cat2val))
    onsettable.to_csv(outfile,index=False)



######### GROUP PHYSIO

conditions = [
    {
        'cat': 'Child_Cuts',
        'catcol': 'cut'
    },
    {
        'cat': 'Child_RF',
        'catcol': 'cut'
    },
    {
        'cat': 'Child_RM',
        'catcol': 'cut'
    },
    {
        'cat': 'TV',
        'catcol': 'code'
    }
]



variable_of_interest = {
    "ACC": "SVM",
    "EDA": "EDA_0",
    "BVP": "BVP_0",
    "TEMP": "TEMP_0",
    "HR": "HR_0",
    "IBI": "IBI"
}

for condition in conditions:
    incat = condition['cat']
    incatcolumn = condition['catcol']
    group_info_file = os.path.join(basedir,"group_information.csv")
    group_info = pd.read_csv(group_info_file,index_col="Subject ID")

    physio = pd.DataFrame({})
    for subject, row in group_info.iterrows():
        for inphysio, inphysiocolumn in variable_of_interest.items():
            physiocombined = utils_combine.combine_physio_condition(basedir, subject, inphysio, inphysiocolumn, incat, incatcolumn)
            grouped = physiocombined.groupby([incatcolumn,'subject_ID']).aggregate(['mean','std','count','median','sum'])
            grouped.columns = ['mean','std','count','median','sum']
            grouped['metric'] = inphysio
            physio = physio.append(grouped.reset_index(),ignore_index=True)

    physio.to_csv(os.path.join(outphysio,"physio_%s-%s.csv"%(incat, incatcolumn)))
