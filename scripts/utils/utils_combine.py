from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

def combine_physio_condition(basedir, subject, inphysio, inphysiocolumn, incat, incatcolumn):
    # read in physio
    physiofile = os.path.join(basedir,'derivatives/preprocessed',subject,'physio/%s.csv'%inphysio)
    physio = pd.read_csv(physiofile, parse_dates=['timestamp'])

    # read in categorical
    catfile = os.path.join(basedir,'derivatives/preprocessed',subject,'conditions/%s.csv'%incat)
    cat = pd.read_csv(catfile, parse_dates=['timestamp_offset', 'timestamp_onset'])

    # add condition to physio
    for idx,row in cat.iterrows():
        timepoints_in_condition = np.where((physio['timestamp'] > row['timestamp_onset']) & (physio['timestamp'] < row['timestamp_offset']))[0]
        physio.at[timepoints_in_condition,incatcolumn] = row[incatcolumn]

    # remove empty
    physio = physio.dropna(subset=[incatcolumn])
    physio['subject_ID'] = subject

    return physio[['subject_ID','timestamp',incatcolumn, inphysiocolumn]]

def combine_2_conditions(basedir, subject, cat1, cat1col, cat2, cat2col):
    # read in cat1
    cat1file = os.path.join(basedir,'derivatives/preprocessed',subject,'conditions/%s.csv'%cat1)
    cat1 = pd.read_csv(cat1file, parse_dates=['timestamp_offset', 'timestamp_onset'])

    # read in cat2
    cat2file = os.path.join(basedir,'derivatives/preprocessed',subject,'conditions/%s.csv'%cat2)
    cat2 = pd.read_csv(cat2file, parse_dates=['timestamp_offset', 'timestamp_onset'])

    # combine two timelines
    allstamps = cat1.timestamp_onset.append(cat1.timestamp_offset) \
        .append(cat2.timestamp_onset).append(cat2.timestamp_offset) \
        .drop_duplicates().drop_duplicates().sort_values().reset_index(drop=True)
    timeline = pd.DataFrame({"timestamp_onset": allstamps})
    timeline['timestamp_offset'] = np.nan
    for idx,ts in timeline.iterrows():
        if not idx == len(timeline)-1:
            timeline.at[idx,'timestamp_offset'] = timeline.loc[idx+1,'timestamp_onset']
    timeline = timeline.dropna()

    # add conditions to timeline
    timeline[cat1col] = ''
    timeline[cat2col] = ''
    # loop over all time intervals and get cat1 and cat2 coding
    for idx,ts in timeline.iterrows():
        cat1loc = np.where((ts.timestamp_onset >= cat1.timestamp_onset) & (ts.timestamp_offset <= cat1.timestamp_offset))[0]
        if len(cat1loc) == 1:
            cat1val = cat1.loc[cat1loc[0],cat1col]
            timeline.at[idx,cat1col] = cat1val
        else:
            assert (len(cat1loc) == 0)
        cat2loc = np.where((ts.timestamp_onset >= cat2.timestamp_onset) & (ts.timestamp_offset <= cat2.timestamp_offset))[0]
        if len(cat2loc) == 1:
            cat2val = cat2.loc[cat2loc[0],cat2col]
            timeline.at[idx,cat2col] = cat2val
        else:
            assert (len(cat2loc) == 0)

    # add transitions
    timeline['transition'] = False
    for idx,ts in timeline.iloc[1:].iterrows():
        if ts[cat1col] != timeline.loc[idx-1,cat1col]:
            timeline.at[idx,'transition'] = True

    timeline['subject_ID'] = subject
    return timeline

def secondsdif(row):
    dif = (row['timestamp_offset'] - row['timestamp_onset'])
    return dif.total_seconds()


def combine_2_conditions_all(basedir, cat1, cat1col, cat2, cat2col):
    # we first obtain the dataset above for all kids
    group_info_file = os.path.join(basedir,"group_information.csv")
    group_info = pd.read_csv(group_info_file,index_col="Subject ID")

    all_subjects = pd.DataFrame({})
    for subject, row in group_info.iterrows():
        subject_info_file = os.path.join(basedir,subject,"subject_information.csv")
        subject_info = pd.read_csv(subject_info_file)
        combined = combine_2_conditions(basedir, subject, cat1, cat1col, cat2, cat2col)
        all_subjects = all_subjects.append(combined, ignore_index=True)
        all_subjects['duration'] = all_subjects.apply(secondsdif, axis=1)
    return all_subjects

def add_percentage(row,times):
    time = row['sum']
    totaltime = times.loc[(row.name[0],row.name[2]),'sum']
    return time/totaltime

def group_2_conditions(all_subjects, cat1col, cat2col, includetransition):
    if not includetransition:
        data = all_subjects[all_subjects.transition==False]
        if len(data)==0:
            return pd.DataFrame({})
    else:
        data = all_subjects

    grouped = data[['subject_ID', cat1col, cat2col,'duration']].groupby(['subject_ID', cat1col, cat2col]).aggregate(['mean','count','median','sum'])
    grouped.columns = ['mean','count','median','sum']

    # add percentage per cat2col
    totaltimes = grouped[['sum']].groupby(['subject_ID',cat2col]).aggregate('sum')
    grouped['percentage'] = grouped.apply(add_percentage,args=(totaltimes,),axis=1)

    # reorder levels for easy reading
    grouped = grouped.reorder_levels(['subject_ID',cat2col,cat1col], axis=0).sort_index(level=['subject_ID',cat2col]).reset_index()
    return grouped
