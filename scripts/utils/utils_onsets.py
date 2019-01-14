from datetime import datetime, timedelta
from collections import Counter
import pandas as pd
import numpy as np
import os


def extract_onsets_subject(all_subjects, subid, cat1, cat1col, cat2, cat2col, cat2val):
    subset = all_subjects[all_subjects.subject_ID==subid].reset_index(drop=True)
    cat1conds = [x for x in np.unique(subset[cat1col]) if not x == '']

    onsettable = pd.DataFrame({})

    for cond in cat1conds:

        # get event
        eventid = np.where(subset[cat1col] == cond)[0]
        eventset = subset.iloc[eventid]
        cat2agg = eventset[['duration', cat1col, cat2col]].groupby(cat2col).agg(sum)
        inprogress = eventset.loc[min(eventid)][cat2col] == cat2val

        # compute percentage
        if cat2val in list(cat2agg.index):
            percentage = cat2agg.loc[cat2val]['duration'] / float(np.sum(eventset['duration']))
        else:
            percentage = 0.0

        cut_duration = max(eventset.timestamp_offset)-min(eventset.timestamp_onset)
        cut_duration = cut_duration.seconds + cut_duration.microseconds/10**6

        # collect aggregates
        out = {
            "subject_ID": subid,
            "%s_ID"%cat1col: cond,
            "%s_onset"%cat1col: min(eventset.timestamp_onset),
            "%s_duration"%cat1col: cut_duration,
            "%s_at_onset"%cat2col: inprogress,
            "%s_attention_percentage"%cat2col: percentage,
            "%s_attention_entire"%cat2col: percentage == 1
        }
        if inprogress:

            # find first instance of this condition
            beendone = False
            for idx in np.arange(min(eventid))[::-1]:
                if subset.iloc[idx][cat2col] != cat2val:
                    beendone = True
                    break
            if beendone:
                ongoingsince = subset.iloc[idx+1].timestamp_onset
                dif = min(eventset.timestamp_onset) - ongoingsince
                out['time_in_progress'] = dif.seconds + dif.microseconds/10**6

            # find last instance of this condition
            tobedone = False
            for idx in np.arange(min(eventid),len(subset)):
                if subset.iloc[idx][cat2col] != cat2val:
                    tobedone = True
                    break
            if tobedone:
                ongoinguntil = subset.iloc[idx-1].timestamp_offset
                dif = ongoinguntil - min(eventset.timestamp_onset)
                out['time_maintained'] = dif.seconds + dif.microseconds/10**6
        else:

            # find last instance of this condition before current tp
            beendone = False
            for idx in np.arange(min(eventid))[::-1]:
                if subset.iloc[idx][cat2col] == cat2val:
                    beendone = True
                    break
            if beendone:
                stoppedsince = subset.iloc[idx].timestamp_offset
                dif = min(eventset.timestamp_onset) - stoppedsince
                out['time_since last'] = dif.seconds + dif.microseconds/10**6

            # find first instance of this condition after current tp
            tobedone = False
            for idx in np.arange(min(eventid),len(subset)):
                if subset.iloc[idx][cat2col] == cat2val:
                    tobedone = True
                    break
            if tobedone:
                nextevent = subset.iloc[idx].timestamp_onset
                dif = nextevent - min(eventset.timestamp_onset)
                out['time_until_next'] = dif.seconds + dif.microseconds/10**6
        onsettable = onsettable.append(out,ignore_index=True)
    return onsettable

def extract_onsets(all_subjects, cat1, cat1col, cat2, cat2col, cat2val):
    subids = np.unique(all_subjects['subject_ID'])
    bigtable = pd.DataFrame()
    for subid in subids:
        onsettable = extract_onsets_subject(all_subjects, subid, cat1, cat1col, cat2, cat2col, cat2val)
        bigtable = bigtable.append(onsettable,ignore_index=True)
    return bigtable
