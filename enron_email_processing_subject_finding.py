# coding: utf-8

import pandas as pd

import difflib
from joblib import Parallel, delayed

import re

#removelist = ".?!"
removelist = ":"
pattern = r'[^\w'+removelist+']'

def findRootSubject(subject_title):
    subject_root = ' '.join(subject_title.split(':')[1:]).strip()

    if len(subject_root)==0: 
	    return  [None, None]
    else:
	    
        subject_root = re.sub(pattern, ' ', subject_root)
        root_subjects = df_other_subjects[df_other_subjects['Subject'].str.contains(subject_root)]['Subject'].unique()
    
        matched_subjects = [subject for subject in root_subjects if difflib.SequenceMatcher(None, a=subject, b=subject_root).ratio()>0.95]
        return [matched_subjects, subject_title]


df_restricted = pd.read_csv('all_results_process_stage_1a.csv', encoding='utf-8')

df_restricted = df_restricted[df_restricted.duplicated(subset=['DateTime', 'From', 'To', 'Subject']) == False]

reply_subjects = df_restricted[(df_restricted['Subject'].str.contains('RE:')) |                   (df_restricted['Subject'].str.contains('Re:')) |                   (df_restricted['Subject'].str.contains('re:'))]['Subject'].values

fwd_subjects = df_restricted[(df_restricted['Subject'].str.contains('FW:')) |                   (df_restricted['Subject'].str.contains('Fw:')) |                   (df_restricted['Subject'].str.contains('fw:'))]['Subject'].values

thread_names = list(reply_subjects) + list(fwd_subjects)

df_other_subjects = df_restricted[df_restricted['Subject'].isin(thread_names) == False]

other_subjects = df_other_subjects['Subject'].unique()

if __name__ == '__main__':

    root_subjects = Parallel(n_jobs=-1, verbose=8)(delayed(findRootSubject)(subject) for subject in thread_names)

    df_root_subject_matches = pd.DataFrame(root_subjects, columns=['Root', 'Subject'])

    df_root_subject_matches.to_csv('root_subject_matching.csv', index=False,  encoding='utf-8')
