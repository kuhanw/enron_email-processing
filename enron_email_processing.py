import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import difflib
import numpy as np
from joblib import Parallel, delayed
from joblib.pool import has_shareable_memory

def sequenceMatch(x, y):
    return difflib.SequenceMatcher(None, a=x, b=y).ratio()

def findMatches(subject_x, list_all_subjects, index_pos):
	if index_pos%100 ==0: print (len(list_all_subjects))
	current_subject_bucket = []
		   
	current_subject_bucket = [subject for subject in list_all_subjects if difflib.SequenceMatcher(None, a=subject, b=subject_x).ratio()>0.9]							        
	
	for del_subjects in current_subject_bucket: list_all_subjects.remove(del_subjects)
	
	subject_results.append(current_subject_bucket)

	return 0

subject_results = []

if __name__ == '__main__':

	df_restricted = pd.read_csv('all_results_process_stage_1a.csv')

	subject_counts = df_restricted.groupby('Subject')['MessageID'].count().reset_index()

	df_restricted[(df_restricted['Subject'].str.contains('zano')) & (df_restricted['Subject'].isnull()==False)]['Body'].values[0]
	
	all_subjects = subject_counts['Subject'].values
	
	n_subjects = len(all_subjects)    
	
	list_all_subjects = list(all_subjects)[:n_subjects]
	
#	list_all_subjects.append('     Conectiv Completes Nuclear Plant Sales:Re')

#	results = Parallel(n_jobs=-1, verbose=8)(delayed(has_shareable_memory)(sequenceMatce(subject_x, subject_y) for subject_x in all_subjects for subject_y in all_subjects)
	Parallel(n_jobs=-1, verbose=8, backend='threading')(delayed(has_shareable_memory)(findMatches(subject_x, list_all_subjects, idx)) for idx, subject_x in enumerate(list_all_subjects))

#	print (subject_results)
	df = pd.DataFrame(list(zip(*[subject_results])), columns=['similar_subjects'])

	df['n_strings'] = df['similar_subjects'].apply(len)
	
	print (df[df['n_strings']>2].head()['similar_subjects'].values[:3])

#	results = np.array(results)
#	results = np.reshape(results, (n_subjects, n_subjects))
#	diff_matrix = pd.DataFrame(results, columns=all_subjects, index=all_subjects)

#	print (diff_matrix.head(2))

#	diff_matrix.to_csv('enron_subject_sequence_matrix.csv', index=False, encoding='utf-8')
