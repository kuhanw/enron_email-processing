#!/usr/bin/python
import os
import pandas as pd

from joblib import Parallel, delayed

'''
Kuhan Wang 17-11-01
A simple script to grab all the Enron emails from the 
individual folders of each user directory
'''

list_of_users = os.listdir(r'd:\datasets\maildir')

def grabDocsUser(user):
    
    path = r'd:\datasets\maildir\\' + user 
    
    all_folders = os.listdir(path)
    
    docs = [os.listdir(path + '\\' + folder) for folder in all_folders if os.path.isdir(path + '\\' + folder)]

    all_docs = [
        [grabDoc(path + '\\' + all_folders[i]  + '\\' + doc, all_folders[i], user) for doc in docs[i]] 
                for i in range(len(all_folders)) if os.path.isdir(all_folders[i])
                ]
    
    all_docs = [doc for docs in all_docs for doc in docs]

    all_docs = [doc for doc in all_docs if doc is not None]

    if len(all_docs)>0: return pd.concat(all_docs)

    else: return None

def grabDoc(path):
	f = open(path, 'r')
	msg = f.readlines()
    
	if len(msg)<6: 
		return None
    
	else:
		line_0 = msg[0]
		line_1 = msg[1]    
		line_2 = msg[2]
		line_3 = msg[3]
		line_4 = msg[4]
		line_5 = msg[5:]

		df_doc = pd.DataFrame([[line_0, line_1, line_2, line_3, line_4, line_5]])

		df_doc['path'] = path

		return df_doc


def grabDocsUserRecursive(path, list_of_docs):
    
	all_folder_paths = os.listdir(path)

	if len(all_folder_paths) == 0: return None

	all_folder_paths = [path + '\\' + cpath for cpath in all_folder_paths]

	for path in all_folder_paths:
		if os.path.isfile(path):
			df_cdoc = grabDoc(path)
			list_of_docs.append(df_cdoc)
		else:
			grabDocsUserRecursive(path, list_of_docs)

	return list_of_docs

if __name__ == '__main__':

	list_of_docs = []
	
	list_of_docs = Parallel(n_jobs=-1, verbose=8)(delayed(grabDocsUserRecursive)(r'd:\datasets\maildir\\' + user, list_of_docs) for user in list_of_users)
	
	list_of_docs = [pd.concat(docs) for docs in list_of_docs]

	df_docs = pd.concat(list_of_docs)

	df_docs.to_csv('all_results.csv', index=False, encoding='utf-8')

