import recordlinkage
import pandas

# Configuration Parameters

match_threshold = 0
name_threshold = 0.8
email_threshold = 0.4

print(f'Match Threshold: {match_threshold}')
print(f'Name Threshold: {name_threshold}')
print(f'Email Threshold: {email_threshold}')

# Load Dataframe
consolidated_path = 'er_source/Consolidated.csv'
df_consolidated = pandas.read_csv(consolidated_path)

# Configure Indexer
indexer = recordlinkage.Index()
indexer.full()
# indexer.sortedneighbourhood(left_on='NAME_FULL', right_on='NAME_FULL',window=9)
candidate_links = indexer.index(df_consolidated)

# Configure and Compute Comparisons
c = recordlinkage.Compare()
c.string('NAME_FULL', 'NAME_FULL', method='damerau_levenshtein', threshold=name_threshold, label='NAME_FULL')
# c.string('EMAIL_ADDRESS', 'EMAIL_ADDRESS', method='damerau_levenshtein', threshold=email_threshold, label='EMAIL_ADDRESS')
feature_vectors = c.compute(candidate_links, df_consolidated)

# See Results
matches = feature_vectors[feature_vectors.sum(axis=1) > match_threshold]

# Build Mention Sets

collection_of_sets = []

match_list = matches.index.values
for index_a, index_b in match_list:
    foundSet = False
    for setItem in collection_of_sets:
        if index_a in setItem:
            foundSet = True
            if index_b not in setItem:
                setItem.add(index_b)
            else:
                pass
        else:
            if index_b in setItem:
                foundSet = True
                setItem.add(index_a)
            else:
                pass
    if not foundSet:
        newSet = {index_a,index_b}
        collection_of_sets.append(newSet)

# Write Names Results to Console
print(f'Number of Resolved Entities: {len(collection_of_sets)}')
for setItem in collection_of_sets:
    print('----------------------------------------------------------------------------')
    for mentionID in setItem:
        print(f'{df_consolidated.iloc[mentionID][1]}\t{df_consolidated.iloc[mentionID][3]}\t{df_consolidated.iloc[mentionID][4]}')
    print('Email Addresses:')
    for mentionID in setItem:
        if (df_consolidated.iloc[mentionID][4] == 'EMAIL_FROM' or df_consolidated.iloc[mentionID][4] == 'RECIPIENT') and not pandas.isna(df_consolidated.iloc[mentionID][2]):
            print(f'\t{df_consolidated.iloc[mentionID][2]}')

# # Write Email Results to Console
# print()
# print('Email Addresses per Name Similarity')
# for setItem in collection_of_sets:
#     print('----------------------------------------------------------------------------')
#     for mentionID in setItem:
#         if (df_consolidated.iloc[mentionID][4] == 'EMAIL_FROM' or df_consolidated.iloc[mentionID][4] == 'RECIPIENT') and not pandas.isna(df_consolidated.iloc[mentionID][2]):
#             print(f'{df_consolidated.iloc[mentionID][2]}\t{df_consolidated.iloc[mentionID][4]}')

# print(f' {index_a}\t{df_consolidated.iloc[index_a][1]}\t{index_b}\t{df_consolidated.iloc[index_b][1]}')



