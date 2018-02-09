import pandas as pd
import numpy as np
import psycopg2
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from sqlalchemy import create_engine

# Connect to Postgres database
conn = psycopg2.connect(host = "gallery.iems.northwestern.edu",dbname = "iems308", user = "pkk503", password = "pkk503_pw")
cursor = conn.cursor()

# Pull all transactions from a specific date as sampling method
cursor.execute("SELECT * FROM pos.trnsact WHERE c6 = '2005-05-05'")
all_data = pd.DataFrame(cursor.fetchall())

# Clean data and pull important features
all_data = all_data[[0,1,2,3,6,11]]
all_data.columns = ['sku','store','register','trannum','stype','sequence']
all_data['stype'] = all_data['stype'].str.strip()

# Select only purchase data
all_data = all_data[all_data['stype'] == 'P']
del all_data['stype']

# Eliminate duplicates
all_data = all_data.drop_duplicates(['sku','store','trannum','register','sequence'])

# Remove extra spaces in values
all_data['sku'] = all_data['sku'].str.strip()
all_data['store'] = all_data['store'].str.strip()
all_data['register'] = all_data['register'].str.strip()
all_data['trannum'] = all_data['trannum'].str.strip()
all_data['sequence'] = all_data['sequence'].str.strip()
all_data['unique_tran'] = all_data['store'].astype(str) + all_data['trannum'].astype(str)

transactions = all_data.copy()
del transactions['sku']
del transactions['sequence']
transactions = transactions.drop_duplicates(['store','register','trannum'])
transactions = transactions.reset_index()
transactions = transactions.rename(columns={'index':'unique_tran'})

# Create "quantity column" so that mlxtend will work on the data
all_data['sold'] = 1

# Reset indices and create unique transaction identifier column
all_data = all_data.reset_index()
del all_data['index']
del all_data['store']
del all_data['trannum']

# Connect to Postgres database
conn = psycopg2.connect(host = "gallery.iems.northwestern.edu",dbname = "iems308", user = "pkk503", password = "pkk503_pw")
cursor = conn.cursor()

# Upload unique transactions table and all data table to database
engine=create_engine('postgresql+psycopg2://pkk503:pkk503_pw@gallery.iems.northwestern.edu:5432/iems308')
transactions.to_sql('transactions',engine,schema = 'pkk503_schema',if_exists = 'append',index = False)
all_data.to_sql('all_data','engine',schema = 'pkk503_schema',if_exists = 'append',index = False)
conn.commit()

# Pull important features from inner join between unique transactions and all data
cursor.execute("SELECT unique_tran, s2.sku, s2.sold, s2.store FROM pkk503_schema.transactions og INNER JOIN pkk503_schema.all_data s2 ON (og.store = s2.store) AND (og.register = s2.register) AND (og.trannum = s2.trannum)")
data = pd.DataFrame(cursor.fetchall())
data.columns = ['unique_tran','sku','sold','store']

# Randomly sample 100 stores from data to avoid memory error
stores = data['store']
stores = stores.to_frame()
stores = stores.drop_duplicates()
rand_stores = stores.sample(100)
data = pd.merge(data,rand_stores,on = ['store'],how = 'inner')
del data['store']

# Create unique baskets with skus as binary columns
basket = (data
          .groupby(['unique_tran', 'sku'])['sold']
          .sum().unstack().reset_index().fillna(0)
          .set_index('unique_tran'))

# Run apriori algorithm with different minimum support values
# Create association rules and sort on lift and confidence
frequent_itemsets = apriori(basket, min_support=0.00034, use_colnames=True)
rules_lift = association_rules(frequent_itemsets,metric = 'lift',min_threshold = 140)
rules_confidence = association_rules(frequent_itemsets,metric = 'confidence')

# Transform sku frozensets into a dataframe and remove duplicates
final_skus = []
for ii in range(0,len(rules_confidence)-1):
    if (len(rules_confidence['antecedants'].iloc[ii]) != 1):
        final_skus.append(list(rules_confidence['antecedants'].iloc[ii])[0])
        final_skus.append(list(rules_confidence['antecedants'].iloc[ii])[1])
        final_skus.append(list(rules_lift['consequents'].iloc[ii]))
    else:
        final_skus.append(list(rules_confidence['consequents'].iloc[ii]))
        final_skus.append(list(rules_confidence['antecedants'].iloc[ii]))

for ii in range(0,len(rules_lift)-1):
    if (len(rules_lift['antecedants'].iloc[ii]) != 1):
        final_skus.append(list(rules_lift['antecedants'].iloc[ii])[0])
        final_skus.append(list(rules_lift['antecedants'].iloc[ii])[1])
        final_skus.append(list(rules_lift['consequents'].iloc[ii]))
    else:
        final_skus.append(list(rules_lift['consequents'].iloc[ii]))
        final_skus.append(list(rules_lift['antecedants'].iloc[ii]))

final_skus = np.hstack(final_skus)
final_skus = pd.DataFrame(final_skus,index = range(0,len(final_skus)), columns = ['sku'])
final_skus = final_skus.astype(int)
final_skus = final_skus.drop_duplicates()

# Connect to Postgres database
conn = psycopg2.connect(host = "gallery.iems.northwestern.edu",dbname = "iems308", user = "pkk503", password = "pkk503_pw")
cursor = conn.cursor()

# Upload final skus to database
engine=create_engine('postgresql+psycopg2://pkk503:pkk503_pw@gallery.iems.northwestern.edu:5432/iems308')
final_skus.to_sql('final_skus',engine,schema = 'pkk503_schema',if_exists = 'append',index = False)
conn.commit()

# Pull final skus with more qualitative information
cursor.execute("SELECT B.sku, brand, style, color FROM pos.skuinfo A RIGHT JOIN pkk503_schema.final_skus B ON A.sku = B.sku")
final_skus = pd.DataFrame(cursor.fetchall())
final_skus.columns = ['sku','brand','style','color']
final_skus.to_csv("final_skus.csv",index = False)
rules_confidence.to_csv("confidence.csv",index = False)
rules_lift.to_csv("lift.csv",index = False)