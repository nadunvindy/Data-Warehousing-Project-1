#!/usr/bin/env python
# coding: utf-8

# In[47]:


import psycopg2 #import the PostgreSQL adapter
import pandas as pd #Used for data analysis and manipilation
from sqlalchemy import create_engine #function to vreat a database engine
import numpy as np #For array manipulation
from psycopg2 import OperationalError
import re #python string library
import pandas as pd
import numpy as np
import mlxtend
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


# In[48]:


def create_connection(db_name, db_user, db_password, db_host, db_port): #Python function to create a connection to PostgreSQL server. (Reused from Data Warehousing Lab3)
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


# In[49]:


db_name = "Project1"  #database parameters
db_user = "postgres"
db_password = "postgres"  
db_host = "localhost"  
db_port = "5432"
db_server = "dw_2024"


# In[50]:


connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}" #connection string to the database
engine = create_engine(connection_string) #Create engine for the database


# In[51]:


factdata = pd.read_sql_query("SELECT * FROM factolympic", engine)


# In[52]:


economic_df = pd.read_csv("./Economic data.csv",header=0)


# In[53]:


economic_df.columns = ["year", "yearcode", "country", "code", "poverty", "gdpcap", "gdpgrowth", "intsrv","mort", "hlthexp", "govhlthcap", "prvhlthcap", "exthlthcap"]


# In[54]:


economic_df=economic_df[0:-5] # Last five rows are invalid rows containing info about the data and spaces in the csv file


# In[55]:


mental_df = pd.read_csv("./mental-illness.csv",header=0)


# In[56]:


mental_df.columns = ["country","code","year","depression","schizophrenia","bipolar","eatingdisorder","anxiety"]


# In[57]:


mental_df


# In[58]:


lifeexp_df = pd.read_csv("./life-expectancy.csv", header=0)
lifeexp_df.columns= ["country","code","year","lifeexpectancy"]


# In[59]:


mental_df


# In[60]:


countries_df = pd.read_csv("./list-of-countries_areas-by-continent-2024.csv", header=None)


# In[61]:


countries_df.columns = ["country", "continent"]


# In[62]:


countries_df


# In[63]:


econ_countries = set(economic_df['country'].unique())
mental_countries = set(mental_df['country'].unique())
life_countries = set(lifeexp_df['country'].unique())
country_list = set(countries_df['country'].unique())


# In[64]:


econ_life = pd.merge(economic_df, lifeexp_df, on=['country'], how='inner')


# In[65]:


economic_df.dtypes


# In[66]:


economic_df['year'] = economic_df['year'].astype(int)


# In[67]:


economic_df = economic_df.drop(['code'], axis=1)


# In[68]:


final_merged =  pd.merge(factdata, lifeexp_df, on = ['country','year'] , how='inner')


# In[69]:


final_merged = final_merged.drop(['schizophrenia','bipolar','eatingdisorder','code','bronze','silver','gold'],axis = 1)


# In[70]:


final_merged


# In[71]:


p25 = final_merged['depression'].quantile(0.25)
p50 = final_merged['depression'].quantile(0.50)
p75 = final_merged['depression'].quantile(0.75)


# In[72]:


p75,p50,p25


# In[73]:


mining_df = final_merged[final_merged['total'] != 0]


# In[74]:


mining_df['lifeexpectancy'] = pd.qcut(mining_df['lifeexpectancy'], 5, labels=['Very Low lifeExp','Low lifeExp', 'Medium lifeExp', 'High lifeExp', 'Very High lifeExp'],duplicates="drop")
mining_df['anxiety'] = pd.qcut(mining_df['anxiety'], 5, labels=['Very Low anxiety','Low anxiety', 'Medium anxiety', 'High anxiety', 'Very High anxiety'],duplicates="drop")
mining_df['depression'] = pd.qcut(mining_df['depression'], 5, labels=['Very Low Depression','Low Depression', 'Medium Depression', 'High Depression', 'Very High Depression'],duplicates="drop")
mining_df['total'] = pd.qcut(mining_df['total'], 5, labels=['Very Low ','Low ', 'Medium ', 'High ', 'Very High '],duplicates="drop")


# In[75]:


mining_df


# In[79]:


new_df = mining_df.astype(str)

#TransactionEncoder() was designed to covert lists to array
list = new_df.values.tolist()

#Covert the list to one-hot encoded boolean numpy array. 
#Apriori function allows boolean data type only, such as 1 and 0, or FALSE and TRUE.
te = TransactionEncoder()
array_te = te.fit(list).transform(list)

#Check the array
array_te

#Check the colunms
te.columns_

#Apriori function can handle dataframe only, covert the array to a dataframe
arm_df = pd.DataFrame(array_te, columns = te.columns_)

frequent_itemsets = apriori(arm_df,min_support=0.2,use_colnames =True)

#Check the length of rules
frequent_itemsets['length']=frequent_itemsets['itemsets'].apply(lambda x: len(x))

#Assume the length is 2 and the min support is >= 0.3
frequent_itemsets[ (frequent_itemsets['length']==2) & 
                  (frequent_itemsets['support']>=0.3)]

rules_con = association_rules(frequent_itemsets, metric="confidence",min_threshold=0.2)

rules_lift = association_rules(frequent_itemsets, metric="lift",min_threshold=0.5)

#Based on min confidence (=0.05), 
#output antecedents, consequents, support, confidence and lift.
result_arm = rules_con[['antecedents','consequents','support','confidence','lift']]

new_result_arm = result_arm[result_arm['confidence']>=0.5]


# In[80]:


frequent_itemsets

