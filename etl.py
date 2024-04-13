#!/usr/bin/env python
# coding: utf-8

# In[109]:


import psycopg2 #import the PostgreSQL adapter
import pandas as pd #Used for data analysis and manipilation
from sqlalchemy import create_engine #function to vreat a database engine
import numpy as np #For array manipulation
from psycopg2 import OperationalError
import re #python string library


# In[110]:


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


# In[111]:


db_name = "Project1"  #database parameters
db_user = "postgres"
db_password = "postgres"  
db_host = "localhost"  
db_port = "5432"
db_server = "dw_2024"


# In[112]:


connection = create_connection(db_name, db_user, db_password, db_host, db_port)  #Create connection to the PostgreSQL server function


# In[113]:


cursor = connection.cursor() #Cursor allow python code to execute PostgreSQL command in a database session


# In[114]:


connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}" #connection string to the database
engine = create_engine(connection_string) #Create engine for the database


# In[115]:


countries_df = pd.read_csv("./list-of-countries_areas-by-continent-2024.csv", header=None)


# In[116]:


countries_df.columns = ["country", "continent"]


# In[117]:


hosts_df = pd.read_csv("./olympic_hosts.csv", header=0)


# In[118]:


hosts_df.columns = ["hostid", "enddate", "startdate","location","name","season","year"]


# In[119]:


lifeexp_df = pd.read_csv("./life-expectancy.csv", header=0)


# In[120]:


lifeexp_df.columns= ["country","code","year","lifeexpectancy"]


# In[121]:


medals_df = pd.read_csv("./olympic_medals.csv", header=0)


# In[122]:


medals_df.columns = ["discipline","year","event","gender","medal","participanttype","participanttitle","url","name","country","code","code3"]


# In[123]:


mental_df = pd.read_csv("./mental-illness.csv",header=0)


# In[124]:


mental_df.columns = ["country","code","year","depression","schizophrenia","bipolar","eatingdisorder","anxiety"]


# In[125]:


economic_df = pd.read_csv("./Economic data.csv",header=0)


# In[126]:


economic_df.columns = ["year", "yearcode", "country", "code", "poverty", "gdpcap", "gdpgrowth", "intsrv","mort", "hlthexp", "govhlthcap", "prvhlthcap", "exthlthcap"]


# In[127]:


population_df = pd.read_csv("./Global Population.csv",header=0,encoding='ISO-8859-1')


# In[128]:


#Data Cleaning


# In[129]:


hosts_df['enddate'] = pd.to_datetime(hosts_df['enddate'], format='%Y-%m-%dT%H:%M:%SZ') #Convert the start times to datetime format
hosts_df['startdate'] = pd.to_datetime(hosts_df['startdate'], format='%Y-%m-%dT%H:%M:%SZ') #Convert the end times to datetime format


# In[130]:


lifeexp_df = lifeexp_df.dropna() #lifeexpectancy has continents with na country codes which will not be required so they are dropped from the dataframe


# In[131]:


economic_df['poverty'] = economic_df['poverty'].replace('..', np.nan) #In each column in the economy data frame there are strings ".." for values with no data which will be replace with NaN 
economic_df['gdpcap'] = economic_df['gdpcap'].replace('..', np.nan)
economic_df['gdpgrowth'] = economic_df['gdpgrowth'].replace('..', np.nan)
economic_df['intsrv'] = economic_df['intsrv'].replace('..', np.nan)
economic_df['mort'] = economic_df['mort'].replace('..', np.nan)
economic_df['hlthexp'] = economic_df['hlthexp'].replace('..', np.nan)
economic_df['govhlthcap'] = economic_df['govhlthcap'].replace('..', np.nan)
economic_df['prvhlthcap'] = economic_df['prvhlthcap'].replace('..', np.nan)
economic_df['exthlthcap'] = economic_df['exthlthcap'].replace('..', np.nan)


# In[132]:


economic_df=economic_df[0:-5] # Last five rows are invalid rows containing info about the data and spaces in the csv file


# In[133]:


economic_df["year"] = economic_df["year"].astype(int) #Convert year to int


# In[134]:


population_df = pd.melt(population_df, id_vars=["Population (Millions of people)"], var_name="Year", value_name="Population")


# In[135]:


population_df['Population'] = population_df['Population'].replace('no data', np.nan)


# In[136]:


population_df.columns = ["country","year","population"]


# In[137]:


def extract_year(value):
    year_pattern = r'\b(18|19|20)\d{2}\b'
    match = re.search(year_pattern, value)
    if match:
        return int(match.group(0))  # Convert the matched year to an integer
    else:
        return None 
        
medals_df['year'] = medals_df['year'].apply(extract_year)  #To obtain the year from the title event.


# In[138]:


medals_df = medals_df.drop(columns=["code"]) #To drop the coutnry code with only two characters as standard and other tables follow the three letter structure


# In[139]:


medals_df = medals_df.rename(columns={"code3":"code"}) #Rename column to code


# In[140]:


olympic_countries = set(medals_df['country'].unique()) #To find countries that have similar names.
list_countries = set(countries_df['country'].unique())


# In[141]:


name_mapping = {
    "Great Britain": "United Kingdom",
    "United States of America": "United States",
    "People's Republic of China": "China",
    "Republic of Korea": "South Korea",
    "Democratic People's Republic of Korea": "North Korea",
    "Russian Federation": "Russia",
    "Republic of Moldova": "Moldova",
    "Islamic Republic of Iran": "Iran",
    "Syrian Arab Republic": "Syria",
    "United Republic of Tanzania": "Tanzania",
    "Hong Kong, China": "Hong Kong",
    "Virgin Islands, US": "United States Virgin Islands",
    "Côte d'Ivoire": "Ivory Coast",
    # Special cases handled individually
    "Chinese Taipei": "Taiwan",  # Taiwan is often referred to as Chinese Taipei in international sports
    "Kosovo": "Kosovo",  # May not be in some lists due to political recognition issues
}


# In[142]:


medals_df['country'] = medals_df['country'].map(name_mapping).fillna(medals_df['country']) #Change the name of countries so that it is the same in both countries


# In[143]:


entities_to_remove = [
    'ROC',
    'Olympic Athletes from Russia',
    'Independent Olympic Athletes',
    'Serbia and Montenegro',
    'Unified Team',
    'Czechoslovakia',
    'Federal Republic of Germany',
    'Soviet Union',
    'German Democratic Republic (Germany)',
    'Yugoslavia',
    'Netherlands Antilles',
    'West Indies Federation',
    'United Arab Republic',
    'Australasia',
    'Bohemia',
    'MIX'
]


# In[144]:


medals_df = medals_df[~medals_df['country'].isin(entities_to_remove)] #Remove non countries


# In[42]:


#Create SQL tables in the PostgreSQL database and upload the data from the dataframes to the tables


# In[39]:


cursor.execute("CREATE TABLE dimcountries (country VARCHAR(255) NOT NULL PRIMARY KEY, continent VARCHAR(255) NOT NULL);")


# In[40]:


connection.commit()


# In[41]:


countries_df.to_sql("dimcountries", con=engine, if_exists="append", index=False)


# In[42]:


cursor.execute("CREATE TABLE economic (year INT NOT NULL, yearcode VARCHAR(255) NOT NULL, country VARCHAR(255) NOT NULL, code VARCHAR(3) NOT NULL,poverty FLOAT,gdpcap FLOAT,gdpgrowth FLOAT,intsrv FLOAT, mort FLOAT, hlthexp FLOAT, govhlthcap FLOAT, prvhlthcap FLOAT, exthlthcap FLOAT);")


# In[43]:


connection.commit()


# In[44]:


economic_df.to_sql("economic", con=engine, if_exists="append", index=False)


# In[45]:


cursor.execute("CREATE TABLE population (country VARCHAR(255), year INT NOT NULL, population FLOAT);")


# In[46]:


connection.commit()


# In[47]:


population_df.to_sql("population", con=engine, if_exists="append", index=False)


# In[48]:


cursor.execute("ROLLBACK")


# In[49]:


cursor.execute("CREATE TABLE lifeexp  (country VARCHAR(255) NOT NULL, code VARCHAR(255) NOT NULL, year INT NOT NULL, lifeexpectancy FLOAT);")


# In[50]:


connection.commit()


# In[51]:


lifeexp_df.to_sql("lifeexp", con=engine, if_exists="append", index=False)


# In[52]:


cursor.execute("CREATE TABLE millness  (country VARCHAR(255) NOT NULL, code VARCHAR(255), year INT NOT NULL, depression FLOAT, schizophrenia FLOAT, bipolar FLOAT, eatingdisorder FLOAT, anxiety FLOAT);")


# In[53]:


connection.commit()


# In[54]:


mental_df.to_sql("millness", con=engine, if_exists="append", index=False)


# In[55]:


cursor.execute("CREATE TABLE hosts  (hostid VARCHAR(255) NOT NULL PRIMARY KEY, enddate TIMESTAMP WITH TIME ZONE, startdate TIMESTAMP WITH TIME ZONE, location VARCHAR(255), name	 VARCHAR(255), season VARCHAR(255), year INT);")


# In[56]:


connection.commit()


# In[57]:


hosts_df.to_sql("hosts", con=engine, if_exists="append", index=False)


# In[58]:


cursor.execute("CREATE TABLE medals (discipline VARCHAR(255), year INT NOT NULL, event VARCHAR(255), gender VARCHAR(255), medal VARCHAR(255), participanttype VARCHAR(255),participanttitle VARCHAR(255), url VARCHAR(255),name VARCHAR(255), country VARCHAR(255), code VARCHAR(255));")


# In[59]:


connection.commit()


# In[60]:


medals_df.to_sql("medals",con=engine,if_exists="append", index=False)


# In[125]:


#Creating the facttable and dimension tables


# In[126]:


#For the facttable my columns are factid,country,year,sum of bronze for that year, sum of silver for that year, sum of gold for that year, mental health statistics


# In[146]:


medal_sums = medals_df.groupby(['year', 'country', 'medal']).size().unstack(fill_value=0).reset_index() #Group the table by type of medals


# In[150]:


medal_sums["total"] = medal_sums["BRONZE"] +  medal_sums["GOLD"] +  medal_sums["SILVER"] #Add total column for medals


# In[151]:


medal_sums.columns = ["year","country","bronze","gold","silver","total"]


# In[162]:


years = hosts_df.drop_duplicates()  #To grab all the years the olympics were held


# In[163]:


years = hosts_df["year"]


# In[164]:


year = pd.DataFrame(data = hosts_df["year"],columns=["year"])


# In[165]:


year = year.drop_duplicates()


# In[157]:


all_years_countries = pd.MultiIndex.from_product([year["year"], countries_df['country'].drop_duplicates()], names=['year', 'country']).to_frame(index=False) #Create a comnonation of all countires and olympic years.


# In[177]:


medal_sums = pd.merge(all_years_countries, medal_sums, on=['year', 'country'], how='inner') #Merge table with medals group table


# In[179]:


medal_sums = medal_sums.fillna(0)


# In[169]:


medal_sums["bronze"] = medal_sums["bronze"].astype(int)


# In[170]:


medal_sums["gold"] = medal_sums["gold"].astype(int)


# In[171]:


medal_sums["silver"] = medal_sums["silver"].astype(int)


# In[172]:


medal_sums["total"] = medal_sums["total"].astype(int)


# In[180]:


facttable = pd.merge(medal_sums, mental_df.drop('code',axis=1 ), on=['year', 'country'], how='inner') #Merge table with mental_illness table


# In[183]:


facttable = facttable.drop_duplicates()


# In[185]:


cursor.execute("CREATE TABLE factolympic ( year INT, country VARCHAR(255), bronze INT,silver INT, gold INT,total INT,depression FLOAT, schizophrenia FLOAT, bipolar FLOAT, eatingdisorder FLOAT,anxiety FLOAT, PRIMARY KEY(year,country) );")


# In[186]:


connection.commit()


# In[187]:


facttable.to_sql("factolympic", con=engine, if_exists="append", index=False)


# In[340]:


year['demidecade'] = year['year'].apply(lambda x: f"{x // 10 * 10} - {(x // 10 * 10) + 9}")


# In[341]:


year.columns = ["year","decade"]


# In[342]:


olympictype = hosts_df.set_index('year')['season'].to_dict()


# In[343]:


year['season'] = year['year'].map(olympictype) #Add season and column to the olympic years table"


# In[88]:


cursor.execute("CREATE TABLE dimtime (year INT PRIMARY KEY, decade VARCHAR(255), season VARCHAR(255));")


# In[89]:


connection.commit()


# In[90]:


year.to_sql("dimtime",con=engine,if_exists="append",index=False)


# In[60]:


lifeexp_df['lifeexpectancy'] = pd.qcut(lifeexp_df['lifeexpectancy'], 3, labels=['Low','Medium', 'High']) #Categorize the lifexepectancy according to deviation


# In[70]:


exp_dim = pd.merge(all_years_countries,lifeexp_df, on=['year', 'country'], how='inner') #Merge tables


# In[78]:


exp_dim['year'] = pd.to_numeric(exp_dim['year'])


# In[74]:


population_df['year'] = pd.to_numeric(population_df['year'])


# In[76]:


exp_dim = pd.merge(exp_dim,population_df, on=['year', 'country'], how='inner')


# In[77]:


exp_dim = exp_dim.drop(columns = ['code'])


# In[80]:


exp_dim['population'] = pd.to_numeric(exp_dim['population'])


# In[81]:


exp_dim['population'] = pd.qcut(exp_dim['population'], 3, labels=['Low','Medium', 'High'])


# In[135]:


cursor.execute("CREATE TABLE dimlife (year INT, country VARCHAR(255), lifeexpectancy VARCHAR(255),population VARCHAR (255), PRIMARY KEY (year,country) );")


# In[136]:


connection.commit()


# In[137]:


exp_dim.to_sql("dimlife", con=engine, if_exists="append", index=False)


# In[101]:


cursor.execute("ALTER TABLE FactOlympic ADD CONSTRAINT fk_fact_population FOREIGN KEY (year) REFERENCES dimtime(year);")


# In[102]:


cursor.execute("ALTER TABLE factolympic ADD CONSTRAINT fk_fact_country FOREIGN KEY (country) REFERENCES dimcountries(country);")


# In[ ]:





# In[ ]:




