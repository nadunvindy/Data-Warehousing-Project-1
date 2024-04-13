#!/usr/bin/env python
# coding: utf-8

# In[1]:


import atoti as tt


# In[2]:


session = tt.Session(                            #Creare atoti session
    user_content_storage=".content",
    port=9095,
    java_options=["-Xms1G", "-Xmx10G"]
)


# In[3]:


db_name = "Project1"
db_user = "postgres"
db_password = "postgres"  
db_host = "localhost"  
db_port = "5432"


# In[4]:


jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?user={db_user}&password={db_password}" #Create URL


# In[5]:


jdbc_url


# In[6]:


fact_olympic = session.read_sql(       #Get main fact table
    "SELECT * FROM factolympic",
    keys=["year","country"],
    table_name="Olympic",
    url=jdbc_url,
)


# In[7]:


dim_countries = session.read_sql(     #Get countries facttable
    "SELECT * FROM dimcountries",
    keys=["country"],
    table_name="Countries",
    url=jdbc_url)


# In[8]:


dim_life = session.read_sql( #Get life expectancy dimension table
    "SELECT * FROM dimlife",
    keys=["year","country"],
    table_name="Life",
    url=jdbc_url)


# In[9]:


dim_time = session.read_sql(  #Get time dimension table
    "SELECT * FROM dimtime",
    keys=["year"],
    table_name="Time",
    url=jdbc_url)


# In[10]:


fact_olympic.join(dim_countries, fact_olympic["country"] == dim_countries["country"])  #Join tables based 


# In[11]:


fact_olympic.join(dim_life, (fact_olympic["year"] == dim_life["year"] )& (fact_olympic["country"] == dim_life["country"]))


# In[12]:


fact_olympic.join(dim_time, fact_olympic["year"] == dim_time["year"])


# In[13]:


session.tables.schema


# In[14]:


fact_olympic.head()


# In[15]:


testcube = session.create_cube(fact_olympic)  #Cube creation


# In[16]:


testcube


# In[17]:


hierarchies, levels, measures = testcube.hierarchies, testcube.levels, testcube.measures


# In[18]:


hierarchies["Countries","continent"] = [dim_countries["continent"],dim_countries["country"]]
hierarchies["Time","time"] = [dim_time["decade"],dim_time["year"],dim_time['season']]
hierarchies["Lifexpectancy","lifeexpectancy"] = [dim_life["lifeexpectancy"],dim_life['population']]


# In[19]:


del hierarchies["Olympic","country"]
del hierarchies["Olympic","year"]
del hierarchies["Time","decade"]
del hierarchies['Time','season']


# In[20]:


del hierarchies['Life', 'population']
del hierarchies ['Life', 'lifeexpectancy']


# In[23]:


list(levels)


# In[24]:


measures


# In[25]:


testcube.query(measures["depression.SUM"],measures["anxiety.SUM"],measures["total.SUM"], levels=[levels[('Countries', 'continent', 'continent')]])  #Query 1 


# In[26]:


testcube.query(measures["depression.SUM"],measures["anxiety.SUM"], levels=[levels[('Countries', 'continent', 'country')]])  #Query 2 


# In[27]:


testcube.query(measures["depression.SUM"],measures["anxiety.SUM"],measures["total.SUM"], levels=[levels[('Time', 'time', 'decade')],levels[('Countries', 'continent', 'country')]])  #Query 3 


# In[28]:


testcube.query(measures["depression.SUM"],measures["anxiety.SUM"],measures["total.SUM"], levels=[levels[('Time', 'time', 'season')],levels[('Countries', 'continent', 'country')]])  #Query 4 


# In[29]:


testcube.query(measures["depression.SUM"],measures["anxiety.SUM"],measures["total.SUM"], levels=[levels['Lifexpectancy', 'lifeexpectancy', 'lifeexpectancy'],levels[('Countries', 'continent', 'continent')]])  #Query 5 


# In[30]:


session.widget

