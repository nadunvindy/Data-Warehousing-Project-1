import atoti as tt

session = tt.Session(                            #Creare atoti session
    user_content_storage=".content",
    port=9095,
    java_options=["-Xms1G", "-Xmx10G"]
)

db_name = "Project1"
db_user = "postgres"
db_password = "postgres"
db_host = "localhost"
db_port = "5432"

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?user={db_user}&password={db_password}" #Create URL

fact_olympic = session.read_sql(       #Get main fact table
    "SELECT * FROM factolympic",
    keys=["year","country"],
    table_name="Olympic",
    url=jdbc_url,
)

dim_countries = session.read_sql(     #Get countries facttable
    "SELECT * FROM dimcountries",
    keys=["country"],
    table_name="Countries",
    url=jdbc_url)

dim_life = session.read_sql( #Get life expectancy dimension table
    "SELECT * FROM dimlife",
    keys=["year","country"],
    table_name="Life",
    url=jdbc_url)

dim_time = session.read_sql(  #Get time dimension table
    "SELECT * FROM dimtime",
    keys=["year"],
    table_name="Time",
    url=jdbc_url)

fact_olympic.join(dim_countries, fact_olympic["country"] == dim_countries["country"])  #Join tables based 

fact_olympic.join(dim_life, (fact_olympic["year"] == dim_life["year"] )& (fact_olympic["country"] == dim_life["country"]))

fact_olympic.join(dim_time, fact_olympic["year"] == dim_time["year"])

testcube = session.create_cube(fact_olympic)  #Cube creation

hierarchies, levels, measures = testcube.hierarchies, testcube.levels, testcube.measures

hierarchies["Countries","continent"] = [dim_countries["continent"],dim_countries["country"]]
hierarchies["Time","time"] = [dim_time["decade"],dim_time["year"],dim_time['season']]
hierarchies["Lifexpectancy","lifeexpectancy"] = [dim_life["lifeexpectancy"],dim_life['population']]

del hierarchies["Olympic","country"]
del hierarchies["Olympic","year"]
del hierarchies["Time","decade"]
del hierarchies['Time','season']

del hierarchies['Life', 'population']
del hierarchies ['Life', 'lifeexpectancy']

