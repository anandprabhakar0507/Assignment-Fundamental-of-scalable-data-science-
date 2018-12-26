
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[3]:


def minTemperature(df,spark):
    return spark.sql("SELECT MIN(temperature)  as mintemp from washing ").first().mintemp ##INSERT YOUR CODE HERE##


# Please now do the same for the mean of the temperature

# In[4]:


def meanTemperature(df,spark):
    return spark.sql("SELECT AVG(temperature) as meantemp from washing").first().meantemp ##INSERT YOUR CODE HERE##


# Please now do the same for the maximum of the temperature

# In[5]:


def maxTemperature(df,spark):
    return spark.sql("SELECT MAX(temperature) as maxtemp from washing").first().maxtemp ##INSERT YOUR CODE HERE##


# Please now do the same for the standard deviation of the temperature

# In[6]:


def sdTemperature(df,spark):
    return spark.sql("SELECT STDDEV(temperature) AS sdtemp FROM washing").first().sdtemp ##INSERT YOUR CODE HERE##


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[7]:


def skewTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        1/(COUNT(temperature))
    ) *
    SUM (
        POWER((temperature - %s),3)/POWER(%s,3)
    )  

as skwtemp from washing 
                    """  ##INSERT YOUR CODE HERE##
%(meanTemperature(df,spark),sdTemperature(df,spark))) ##INSERT YOUR CODE HERE##


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[8]:


def kurtosisTemperature(df,spark):     
    return spark.sql("""
    SELECT 
        (
            1/Float(COUNT(temperature))
        ) *
        SUM (
            POWER((temperature - %s), 4) / POWER(%s, 4)
        ) AS krttemp FROM washing"""
                    %(meanTemperature(df,spark),sdTemperature(df,spark))).first().krttemp ##INSERT YOUR CODE HERE##


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[9]:


def meanHardness(df,spark):
    return spark.sql("SELECT AVG(hardness) as avghard from washing").first().avghard
def sdHardness(df,spark):
    return spark.sql("SELECT STDDEV(hardness) as sdhard from washing").first().sdhard
def correlationTemperatureHardness(df,spark):
    return spark.sql("""
    SELECT
        (
            SUM((temperature-%s) * (hardness-%s)) / Float(COUNT(temperature))
        ) /
        (
            %s * %s
        )
    AS cortemphrd FROM washing
                        """ %(meanTemperature(df,spark),meanHardness(df,spark),sdTemperature(df,spark),sdHardness(df,spark))).first().cortemphrd ##INSERT YOUR CODE HERE##


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[10]:


### TODO Please provide your Cloudant credentials here by creating a connection to Cloudant and insert the code
# @hidden_cell
credentials_1 = {
  'password':"""d1f55eee7ad178830d6a5a347240d29c85f727dae912abd7489d01165983e462""",
  'custom_url':'https://c0669b07-319f-4f19-a9c4-20b2fc928d85-bluemix:d1f55eee7ad178830d6a5a347240d29c85f727dae912abd7489d01165983e462@c0669b07-319f-4f19-a9c4-20b2fc928d85-bluemix.cloudantnosqldb.appdomain.cloud',
  'username':'c0669b07-319f-4f19-a9c4-20b2fc928d85-bluemix',
  'url':'https://undefined'
}
### Please have a look at the latest video "Connect to Cloudant/CouchDB from ApacheSpark in Watson Studio" on https://www.youtube.com/c/RomeoKienzler
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[11]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[12]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_1['custom_url'].split(':')[2].split('@')[1])    .config("cloudant.username", credentials_1['username'])    .config("cloudant.password",credentials_1['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# In[13]:


df=readDataFrameFromCloudant(database)


# In[14]:


minTemperature(df,spark)


# In[15]:


meanTemperature(df,spark)


# In[16]:


maxTemperature(df,spark)


# In[17]:


sdTemperature(df,spark)


# In[18]:


skewTemperature(df,spark)


# In[19]:


kurtosisTemperature(df,spark)


# In[20]:


correlationTemperatureHardness(df,spark)


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
