
# Assignment 3

Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.

YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
. Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.

So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.


All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 

Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.


```python
def minTemperature(df,spark):
    return spark.sql("SELECT MIN(temperature)  as mintemp from washing ").first().mintemp ##INSERT YOUR CODE HERE##
```

Please now do the same for the mean of the temperature


```python
def meanTemperature(df,spark):
    return spark.sql("SELECT AVG(temperature) as meantemp from washing").first().meantemp ##INSERT YOUR CODE HERE##
```

Please now do the same for the maximum of the temperature


```python
def maxTemperature(df,spark):
    return spark.sql("SELECT MAX(temperature) as maxtemp from washing").first().maxtemp ##INSERT YOUR CODE HERE##
```

Please now do the same for the standard deviation of the temperature


```python
def sdTemperature(df,spark):
    return spark.sql("SELECT STDDEV(temperature) AS sdtemp FROM washing").first().sdtemp ##INSERT YOUR CODE HERE##
```

Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.


```python
def skewTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        1/COUNT(temperature)
    ) *
    SUM (
        POWER((temperature - %s),3)/POWER(%s,3)
    )  

as skwtemp from washing 
                    """  
 %(meanTemperature(df,spark),sdTemperature(df,spark))).first().skwtemp ##INSERT YOUR CODE HERE##
```

Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.


```python
def kurtosisTemperature(df,spark):     
    return spark.sql("""
    SELECT 
        (
            1/COUNT(temperature)
        ) *
        SUM (
            POWER((temperature - %s), 4) / POWER(%s, 4)
        ) AS krttemp FROM washing"""
                    %(meanTemperature(df,spark),sdTemperature(df,spark))).first().krttemp ##INSERT YOUR CODE HERE##
```

Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.


```python
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
```

### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
#axx
### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code

### TODO Please provide your Cloudant credentials here


```python
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
```


```python
#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata
```


```python
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host",credentials_1['custom_url'].split(':')[2].split('@')[1])\
    .config("cloudant.username", credentials_1['username'])\
    .config("cloudant.password",credentials_1['password'])\
    .config("jsonstore.rdd.partitions", 1)\
    .getOrCreate()

```


```python
df=readDataFrameFromCloudant(database)
```

    +--------------------+--------------------+-----+--------+----------+---------+--------+-----+-----------+-------------+-------+
    |                 _id|                _rev|count|flowrate|fluidlevel|frequency|hardness|speed|temperature|           ts|voltage|
    +--------------------+--------------------+-----+--------+----------+---------+--------+-----+-----------+-------------+-------+
    |01324dc538b105fd2...|1-73057608a1abe1c...|    1|    null|      null|       76|    null| null|       null|1545594819311|    228|
    |01324dc538b105fd2...|1-e54cc0d887db725...|    8|      11|acceptable|     null|      73| null|         81|1545594824351|   null|
    |01324dc538b105fd2...|1-9ff1ea3f0a9cfb7...|   10|      11|acceptable|     null|      79| null|         84|1545594826356|   null|
    |01324dc538b105fd2...|1-70a0d111d02d619...|   20|      11|acceptable|     null|      76| null|         86|1545594836374|   null|
    |01324dc538b105fd2...|1-9083d062cdebda4...|   22|      11|acceptable|     null|      74| null|         89|1545594838389|   null|
    |01324dc538b105fd2...|1-8c6e38cb5fe79ba...|   24|      11|acceptable|     null|      78| null|         80|1545594840394|   null|
    |01324dc538b105fd2...|1-30b3af525910d2e...|   25|      11|acceptable|     null|      76| null|         80|1545594841394|   null|
    |01324dc538b105fd2...|1-ddf34782bf64629...|    9|    null|      null|       64|    null| null|       null|1545594843364|    238|
    |01324dc538b105fd2...|1-10fb515718eea6f...|   34|      11|acceptable|     null|      73| null|         86|1545594850417|   null|
    |01324dc538b105fd2...|1-256a3d0a88c5473...|   35|      11|acceptable|     null|      71| null|         99|1545594851417|   null|
    |01324dc538b105fd2...|1-b975ce07e5dc9f2...|   36|      11|acceptable|     null|      74| null|         81|1545594852418|   null|
    |01324dc538b105fd2...|1-333c166de229fa1...|   25|    null|      null|       63|    null| null|       null|1545594891391|    225|
    |01324dc538b105fd2...|1-168701d6efd9235...|   28|    null|      null|       70|    null| null|       null|1545594900398|    223|
    |01324dc538b105fd2...|1-c926540ab7dd2e0...|   18|    null|      null|     null|    null| 1087|       null|1545594906399|   null|
    |01324dc538b105fd2...|1-2d8c8d64e045379...|  108|      11|acceptable|     null|      80| null|         92|1545594924611|   null|
    |01324dc538b105fd2...|1-c40566152d1e3cd...|   22|    null|      null|     null|    null| 1036|       null|1545594926409|   null|
    |01324dc538b105fd2...|1-339cf71de4216a6...|  124|      11|acceptable|     null|     115| null|         92|1545594940659|   null|
    |01324dc538b105fd2...|1-50f2f8387839908...|  126|      11|acceptable|     null|     125| null|        100|1545594942661|   null|
    |01324dc538b105fd2...|1-b58743cda959d30...|   46|    null|      null|       69|    null| null|       null|1545594954442|    238|
    |01324dc538b105fd2...|1-1856f8e5c6be15f...|  140|      11|acceptable|     null|     195| null|         98|1545594956708|   null|
    +--------------------+--------------------+-----+--------+----------+---------+--------+-----+-----------+-------------+-------+
    only showing top 20 rows
    



```python
minTemperature(df,spark)
```




    80




```python
meanTemperature(df,spark)
```




    89.99365683476054




```python
maxTemperature(df,spark)
```




    100




```python
sdTemperature(df,spark)
```




    6.067510244216506




```python
skewTemperature(df,spark)
```




    -0.014621425829490997




```python
kurtosisTemperature(df,spark)
```




    1.784718127479673




```python
correlationTemperatureHardness(df,spark)
```




    -0.028323090551472738



Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
