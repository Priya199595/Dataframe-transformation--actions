# Databricks notebook source
# DBTITLE 1,Transformations (Dataframe)
#Transformation is a type of operation which transforms dataframe from one form to another.It is a lazy evolution which is based on DAG(Directed Acyclic Graph)

#When we want to play with actual data, actions play a vital role there..Actions returns the data to the driver to display. Collect(), count() etc are some examples of action.


#Transformation list.                                Actions list


#.  intersection                                      #reduce. 
                                                       #collect. 
#   distinct                                           foreach
#.  map                                                
#.  flatmap                                            count
#   groupBy                                            first
#.  sortBy                                             fold
#.  filter                                             take
#.  mapPartition                                       takeSample
#   union                                              aggregate                                    
#.  subtract                                            top                                  
#.  mapPartionWithIndex                                 treeAggregate
#.  sample                                              forEachPartition
#.  cartesian                                           save
#.   keyBy                                              countApprox
#.   zipWithIndex                                       countApproxDistinct
#.   zipWithUniqueId                                    histogram
#.   zipPartitions                                       min
#.    zip                                                max
#.    repartition                                        mean
#.    coalesce                                           variance
#.    pipe                                               sum
#.    randomSplit                                        sampleVariance
#                                                         collectAsMap

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/")

# COMMAND ----------

df = spark.read.format("csv").option("inferschema", True).option("header", True).option("sep",",").load("dbfs:/FileStore/tables/final_data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Narrow Transformation1(filter)
df1= df.filter(df.Location=='Karnataka')

# COMMAND ----------

# DBTITLE 1,Narrow Transformation2(filter)
df2= df.filter(df.Location=='Delhi')

# COMMAND ----------

# DBTITLE 1,Narrow Transformation3(union)
df3= df1.union(df2)

# COMMAND ----------

# DBTITLE 1,Wide Transformation(groupBy)
df4 =df3.groupBy('class').count()

# COMMAND ----------

# DBTITLE 1,Time for some Actions(collect)
df4.collect()

# COMMAND ----------

df4.take(2)

# COMMAND ----------


