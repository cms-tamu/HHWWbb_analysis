#  Workflows
Here a proposal:
-  MINIAOD in Brazos
-  c++ code will add HME to MINIAOD and same them on stica ()
-  python miniAOD2RDD.py to save RDD on stica (/data/RDD)
-  used thos DNN to run main analysis
-  main analysis will produce a DNN with additional bool tables (collection of 0 and 1, each representing a selection)
-  training DNN using keras selection and save RDD with DNN ourput
-  create inputs for limits

## ADD HME to MINIAOD
-  TO BE FILLED

## Converting root files to RDD
-  Inputs are defined in utilities/
-  Outputs will be in /data/RDD
-  Execution:
```
spark-submit --class org.apache.spark.deploy.master.Master \
--packages org.diana-hep:spark-root_2.11:0.1.15,org.diana-hep:histogrammar-sparksql_2.11:1.0.4 \
--master spark://cmstca:7077 \
--deploy-mode client $PWD/miniAOD2RDD.py
```
(or python miniAOD2RDD.py)

## Run Analysis
-  Inputs are defined in utilities/
-  Execution:
```
spark-submit --class org.apache.spark.deploy.master.Master \
--packages org.diana-hep:histogrammar-sparksql_2.11:1.0.4 \
--master spark://cmstca:7077 \
--deploy-mode client $PWD/analyzeRDD.py
```
(or python analyzeRDD.py)

## Train DNN
- TO BE FILLED

## Extra
#### Submit a pyspark script
Start the master:
```
/data/spark-2.3.0-bin-hadoop2.7/sbin/start-master.sh
```
-  Give you a file where you can see the marster url: spark://cmstca:7077
Start the slave:
```
/data/spark-2.3.0-bin-hadoop2.7/sbin/start-slave.sh spark://cmstca:7077
```
Then you can submit the job:
```
spark-submit --class org.apache.spark.deploy.master.Master \
--master spark://cmstca:7077 \
--deploy-mode client /home/lpernie/HHWWbb/HHWWbb_analysis/analyzeRDD.py
```
And at the end:
```
/data/spark-2.3.0-bin-hadoop2.7/sbin/stop-all.sh
```

####
Using hadoop command
hdfs dfs -df -h
hdfs dfs -df -h /data/
hadoop fs -ls /

#### Needed in your .bashrc
```
#Exporting stuff
export PYTHONPATH=/home/demarley/Downloads/root/lib:$PYTHONPATH
export LD_LIBRARY_PATH=/home/demarley/anaconda2/lib/:/home/demarley/Downloads/root/lib:$LD_LIBRARY_PATH
export SPARK_HOME=/data/spark
export PATH="/home/demarley/anaconda2/bin:/home/demarley/.local/bin:$PATH"
#SPARK and HADOOP
export PATH=$SPARK_HOME/bin:$PATH
export HADOOP_HOME=/data/hadoop-3.1.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin
#JAVA
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$PATH
#ROOT
source /data/root/bin/thisroot.sh
#Histogrammar
export PYTHONPATH=/data/histogrammar/histogrammar-python:$PYTHONPATH
```
