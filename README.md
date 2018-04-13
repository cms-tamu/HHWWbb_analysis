#  Workflows
Here a proposal:
-  MINIAOD in Brazos
-  c++ code will add HME to MINIAOD and same them on stica ()
-  python miniAOD2RDD.py to save RDD on stica (/data/MDD)
-  used thos DNN to run main analysis
-  main analysis will produce a DNN with additional bool tables (collection of 0 and 1, each representing a selection)
-  training DNN using keras selection and save MDD with DNN ourput
-  create inputs for limits

## ADD HME to MINIAOD
-  TO BE FILLED

## Converting root files to MDD
- python miniAOD2RDD.py
- inputs are defined in utilities/
- outputs will be in /data/MDD

## Run Analysis
- TO BE FILLED

## Train DNN
- TO BE FILLED

## Extra
#### Submit a pyspark script using YARN
http://tech.magnetic.com/2016/03/pyspark-carpentry-how-to-launch-a-pyspark-job-with-yarn-cluster.html
```
spark-submit --master yarn-client --queue default \
    --num-executors 20 --executor-memory 1G --executor-cores 2 \
    --driver-memory 1G \
    YOUR_SCRIPT.py
```
At the moment it gives the error:
Exception in thread "main" java.lang.Exception: When running with master 'yarn-cluster' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.

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
```
