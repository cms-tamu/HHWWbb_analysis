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
```
spark-submit --master yarn-client --queue default \
    --num-executors 20 --executor-memory 1G --executor-cores 2 \
    --driver-memory 1G \
    YOUR_SCRIPT.py
```
At the moment it gives the error:
Exception in thread "main" java.lang.Exception: When running with master 'yarn-cluster' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.
