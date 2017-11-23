'''
Created on 22 Nov 2017

@author: martinr
'''
import os
import sys
import glob
import logging
from .misc_utils import stripMargin
from .exceptions import SparkEnvException
    
def _setupSpark(spark_home=None,python_path=None):
    '''Setup Spark import environment

    Sets environment variables and adds dependencies to sys.path.

    Parameters
    ----------
    spark_home : optional, default = None
        Path to Spark installation, will use environment variable
        SPARK_HOME if not provided.
    python_path : optional, default = None
        Path to Python for Spark workers (PYSPARK_PYTHON),
        will use the currently running Python if not provided.
    '''
    if not spark_home:
        if not 'SPARK_HOME' in os.environ:
            raise SparkEnvException("Environment variable SPARK_HOME must be set " +
                                   "to the root directory of the SPARK installation")
        spark_home = os.environ.get('SPARK_HOME', None)

    if not python_path:
        python_path = sys.executable

    os.environ['SPARK_HOME'] = spark_home
    os.environ['PYSPARK_PYTHON'] = python_path
        
    spark_home_py = os.path.expandvars("$SPARK_HOME/python")
    sys.path.append(spark_home_py)
    file_list = glob.glob(spark_home_py + "/lib/py4j*.zip")
    if not file_list:
        raise SparkEnvException("py4j*.zip not found - this needs to be on PYTHONPATH")
    sys.path.append(file_list[0])
    try:
        from pyspark import SparkContext  # @UnresolvedImport @UnusedImport
        logging.getLogger(__name__).debug("pyspark successfully configured")        
    except:
        raise SparkEnvException('Required pyspark modules not found!')

def sparkEnv(spark_home=None,python_path=None):
    '''Setup Spark import environment
    
    Try and import pyspark and attempt to setup environement if the 
    import fails.

    Parameters
    ----------
    spark_home : optional, default = None
        Path to Spark installation, will use environment variable
        SPARK_HOME if not provided.
    python_path : optional, default = None
        Path to Python for Spark workers (PYSPARK_PYTHON),
        will use the currently running Python if not provided.
    '''
    
    try:
        from pyspark import SparkContext  # @UnresolvedImport @UnusedImport
        logging.getLogger(__name__).debug("pyspark already on sys.path using that version")
    except:
        try:
            _setupSpark(spark_home,python_path)
        except:
            logging.exception("Cannot import pyspark modules!")
    

def isLocal(spark):
    '''Return True if spark is running in local mode, false otherwise.
    
    Parameters
    ----------
    spark : Valid SparkSession object.
    '''
    master = spark.conf.get("spark.master","")
    return master == "local" or master.startswith("local[")

def versionInfo(spark):
    '''Return information on the spark environment we are running in.
    
    Parameters
    ----------
    spark : Valid SparkSession object.
    '''
    sc = spark.sparkContext
    sparkVersion = sc.version
    sparkMaster  = sc.master
    local        = isLocal(spark)
    defaultPar   = sc.defaultParallelism
    versionInfo = """
        |---------------------------------------------------------------------------------
        | Spark version: {sparkVersion}
        | Spark master : {sparkMaster}
        | Spark running locally? {local}
        | Default parallelism: {defaultPar}
        |---------------------------------------------------------------------------------
        |""".format(**locals())
    return stripMargin(versionInfo)

def getAllConf(spark):
    '''Return current spark configuration as string of key/value lines.
    
    Parameters
    ----------
    spark : Valid SparkSession object.
    '''
    confAll = spark.sparkContext.getConf().getAll()
    return "\n".join(["key: [{}], value: [{}]".format(k,v) for k,v in confAll])



#TODO: Add unittest
if __name__ == '__main__':
    pass