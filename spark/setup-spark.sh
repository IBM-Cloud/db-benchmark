#!/bin/bash
set -e

# install java8
sudo apt-get install openjdk-8-jdk
sudo apt-get install default-jdk  -y

cd /tmp
wget https://downloads.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop2.7.tgz
tar xvf spark-3.2.1-bin-hadoop2.7.tgz
mv spark-3.2.1-bin-hadoop2.7 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PYSPARK_PYTHON=`which python`" >> ~/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/sbin" >> ~/.profile
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.7/hadoop-aws-2.7.7.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
cd /tmp

source ~/.profile

#virtualenv spark/py-spark --python=/usr/bin/python3.6
#source spark/py-spark/bin/activate

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade pyspark

# check
python
import pyspark
pyspark.__version__
quit()

