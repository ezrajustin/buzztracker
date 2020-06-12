#!/bin/bash
# This bash script sets up the master node with all appropriate pkgs.

# install Java8
sudo apt update
sudo apt install openjdk-8-jre-headless
java -version

# install Scala
sudo apt install scala
scala -version

# keyless SSH
sudo apt install openssh-server openssh-client
# create RSA key-pair
cd ~/.ssh
ssh-keygen -t rsa -P ""
# type `id_rsa` when asked for file name

# install Spark 2.4.5
wget http://apache.claz.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
tar xvf spark-2.4.5-bin-hadoop2.7.tgz
sudo mv spark-2.4.5-bin-hadoop2.7/ /usr/local/spark

# update`~/.bash_profile` file
echo 'export PATH=/usr/local/spark/bin:$PATH' >>~/.bash_profile
#load new `.bash_profile` config
source ~/.bash_profile

# configure master node to track workers
cp /usr/local/spark/conf/spark-env.sh.template /usr/local/spark/conf/spark-env.sh
echo 'export SPARK_MASTER_HOST=$MASTER_PUBLIC_IP' >>/usr/local/spark/conf/spark-env.sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >>/usr/local/spark/conf/spark-env.sh
echo 'export PYSPARK_PYTHON=python3' >>/usr/local/spark/conf/spark-env.sh
echo 'export PYSPARK_DRIVER_PYTHON=/usr/bin/python3' >>/usr/local/spark/conf/spark-env.sh

# add IPs of worker nodes by adding the following to `/usr/local/spark/conf/slaves` file:
# `# contents of conf/slaves`
# `<worker-private-ip1>`
# `<worker-private-ip2>`
# `<worker-private-ip3>`...
