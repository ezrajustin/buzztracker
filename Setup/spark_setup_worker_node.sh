#!/bin/bash
# This bash script sets up the worker nodes with all appropriate pkgs.

# install Java8
sudo apt update
sudo apt install openjdk-8-jre-headless
java -version

# install Scala
sudo apt install scala
scala -version

# add id_rsa pub key to all workers so that master can communicate with workers
~/.ssh/authorized_keys

# install Spark 2.4.5
wget http://apache.claz.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
tar xvf spark-2.4.5-bin-hadoop2.7.tgz
sudo mv spark-2.4.5-bin-hadoop2.7/ /usr/local/spark

# update`~/.bash_profile` file
echo 'export PATH=/usr/local/spark/bin:$PATH' >>~/.bash_profile
#load new `.bash_profile` config
source ~/.bash_profile
