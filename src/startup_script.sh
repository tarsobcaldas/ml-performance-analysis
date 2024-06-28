#!/bin/bash

# Update and upgrade the system
sudo apt-get update
sudo apt-get -y upgrade

# Install Java (Zulu OpenJDK 8)
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9
sudo apt-add-repository 'deb http://repos.azulsystems.com/ubuntu stable main'
sudo apt-get update
sudo apt-get -y install zulu-8

# Install Scala
wget https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10.deb
sudo dpkg -i scala-2.12.10.deb

# Install Python 3.7
sudo apt-get -y install python3.7

# Install Python libraries
pip3 install pandas==1.1.5 pyarrow==1.0.1 numpy==1.19.5 koalas==1.7.0 dask==2021.03.0

# Any additional setup can be added here
