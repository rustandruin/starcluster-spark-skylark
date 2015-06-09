#!/usr/bin/env bash

# NB: if you got this off github, the sge.tar.gz file is stored in the LFS
# system, so you need to install git-lfs, delete this repo, and redownload the
# repo to get the actual file (otherwise you just get a text pointer)

# Instructions:
# open a fresh Ubuntu Vivid AMI
# copy this file, sge.tar.gz and scimage_13.04.py to ~
# edit /etc/apt/sources.list first to enable multiverse for Vivid
# then run this file from ~
# save this instance as a new starcluster compatible AMI


sudo apt-get -y update; sudo apt-get -y upgrade
sudo apt-get -y install nfs-kernel-server nfs-common portmap
sudo ln -s /etc/init.d/nfs-kernel-server /etc/init.d/nfs
sudo ln -s /lib/systemd/system/nfs-kernel-server.service /lib/systemd/system/nfs.service
sudo apt-get -y install python-scipy python-numpy
mkdir starclustersetup
cp scimage_13.04.py starclustersetup
cd starclustersetup
chmod 764 scimage_13.04.py
sudo python scimage_13.04.py
sudo service apache2 stop
sudo apt-get -y install nginx-core nginx
cd ..
sudo apt-get -y install upstart # for some reason this is missing!
echo 'echo "service portmap \$1" > /etc/init.d/portmap' | sudo bash
sudo chmod 755  /etc/init.d/portmap
cd /opt
sudo rm -rf ./sge6-fresh
cd
mkdir sge
cp sge.tar.gz ./sge
cd sge
tar -xvsf sge.tar.gz
sudo cp -r sge6-fresh /opt
cd
rm -r sge starclustersetup
rm sge.tar.gz scimage_13.04.py
rm doall.sh

