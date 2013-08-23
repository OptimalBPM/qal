#!/bin/bash

set +v

echo "Staging temp area on local hard drive..."

sudo rm -rf /tmp/qal_dist
sudo rsync -qavz --exclude subdirectory5 --exclude-from .gitignore --exclude .gitignore --exclude .gitignore_global ./ /tmp/qal_dist/

cd /tmp/qal_dist

if [ X"$1" = X"upload" ]; then
  echo "Building and uploading QAL"
  sudo python3.2 setup.py -q sdist bdist_egg upload
else
  echo "Building QAL..."
  sudo python3.2 setup.py -q sdist bdist bdist_egg
fi

echo "Uninstalling..."

sudo pip-3.2 uninstall qal -y

if [ X"$1" = X"upload" ]; then
  echo "Installing from PyPi..giving pypi 5 seconds more to react..."
  sleep 5
  sudo pip-3.2 install qal
else
  echo "Installing from local .egg-file..."
  sudo easy_install3 dist/qal-0.1-py3.2.egg
fi
