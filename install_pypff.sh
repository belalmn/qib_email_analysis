#!/bin/bash

# Download the libpff source
wget https://github.com/libyal/libpff/releases/download/20231205/libpff-alpha-20231205.tar.gz

# Extract the tar.gz file
tar -xzf libpff-alpha-20231205.tar.gz

# Navigate to the extracted directory
cd libpff-20231205

# Build and install the library
python setup.py build
python setup.py install

# Clean up by removing the downloaded and extracted files
cd ../
rm -rf libpff-20231205 libpff-alpha-20231205.tar.gz

echo "libpff installed successfully"