#!/bin/bash

DATE_STRING=$(date +"%Y-%m-%d")
ZIP_NAME="email_analysis_${DATE_STRING}"

# Create temp directory
mkdir -p tmp

# Copy all source files except models
mkdir -p tmp/src
cp -r src/config tmp/src/config
cp -r src/database tmp/src/database
cp -r src/extract tmp/src/extract
cp -r src/load tmp/src/load
cp -r src/notebooks tmp/src/notebooks
cp -r src/transform tmp/src/transform

# Create data folder
mkdir -p tmp/data
mkdir -p tmp/data/interim
mkdir -p tmp/data/processed
mkdir -p tmp/data/chroma
mkdir -p tmp/data/raw

# Create models folder
mkdir -p tmp/src/models

# Copy requirements.txt and config.json files
cp requirements.txt tmp
cp config.json tmp

# Remove all .pyc files and all __pycache__ folders
find tmp -name '*.pyc' -delete
find tmp -name '__pycache__' -delete

# Create zip file
cd tmp
zip -r "${ZIP_NAME}.zip" *
mv "${ZIP_NAME}.zip" ../
cd ..

# Clean up
rm -rf tmp