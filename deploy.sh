#!/usr/bin/env bash
rm -rf dist
rm -rf pip_modules
mkdir dist
cp driver.py dist/
pip install -r requirements.txt -t pip_modules
cd pip_modules
zip -r ../dist/dependencies.zip ./*
cd ..
python setup.py bdist_wheel

for f in dist/*.whl; do
    mv -- "$f" "${f%.whl}.zip"
done

#Push to S3 here...
