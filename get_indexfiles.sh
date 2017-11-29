#!/bin/bash
./cdx-index-client.py -c all --fl url,filename,offset,length,status,mime -z $1 -d $2
