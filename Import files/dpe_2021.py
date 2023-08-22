# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
import io
import os
import requests
import re
from copy import copy

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

increase_url = 3334
print_every = 100 * increase_url
print_now = 0

def correct(previous_line, lines, f):
    i = 0
    # handle previous line
    length = len(re.split(',', previous_line))
    while(length < 245):
        previous_line = previous_line[:-1] + lines[i]
        length = len(re.split(',', previous_line))
        i += 1
    f.write(previous_line + '\n')

    end_line = ''
    # handle next_lines
    while i < len(lines):
        line = lines[i]
        length = len(re.split(',', line))
        while(length < 245):
            try:
                line = line[:-1] + lines[i+1]
            except: # index out of range need to get next url
                return line
            length = len(re.split(',', line))
            i += 1
        f.write(line + '\n')
        i += 1

    return end_line


with open('/dbfs/FileStore/tables/dpe_2021.csv', "w", encoding="utf-8") as f: 
    response = requests.get("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=10000&page=1&format=csv")
    lines = response.text.split('\n')[:-1]
    current_line = correct('', lines, f)
    number = increase_url
    while response.text != '':
        if number > print_now:
            print(f'request with number {number//increase_url}')
            print_now += print_every
        url = f'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/lines?size=10000&format=csv&after={number}&header=false'
        response = requests.get(url)
        lines = response.text.split('\n')[:-1]
        end_line = correct(current_line, lines, f)
        number += increase_url
