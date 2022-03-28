#!/bin/sh
spark-submit \
  --class org.example.FirstApp \
  --master local[4] \
  target/spark-exercise1-1.0-SNAPSHOT.jar target/resources/books.csv > results.txt