# Spark Application for training

## Requrements
* Apache Spark v3
* Apache Maven v3
* Scala 2.12 

## How to run the project
* To build the project run the next command from root directory: `mvn clean package` 
* To run spark application run the next script from root directory: `./run.sh`

Note that as an entry point we use books.csv file from resources. After building the file will be saved in target/resources directory. 
Application running result will be kept in the **result.txt** file

