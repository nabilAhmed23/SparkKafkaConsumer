# Spark Kafka Consumer  
##### Kafka Consumer to push tweets to database using Spark.  
This SBT project is used to push subscribed tweets to a database using Spark JDBC.  
  
The arguments required to run the spark job are as follows:  
1) Location of consumer `.properties` file:  
The `.properties` file should contain the following properties:  

| Property                     | Description                                            |  
| :--------------------------- | :----------------------------------------------------- |  
| `bootstrap.servers`          | Kafka Bootstrap Server(s) (comma-separated)            |  
| `group.id`                   | Consumer Group ID                                      |  
| `key.deserializer`           | Fully Qualified Class Name of Kafka Key Deserializer   |  
| `value.deserializer`         | Fully Qualified Class Name of Kafka Value Deserializer |  
| `auto.offset.reset`          | Offset Reset Policy, default = earliest                |  
| `database.driver`            | Driver Name for database                               |  
| `database.url`               | JDBC URL for database                                  |  
| `database.table`             | Table name to push tweets into                         |  
| `database.username`          | Username for database authentication                   |  
| `database.password`          | Password for database authentication                   |  
  
Example (MySQL):  
`bootstrap.servers=localhost:9092,localhost:9093`  
`group.id=twitter-consumer`  
`key.deserializer=org.apache.kafka.common.serialization.StringDeserializer`  
`value.deserializer=org.apache.kafka.common.serialization.StringDeserializer`  
`auto.offset.reset=earliest`  
`database.driver=com.mysql.jdbc.Driver`  
`database.url=jdbc:mysql://localhost:3306/MySQLDB`  
`database.table=Twitter_Topics`  
`database.username=admin`  
`database.password=admin`  
  
2) Pipe-separated list of topics:  
The list of topics/keywords to subscribe to.  
  
Syntax:  
`spark-submit --master <master_url> --jars <jdbc_jar> <JAR_file_build> <properties_file> <tweets_table>`  
  
Example:  
`spark-submit --master localhost:7077 --jars mysql-connector-java-8.0.21.jar /path/to/SparkKafkaConsumer.jar /path/to/consumer.properties tweets_table`  
  
Note:  
This project was built on top of Spark 3.0.0 for Hadoop 2.7, with Scala version 2.12.11, Java 8 (u251), and sbt version 1.3.13.  
This project uses Kafka Clients API v2.5.0 of Apache Kafka for producing tweets, and Google's Gson API v2.2.4 for parsing tweets JSON string.  
This project assumes the table used to push tweets has been created before-hand with the following structure:  
| Column                              | Type              |  
| :---------------------------------- | :---------------- |  
| `topic`                             | `String`          |  
| `topic_alias`                       | `String`          |  
| `tweet_id`                          | `Int64`           |  
| `tweet_text`                        | `String`          |  
| `tweet_source`                      | `String`          |  
| `tweet_created_at`                  | `DateTime`        |  
| `tweet_full_text`                   | `String NULL`     |  
| `user_id`                           | `Int64`           |  
| `user_name`                         | `String`          |  
| `user_screen_name`                  | `String`          |  
| `quoted_tweet_id`                   | `Int64 NULL`      |  
| `quoted_tweet_text`                 | `String NULL`     |  
| `quoted_tweet_source`               | `String NULL`     |  
| `quoted_tweet_created_at`           | `DateTime NULL`   |  
| `quoted_tweet_full_text`            | `String NULL`     |  
| `quoted_tweet_user_id`              | `Int64 NULL`      |  
| `quoted_tweet_user_name`            | `String NULL`     |  
| `quoted_tweet_user_screen_name`     | `String NULL`     |  
