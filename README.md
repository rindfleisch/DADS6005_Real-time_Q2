# DADS6005_Real-time_Q2


# Steps of command

## Installation and Setup

### Install DBeaver
#### create 2 database connections
1. Open DBeaver -> PostgreSQL connect
-	Database: root
-	User: root
-	Pass: secret
2.Open DBeaver -> PostgreSQL connect
-	Database: quiz02_dev
-	User: root
-	Pass: secret

Open SQL Script

```sql
CREATE DATABASE quiz02_raw;
CREATE DATABASE quiz02_dev;
```

In SQL- quiz02_dev
```sql
SELECT * from quiz02_raw;   ---> This shows empty table
```

### Install psycopg2 (postgresql client)
```shell
conda install -c anaconda psycopg2
```

### Create Table (PostgreSQL) using create_table.py.
```shell
python create_table.py food_coded.csv quiz02_raw
```

### Kafka. 

#### Create Kafka topic

```shell
docker exec -it kafka.quiz02 /bin/bash
```
```shell
kafka-topics --bootstrap-server kafka.quiz02:9092 --create --topic quiz02_raw
```
```shell
kafka-topics --bootstrap-server kafka:9092 --topic quiz02_persist --create --partitions 2 --replication-factor 1
```
### KSQL

#### ksqldb-cli
Run bash to ksqldb-cli.
```shell
docker exec -it ksqldb-cli.quiz02 /bin/bash
```
Run ksql-cli
```shell
ksql http://ksqldb-server.quiz02:8088
```

Set offset to begin (Option)
```sql
SET 'auto.offset.reset' = 'earliest';
```

Create Stream
Raw Zone: create_ksqldb_quiz02_raw_table.sql
```sql
CREATE STREAM quiz02_raw (
index int,
GPA varchar,
Gender int,
breakfast int,
calories_chicken int,
calories_day double,
calories_scone double,
coffee int,
comfort_food varchar,
comfort_food_reasons varchar,
comfort_food_reasons_coded double,
cook double,
comfort_food_reasons_coded_1 int,
cuisine double,
diet_current varchar,
diet_current_coded int,
drink double,
eating_changes varchar,
eating_changes_coded int,
eating_changes_coded1 int,
eating_out int,
employment double,
ethnic_food int,
exercise double,
father_education double,
father_profession varchar,
fav_cuisine varchar,
fav_cuisine_coded int,
fav_food double,
food_childhood varchar,
fries int,
fruit_day int,
grade_level int,
greek_food int,
healthy_feeling int,
healthy_meal varchar,
ideal_diet varchar,
ideal_diet_coded int,
income double,
indian_food int,
italian_food int,
life_rewarding double,
marital_status double,
meals_dinner_friend varchar,
mother_education double,
mother_profession varchar,
nutritional_check int,
on_off_campus double,
parents_cook int,
pay_meal_out int,
persian_food double,
self_perception_weight double,
soup double,
sports double,
thai_food int,
tortilla_calories double,
turkey_calories int,
type_sports varchar,
veggies_day int,
vitamins int,
waffle_calories int,
weight varchar )  WITH (KAFKA_TOPIC='quiz02_raw',VALUE_FORMAT='AVRO');

```

####
Persist Zone: create_ksqldb_quiz02_persist_table.sql
```sql
CREATE STREAM quiz02_persist
with (
    KAFKA_TOPIC = 'quiz02_persist',
    VALUE_FORMAT = 'AVRO',
    PARTITIONS = 2
) as SELECT index,breakfast,coffee,calories_day,drink,eating_changes_coded,exercise,fries,soup,nutritional_check,employment,fav_food,income,sports,
veggies_day,indian_food,Italian_food,persian_food,thai_food,vitamins,self_perception_weight,weight
FROM quiz02_raw 
where calories_day >= 1.0
EMIT CHANGES;
```

#### Create Connector
Create source.
```sql
ksql> CREATE SOURCE CONNECTOR `postgres-dev01` WITH(
    "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
    "connection.url"='jdbc:postgresql://postgres:5432/quiz02_dev?user=root&password=secret',
    "mode"='incrementing',
    "incrementing.column.name"='index',
    "topic.prefix"='',
    "table.whitelist"='quiz02_raw',
    "key"='index');
```

Create sink.
```sql
ksql> CREATE SINK CONNECTOR `elasticsearch-sink-map01` WITH(
    "connector.class"='io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    "connection.url"='http://elasticsearch:9200',
    "connection.username"='',
    "connection.password"='',
    "batch.size"='1',
    "write.method"='insert',
    "topics"='quiz02_all',
    "type.name"='changes',
    "value.converter.schema.registry.url" ='http://schema-registry.quiz02:8081',
    "value.converter" = 'io.confluent.connect.avro.AvroConverter',
    "key.ignore" = 'true',
    "key"='index');

```



#### Verify quiz02_raw
```sql
SELECT index,breakfast,coffee,calories_day,drink,eating_changes_coded,exercise,fries,soup,nutritional_check,employment,fav_food,income,sports,
veggies_day,indian_food,Italian_food,persian_food,thai_food,vitamins,self_perception_weight,weight
FROM quiz02_raw
EMIT CHANGES;
```
![output](https://user-images.githubusercontent.com/82042221/236472100-a83a0c22-5a0c-4e9c-ba42-9ed6c821baa6.jpg)


### Insert Data
$ python insert_data.py  $file_input $table_name

```shell
python insert_data.py food_coded.csv quiz02_raw
```

Result

![output](https://user-images.githubusercontent.com/82042221/236473448-36b477e6-3393-46cd-8a5a-5886ef750704.jpg)



### KSQL MAPPING/CLEANSING (quiz02_raw -> quiz02_analyze)
```sql
CREATE STREAM quiz02_all
with (
    KAFKA_TOPIC = 'quiz02_analyze',
    VALUE_FORMAT = 'AVRO',
    PARTITIONS = 2
) as SELECT index, breakfast, GPA, weight, veggies_day, fav_cuisine, cook
CASE 
	WHEN gender = 1 THEN 'Female'
	WHEN gender = 2 THEN 'Male'
	ELSE 'Unknown'
END AS gender,
CASE 
	WHEN grade_level = 1 THEN 'Freshman'
	WHEN grade_level = 2 THEN 'Sophomore'
	WHEN grade_level = 3 THEN 'Junior'
	WHEN grade_level = 4 THEN 'Senior'
	ELSE 'Unknown'
END AS grade_level,
CASE 
	WHEN breakfast = 1 THEN 'Cereal'
	WHEN breakfast = 2 THEN 'Donut'
	ELSE 'Unknown'
END AS breakfast,
CASE 
	WHEN coffee = 1 THEN 'creamy frapuccino'
	WHEN coffee = 2 THEN 'espresso shown'
	ELSE 'Unknown'
END AS coffee,
CASE
	WHEN calories_day = 1 THEN 'I dont know how many calories i should consume'
	WHEN calories_day = 2 THEN 'It is not at all important'
	WHEN calories_day = 3 THEN 'It is moderately important'
	WHEN calories_day = 4 THEN 'It is very important'
	ELSE 'Unknown'
END AS calories_day,
CASE 
	WHEN drink = 1 THEN 'orange juice'
	WHEN drink = 2 THEN 'soda'
	ELSE 'Unknown'
END AS drink,
CASE

	WHEN cook = 1 THEN 'Every day'
	WHEN cook = 2 THEN 'A couple of times a week '
	WHEN cook = 3 THEN 'Whenever I can, but that is not very often'
	WHEN cook = 4 THEN 'I only help a little during holidays'
	WHEN cook = 5 THEN 'Never, I really do not know my way around a kitchen'
	ELSE 'Unknown'
END AS cook,
CASE
	WHEN fruit_day = 1 THEN 'very unlikely'
	WHEN fruit_day = 2 THEN 'unlikely'
	WHEN fruit_day = 3 THEN 'neutral'
	WHEN fruit_day = 4 THEN 'likely'
	WHEN fruit_day = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS fruit_day,
CASE
    WHEN eating_changes_coded = 1 THEN 'worse'
    WHEN eating_changes_coded = 2 THEN 'better'
    WHEN eating_changes_coded = 3 THEN 'the same'
    WHEN eating_changes_coded = 4 THEN 'unclear'
	WHEN eating_changes_coded = 5 THEN 'healthier'
	WHEN eating_changes_coded = 6 THEN 'unclear'
	WHEN eating_changes_coded = 7 THEN 'drink coffee'
	WHEN eating_changes_coded = 8 THEN 'less food'
	WHEN eating_changes_coded = 9 THEN 'more sweets'
	WHEN eating_changes_coded = 10 THEN 'timing'
	WHEN eating_changes_coded = 11 THEN 'more carbs or snacking'
	WHEN eating_changes_coded = 12 THEN 'drink more water'
	WHEN eating_changes_coded = 13 THEN 'more variety'
    ELSE 'Unknown'
END AS eating_changes_coded,
CASE
	WHEN exercise = 1 THEN 'everyday'
	WHEN exercise = 2 THEN 'twice or three times per week'
	WHEN exercise = 3 THEN 'once a week'
	WHEN exercise = 4 THEN 'sometimes'
	WHEN exercise = 5 THEN 'never'
	ELSE 'Unknown'
END AS exercise,
CASE
	WHEN eating_out = 1 THEN 'Never'
	WHEN eating_out = 2 THEN '1-2 times'
	WHEN eating_out = 3 THEN '2-3 times'
	WHEN eating_out = 4 THEN '3-5 times'
	WHEN eating_out = 5 THEN 'everyday'
END AS eating_out,
CASE
	WHEN fries = 1 THEN 'mcdonald''s fries'
	WHEN fries = 2 THEN 'home fries'
	ELSE 'Unknown'
END AS fries,
CASE
	WHEN soup = 1 THEN 'veggie soup'
	WHEN soup = 2 THEN 'creamy soup'
	ELSE 'Unknown'
END AS soup,
CASE
	WHEN nutritional_check = 1 THEN 'never'
	WHEN nutritional_check = 2 THEN 'on certain products only'
	WHEN nutritional_check = 3 THEN 'very rarely'
	WHEN nutritional_check = 4 THEN 'on most products'
	WHEN nutritional_check = 5 THEN 'on everything'
	ELSE 'Unknown'
END AS nutritional_check,
CASE
	WHEN EMPLOYMENT = 1 THEN 'yes full time'
	WHEN EMPLOYMENT = 2 THEN 'yes part time'
	WHEN EMPLOYMENT = 3 THEN 'no'
	WHEN EMPLOYMENT = 4 THEN 'other'
	ELSE 'Unknown'
END AS EMPLOYMENT,
CASE
	WHEN fav_food = 1 THEN 'cooked at home'
	WHEN fav_food = 2 THEN 'store bought'
	WHEN fav_food = 3 THEN 'both bought at store and cooked at home'
	ELSE 'Unknown'
END AS fav_food,
CASE
	WHEN income = 1 THEN 'less than $15,000'
	WHEN income = 2 THEN '$15,001 to $30,000'
	WHEN income = 3 THEN '$30,001 to $50,000'
	WHEN income = 4 THEN '$50,001 to $70,000'
	WHEN income = 5 THEN '$70,001 to $100,000'
	WHEN income = 6 THEN 'higher than $100,000'
	ELSE 'Unknown'
END AS income,
CASE
	WHEN sports = 1 THEN 'yes'
	WHEN sports = 2 THEN 'no'
	WHEN sports = 99 THEN 'no answer'
	ELSE 'Unknown'
END AS sports,
CASE
	WHEN veggies_day = 1 THEN 'very unlikely'
	WHEN veggies_day = 2 THEN 'unlikely'
	WHEN veggies_day = 3 THEN 'neutral'
	WHEN veggies_day = 4 THEN 'likely'
	WHEN veggies_day = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS veggies_day,
CASE
	WHEN indian_food = 1 THEN 'very unlikely'
	WHEN indian_food = 2 THEN 'unlikely'
	WHEN indian_food = 3 THEN 'neutral'
	WHEN indian_food = 4 THEN 'likely'
	WHEN indian_food = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS indian_food,
CASE
	WHEN italian_food = 1 THEN 'very unlikely'
	WHEN italian_food = 2 THEN 'unlikely'
	WHEN italian_food = 3 THEN 'neutral'
	WHEN italian_food = 4 THEN 'likely'
	WHEN italian_food = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS italian_food,
CASE
	WHEN persian_food = 1 THEN 'very unlikely'
	WHEN persian_food = 2 THEN 'unlikely'
	WHEN persian_food = 3 THEN 'neutral'
	WHEN persian_food = 4 THEN 'likely'
	WHEN persian_food = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS persian_food,
CASE
	WHEN pay_meal_out = 1 THEN 'up to $5.00'
	WHEN pay_meal_out = 2 THEN '$5.01 to $10.00'
	WHEN pay_meal_out = 3 THEN '$10.01 to $20.00'
	WHEN pay_meal_out = 4 THEN '$20.01 to $30.00'
 	WHEN pay_meal_out = 5 THEN '$30.01 to $40.00'
 	WHEN pay_meal_out = 6 THEN 'more than $40.01'
	ELSE 'Unknown'
END AS pay_meal_out,
CASE
	WHEN thai_food = 1 THEN 'very unlikely'
	WHEN thai_food = 2 THEN 'unlikely'
	WHEN thai_food = 3 THEN 'neutral'
	WHEN thai_food = 4 THEN 'likely'
 	WHEN thai_food = 5 THEN 'very likely'
	ELSE 'Unknown'
END AS thai_food,
CASE
	WHEN vitamins = 1 THEN 'yes'
	WHEN vitamins = 2 THEN 'no'
	ELSE 'Unknown'
END AS vitamins,
CASE
	WHEN self_perception_weight = 1 THEN 'slim'
	WHEN self_perception_weight = 2 THEN 'very fit'
	WHEN self_perception_weight = 3 THEN 'just right'
	WHEN self_perception_weight = 4 THEN 'slightly overweight'
	WHEN self_perception_weight = 5 THEN 'overweight'
	WHEN self_perception_weight = 6 THEN 'i dont think myself in this terms'
	ELSE 'Unknown'
END AS self_perception_weight
FROM quiz02_raw
EMIT CHANGES;
```

### KSQLDB Analyze
Analyze Zone:


## Sink NoSQL
CONNECTOR SINK
```sql
CREATE SINK CONNECTOR `elasticsearch-sink-all-01` WITH(
    "connector.class"='io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    "connection.url"='http://elasticsearch:9200',
    "connection.username"='',
    "connection.password"='',
    "batch.size"='1',
    "write.method"='insert',
    "topics"='quiz02_all',
    "type.name"='changes',
    "value.converter.schema.registry.url" ='http://schema-registry.quiz02:8081',
    "value.converter" = 'io.confluent.connect.avro.AvroConverter',
    "key.ignore" = 'true',
    "key"='index');
```
## Visualize

![image](https://user-images.githubusercontent.com/22583786/236402795-6bab83ab-25b9-48f2-aefd-6b8b8abfa559.png)

## Note

Show connector information
```sql
DESCRIBE connector `postgres_test02`;
```
![image](https://user-images.githubusercontent.com/22583786/235336731-9b15db7b-00c0-438a-9bdf-106a9eebf5a7.png)

Show stream information
```sql
DESCRIBE quiz02_raw;
```
![image](https://user-images.githubusercontent.com/22583786/235336810-400fd581-7f07-4a17-8270-1b527e11f09c.png)


Delete connector
```sql
drop connector `postgres_test01`;
```
