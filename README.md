# DADS6005_Real-time_Q2

1 ภูมิ เจวประเสริฐพัทธ์ 6420412004 \   
2 ก้องปภณ สังข์รุ่ง 6420412001 \
3 กฤติน ค้าไกล 6420412012 \
4 สิรพงศ์ บุญพจน์ศิริ 6420412007 \


# Steps of command

## Installation and Setup

### Install DBeaver
#### create 2 database connections
1. Open DBeaver -> PostgreSQL connect
-	Database: root
-	User: root
-	Pass: secret
2.Open DBeaver -> PostgreSQL connect
-	Database: AS2_dev
-	User: root
-	Pass: secret

Open SQL Script

```sql
CREATE DATABASE AS2_raw;
CREATE DATABASE AS2_dev;
```

In SQL- AS2_dev
```sql
SELECT * from AS2_raw;   ---> This shows empty table
```

### Install psycopg2 (postgresql client)
```shell
conda install -c anaconda psycopg2
```

### Create Table (PostgreSQL) using create_table.py.
```shell
python create_table.py food_coded.csv AS2_raw
```

### Kafka. 

#### Create Kafka topic

```shell
docker exec -it kafka.AS2 /bin/bash
```
```shell
kafka-topics --bootstrap-server kafka.AS2:9092 --create --topic AS2_raw
```
```shell
kafka-topics --bootstrap-server kafka:9092 --topic AS2_persist --create --partitions 2 --replication-factor 1
```
### KSQL

#### ksqldb-cli
Run bash to ksqldb-cli.
```shell
docker exec -it ksqldb-cli.AS2 /bin/bash
```
Run ksql-cli
```shell
ksql http://ksqldb-server.AS2:8088
```

Set offset to begin (Option)
```sql
SET 'auto.offset.reset' = 'earliest';
```

Create Stream
Raw Zone: create_ksqldb_AS2_raw_table.sql
```sql
CREATE STREAM AS2_raw (
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
weight varchar )  WITH (KAFKA_TOPIC='AS2_raw',VALUE_FORMAT='AVRO');

```

####
Persist Zone: create_ksqldb_AS2_persist_table.sql
```sql
CREATE STREAM AS2_persist
with (
    KAFKA_TOPIC = 'AS2_persist',
    VALUE_FORMAT = 'AVRO',
    PARTITIONS = 2
) as SELECT index,breakfast,coffee,calories_day,drink,eating_changes_coded,exercise,fries,soup,nutritional_check,employment,fav_food,income,sports,
veggies_day,indian_food,Italian_food,persian_food,thai_food,vitamins,self_perception_weight,weight
FROM AS2_raw 
EMIT CHANGES;
```

#### Create Connector
Create source.
```sql
ksql> CREATE SOURCE CONNECTOR `postgres-dev01` WITH(
    "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
    "connection.url"='jdbc:postgresql://postgres:5432/AS2_dev?user=root&password=secret',
    "mode"='incrementing',
    "incrementing.column.name"='index',
    "topic.prefix"='',
    "table.whitelist"='AS2_raw',
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
    "topics"='AS2_all',
    "type.name"='changes',
    "value.converter.schema.registry.url" ='http://schema-registry.AS2:8081',
    "value.converter" = 'io.confluent.connect.avro.AvroConverter',
    "key.ignore" = 'true',
    "key"='index');

```



#### Verify AS2_raw
```sql
SELECT index,breakfast,coffee,calories_day,drink,eating_changes_coded,exercise,fries,soup,nutritional_check,employment,fav_food,income,sports,
veggies_day,indian_food,Italian_food,persian_food,thai_food,vitamins,self_perception_weight,weight
FROM AS2_raw
EMIT CHANGES;
```
![output](https://user-images.githubusercontent.com/82042221/236472100-a83a0c22-5a0c-4e9c-ba42-9ed6c821baa6.jpg)


### Insert Data
$ python insert_data.py  $file_input $table_name

```shell
python insert_data.py food_coded.csv AS2_raw
```

Result

![output](https://user-images.githubusercontent.com/82042221/236473448-36b477e6-3393-46cd-8a5a-5886ef750704.jpg)

### Export file from PosgreSQL
Q1 Easy Question: What is the average number of times college students consume vegetables per week?
To answer this question, we use simple statistics by calculating the mean value of the 'veggies_day' column in the dataset.
```sql
SELECT AVG(veggies_day) FROM AS2_raw;
```
![4584f49a-29c6-4fbb-a94f-22700bd661c9](https://user-images.githubusercontent.com/90588689/236502387-11e103a6-07f5-4ec0-82ee-e47bd87c3c52.jpg)


Q2 Medium Question: What are the most popular cuisines among college students respectively ? For this question, we perform selection from  the 'fav_cuisine_coded' column (favorite cuisine of the students). Then, use aggregation functions to find the most popular cuisines.
```sql
SELECT

    CASE

        WHEN fav_cuisine_coded = 1 THEN 'Italian/French/Greek'

        WHEN fav_cuisine_coded = 2 THEN 'Spanish/Mexican'

        WHEN fav_cuisine_coded = 3 THEN 'Arabic/Turkish'

        WHEN fav_cuisine_coded = 4 THEN 'Asian/Chinese/Thai/Nepal'

        WHEN fav_cuisine_coded = 5 THEN 'American'

        WHEN fav_cuisine_coded = 6 THEN 'African'

        WHEN fav_cuisine_coded = 7 THEN 'Jamaican'

        WHEN fav_cuisine_coded = 8 THEN 'Indian'

        ELSE 'None'

    END AS fav_cuisine,

    COUNT(*) AS count

FROM AS2_raw

GROUP BY fav_cuisine_coded

ORDER BY count DESC

LIMIT 3;
```

![e5cbeefa-2340-4b6e-9159-faf9059a3749](https://user-images.githubusercontent.com/90588689/236502343-6b89390e-ae2d-424a-925e-46e988359962.jpg)

Q3 Hard Question: What are the average number of vegetables consumed per day for each nutritional awareness level. We use 'nutritional_check'  and 'veggies_day' columns to find the average number of vegetables consumed per day for each nutritional awareness level.

```sql
SELECT

    CASE

        WHEN nutritional_check = 1 THEN 'Never'

        WHEN nutritional_check = 2 THEN 'On certain products only'

        WHEN nutritional_check = 3 THEN 'Very rarely'

        WHEN nutritional_check = 4 THEN 'On most products'

        WHEN nutritional_check = 5 THEN 'On everything'

        ELSE 'None'

    END AS fav_cuisine,

    AVG(veggies_day) AS avg_veggies_per_day

FROM AS2_raw

GROUP BY nutritional_check;
```

![ee6a41b0-2341-4a81-945f-8e4e18cf18c3](https://user-images.githubusercontent.com/90588689/236514160-45a603eb-cb06-4643-8e1a-e57bbc6bbad7.jpg)



## Visualize

Q2

![e62fada8-90de-4dc4-bd41-70a69674d504](https://user-images.githubusercontent.com/90588689/236518496-308f9a1b-b149-4e93-a98c-59aa794be8d0.jpg)

Q3

![204582a0-72da-4804-b8f5-bbfe6743b866](https://user-images.githubusercontent.com/90588689/236518562-f4abdd28-81c6-415e-bd17-4ad9ed215e1e.jpg)


