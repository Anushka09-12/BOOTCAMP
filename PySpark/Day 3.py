import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()
import datetime
from pyspark.sql import Row
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]
df=spark.createDataFrame(users)
df\
.select("id","current_city")\
.where('current_city IS NOT NULL')\
.show()
df.show()
from pyspark.sql.functions import * 
df.select("id","current_city").filter(col("current_city").isNotNull()).show()
df.select("id","customer_from").where("customer_from IS NULL").show()
df.select("id","customer_from").show()
df.select("id", "current_city", "customer_from").orderBy("current_city").show()
df.select("id", "current_city", "customer_from").orderBy("current_city").show()
df.select("id", "current_city", "customer_from").orderBy("current_city", ascending = False).show()
df.select("id", "current_city", "customer_from").orderBy(col("customer_from").desc()).show()
df.select("id","current_city","customer_from").orderBy(desc_nulls_last("customer_from")).show()
zipdf = spark.read.option("header", True).option("inferSchema", True).csv("/home/labuser/Desktop/Python/zipcode.csv")
zipdf.show()
zipdf.dtypes
zipdf1 = zipdf.drop_duplicates().orderBy('id')
zipdf1.show()
zipdf1.dropna().show()
zipdf1.na.drop().show()
zipdf1.na.drop(subset = "population").show()
zipdf1.dropna("all",subset="type").show()
zipdf1.fillna(30000).show()
zipdf1.na.fill(30000).show()
zipdf1.na.fill("Mumbai").show()
zipdf1.na.fill({'population': 30000, 'city': 'Mumbai', 'type':'VIP'}).show()
zipdf1.na.drop(thresh = 5).show()
zipdf2 = zipdf1.na.fill({'population': 30000, 'city': 'Mumbai', 'type':'VIP'})
zipdf2.replace("Mumbai", "Delhi").show()
df.select("id", "current_city", "customer_from").replace("", "LA").show()
df.select("id", "current_city", "customer_from").replace("Dallas", "LA", subset = "current_city").show()
df.select("id", "current_city", "customer_from").replace("Dallas", "LA", subset = "current_city").show()
baby_names = spark.read.option("header", True).option("inferSchema", True).csv("/home/labuser/Desktop/Python/Baby_Names.csv")
baby_names.show()
baby_names.groupBy("Year").count().show()
baby_names.groupBy("Year").count().sort(col("Year").desc()).show()
baby_names.write.mode("overwrite").parquet("/Processed Data/Baby Names/baby.parquet")
baby_names.count()
baby_names.write\
.mode("overwrite")\
.option("path", "home/labuser/Desktop/Python/Processed Data/Baby Names/babytableparquet").saveAsTable("babyNames")
baby_names.write\
.mode("overwrite")\
.partitionBy("Year")\
.option("path", "/home/labuser/Desktop/Python/Processed Data/Baby Names/babyPartitionByYear")\
.saveAsTable("babyNamesYear")
spark.sql("select * from babynames where Year = 2008").show()
baby_names.write\
.mode("overwrite")\
.partitionBy("Year","Sex")\
.option("path","/home/labuser/Desktop/Python/Processed Data/Baby_Names_Gender/")\
.saveAsTable("babynames_year_gender")
baby_names.write\
.mode("overwrite")\
.option("maxRecordsPerFile", 5000)\
.partitionBy("Year","Sex")\
.option("path","/home/labuser/Desktop/Python/Processed Data/Baby_Names_GenderMax5k/")\
.saveAsTable("babynames_year_gender_max5K")
