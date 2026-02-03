from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

os.environ['HADOOP_USER_NAME'] = 'root'

# 1. Создаем сессию Spark
# Имя хоста 'namenode' берем из нашего docker-compose
spark = SparkSession.builder \
    .appName("QuickHDFSTest") \
    .master("local[*]") \
    .config("dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# 2. Тестовые данные
data = [("Ivan", 25), ("Anna", 30), ("Petr", 18), ("Elena", 45)]

# 3. Описываем схему
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# 4. Создаем DataFrame
df = spark.createDataFrame(data, schema)

# 5. Показываем результат в консоли
print("Наш легкий DataFrame:")
df.show()

# 6. Записываем в HDFS (путь внутри контейнера к NameNode)
# Мы создадим папку /user/spark_test
try:
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/spark_test")
    print("✅ Успешно сохранено в HDFS!")
except Exception as e:
    print(f"❌ Ошибка записи: {e}")

# 7. Читаем обратно для проверки
print("Чтение записанных данных из HDFS:")
df_load = spark.read.parquet("hdfs://namenode:9000/user/spark_test")
df_load.filter(df_load.age > 20).show()

spark.stop()
