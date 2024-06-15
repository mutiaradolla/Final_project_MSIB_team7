from pyspark.sql import SparkSession

def read_and_write_sales_data():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Read and Write hasil keuntungan") \
        .config("spark.jars", "/home/mutiara/Downloads/postgresql-42.2.24.jar") \
        .getOrCreate()
    
    # Detail koneksi PostgreSQL (sumber)
    jdbc_url_source = "jdbc:postgresql://localhost:5432/postgres"
    jdbc_properties_source = {
        "user": "postgres",
        "password": "Jepara1605",
        "driver": "org.postgresql.Driver"
    }

    # Membaca data dari tabel PostgreSQL sumber
    df = spark.read.jdbc(url=jdbc_url_source, table='hasil_keuntungan', properties=jdbc_properties_source)
    
    # Menyimpan data sementara sebagai file Parquet di sistem file lokal (opsional, dapat dihapus jika tidak diperlukan)
    df.write.parquet('file:///home/mutiara/dummy_data/hasil_keuntungan', mode='overwrite')

    # Detail koneksi PostgreSQL (OLAP)
    jdbc_url_olap = "jdbc:postgresql://localhost:5432/db_OLAP"
    jdbc_properties_olap = {
        "user": "postgres",
        "password": "Jepara1605",
        "driver": "org.postgresql.Driver"
    }

    # Menulis data ke tabel di database OLAP
    df.write.jdbc(url=jdbc_url_olap, table='hasil_keuntungan2', mode='append', properties=jdbc_properties_olap)
    
    # Menghentikan SparkSession
    spark.stop()

if __name__ == "__main__":
    read_and_write_sales_data()