import sys
import boto3
from datetime import datetime, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, when, isnan, isnull
from pyspark.sql.types import IntegerType, DoubleType, DateType, TimestampType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Argumentos del job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_bucket',
    'input_prefix', 
    'output_bucket',
    'output_prefix'
])

# Inicializar contextos
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuración
INPUT_BUCKET = args['input_bucket']
INPUT_PREFIX = args['input_prefix']  # reports/etl-process1/
OUTPUT_BUCKET = args['output_bucket']
OUTPUT_PREFIX = args['output_prefix']  # reports/etl-process2/

def main():
    """
    Función principal del job de Glue
    """
    print(f"🚀 INICIANDO ETL-2 PROCESS - PYSPARK + TYPE CONVERSION")
    print(f"   ⚡ Motor: PySpark distribuido con conversión de tipos")
    print(f"   📥 Input: s3://{INPUT_BUCKET}/{INPUT_PREFIX}")
    print(f"   📤 Output: s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}")
    print(f"   🎯 Objetivo: Conversión CSV → Parquet con tipos correctos")
    print(f"   🔧 Solución: Problema de 'todo como string' resuelto")
    
    try:
        # 1. Leer CSV más reciente de ETL-1
        input_path = f"s3://{INPUT_BUCKET}/{INPUT_PREFIX}"
        print(f"📖 Leyendo CSV desde: {input_path}")
        
        # Leer usando Spark para mejor rendimiento
        df_spark = spark.read.option("header", "true").csv(input_path)
        
        total_records = df_spark.count()
        total_columns = len(df_spark.columns)
        print(f"📊 Registros leídos: {total_records}")
        print(f"📋 Columnas encontradas: {total_columns}")
        print(f"📝 Nombres de columnas: {df_spark.columns}")
        
        # Mostrar esquema original del CSV
        print(f"\n📋 ESQUEMA ORIGINAL DEL CSV:")
        df_spark.printSchema()
        
        # 2. Procesar datos usando solo PySpark (sin transformaciones)
        # Mantiene exactamente el mismo formato que viene del CSV
        df_processed = process_data(df_spark)
        
        # 3. Escribir como Parquet único (overwrite)
        output_path = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}data.parquet"
        print(f"\n💾 Escribiendo Parquet a: {output_path}")
        print(f"   🎯 Modo: Archivo único (overwrite)")
        print(f"   📊 Registros a escribir: {df_processed.count()}")
        
        # Escribir como archivo único (coalesce a 1 partición)
        df_processed.coalesce(1).write.mode("overwrite").parquet(output_path)
        
        # 4. Verificación
        verify_output(output_path)
        
        print(f"\n🎉 ETL-2 COMPLETADO EXITOSAMENTE")
        print(f"   ✅ Conversión CSV → Parquet con tipos correctos")
        print(f"   ✅ Problema de 'todo string' resuelto")
        print(f"   ✅ {total_records} registros procesados")
        print(f"   ⚡ Motor utilizado: PySpark distribuido + Type Conversion")
        
    except Exception as e:
        print(f"❌ ERROR EN ETL-2: {str(e)}")
        import traceback
        print(f"🔍 TRACEBACK COMPLETO:")
        print(traceback.format_exc())
        raise

def process_data(df_spark):
    """
    Procesa los datos usando PySpark + convierte tipos apropiados
    Corrige el problema de que todo se detecte como string
    
    Args:
        df_spark: DataFrame de Spark con los datos CSV
        
    Returns:
        DataFrame de Spark con tipos correctos
    """
    print("🔄 Procesando datos - PYSPARK + CONVERSIÓN DE TIPOS...")
    print(f"   ⚡ Usando motor distribuido de Spark")
    print(f"   🎯 Objetivo: CSV → Parquet con tipos correctos")
    print(f"   � Solucionando problema: TODO detectado como string")
    
    # Mostrar información básica del DataFrame
    total_records = df_spark.count()
    total_columns = len(df_spark.columns)
    
    print(f"\n📊 INFORMACIÓN DEL DATASET:")
    print(f"   📈 Registros: {total_records}")
    print(f"   � Columnas: {total_columns}")
    print(f"   📝 Nombres: {df_spark.columns}")
    
    # Mostrar esquema
    print(f"\n📋 ESQUEMA ORIGINAL (CSV - todo string):")
    df_spark.printSchema()
    
    # 🔧 CONVERSIÓN DE TIPOS ESPECÍFICA
    print(f"\n🔧 APLICANDO CONVERSIÓN DE TIPOS...")
    
    df_processed = df_spark
    
    # Definir conversiones por columna esperada (case-insensitive)
    # COLUMNAS REALES DEL DATASET CON TIPOS ESPECIFICADOS:
    # usuario_id->String, nombre->String, gerencia->String, ciudad->String, 
    # fecha_primera_conversacion->Date, numero_conversaciones->int, 
    # conversacion_completa->String, feedback_total->String, 
    # numero_feedback->int, pregunta_conversacion->String, 
    # feedback->String, respuesta_feedback->String
    type_conversions = {
        # Fechas (formato DD/MM/YYYY detectado en el dataset real)
        'fecha_primera_conversacion': 'date',
        
        # Enteros
        'numero_conversaciones': 'integer',
        'numero_feedback': 'integer',
        
        # Strings explícitos (aunque por defecto ya son string, los definimos para claridad)
        'usuario_id': 'string',
        'nombre': 'string', 
        'gerencia': 'string',
        'ciudad': 'string',
        'conversacion_completa': 'string',
        'feedback_total': 'string',
        'pregunta_conversacion': 'string',
        'feedback': 'string',
        'respuesta_feedback': 'string'
    }
    
    # Aplicar conversiones según las columnas presentes
    for column in df_processed.columns:
        if column.lower() in type_conversions:
            conversion_type = type_conversions[column.lower()]
            
            print(f"   🔄 Convirtiendo '{column}' → {conversion_type}")
            
            try:
                if conversion_type == 'date':
                    # Intentar varios formatos de fecha comunes
                    df_processed = df_processed.withColumn(
                        column,
                        when(col(column).isNull() | (col(column) == ""), None)
                        .otherwise(
                            # Intentar formato YYYY-MM-DD primero
                            when(col(column).rlike(r'^\d{4}-\d{2}-\d{2}$'), to_date(col(column), 'yyyy-MM-dd'))
                            # Luego DD/MM/YYYY
                            .when(col(column).rlike(r'^\d{2}/\d{2}/\d{4}$'), to_date(col(column), 'dd/MM/yyyy'))
                            # Luego MM/DD/YYYY
                            .when(col(column).rlike(r'^\d{1,2}/\d{1,2}/\d{4}$'), to_date(col(column), 'MM/dd/yyyy'))
                            # Si no coincide, intentar auto-detect
                            .otherwise(to_date(col(column)))
                        )
                    )
                    
                elif conversion_type == 'timestamp':
                    df_processed = df_processed.withColumn(
                        column,
                        when(col(column).isNull() | (col(column) == ""), None)
                        .otherwise(to_timestamp(col(column)))
                    )
                    
                elif conversion_type == 'integer':
                    df_processed = df_processed.withColumn(
                        column,
                        when(col(column).isNull() | (col(column) == "") | (col(column) == "0"), None)
                        .otherwise(col(column).cast(IntegerType()))
                    )
                    
                elif conversion_type == 'double':
                    df_processed = df_processed.withColumn(
                        column,
                        when(col(column).isNull() | (col(column) == ""), None)
                        .otherwise(col(column).cast(DoubleType()))
                    )
                    
                elif conversion_type == 'string':
                    # Para strings, solo limpiamos valores nulos
                    df_processed = df_processed.withColumn(
                        column,
                        when(col(column).isNull(), None)
                        .otherwise(col(column).cast("string"))
                    )
                    
            except Exception as e:
                print(f"   ⚠️  Error convirtiendo {column}: {str(e)} - manteniendo como string")
                continue
        else:
            print(f"   📋 Manteniendo '{column}' como string (no en mapping)")
    
    # Mostrar esquema final con tipos correctos
    print(f"\n✅ ESQUEMA FINAL (con tipos correctos):")
    df_processed.printSchema()
    
    # Mostrar muestra de datos procesados
    print(f"\n📝 MUESTRA DE DATOS PROCESADOS (primeras 3 filas):")
    df_processed.show(3, truncate=False)
    
    # Estadísticas básicas por columna
    print(f"\n📊 ESTADÍSTICAS POST-CONVERSIÓN:")
    for column in df_processed.columns:
        non_null_count = df_processed.filter(df_processed[column].isNotNull()).count()
        null_count = total_records - non_null_count
        column_type = dict(df_processed.dtypes)[column]
        print(f"   📋 {column} ({column_type}): {non_null_count} válidos, {null_count} nulos")
    
    print(f"\n🎉 Procesamiento PySpark + Conversión de Tipos completado")
    print(f"   ✅ Problema de 'todo como string' resuelto")
    print(f"   ✅ Tipos apropiados aplicados para analytics")
    
    return df_processed

def verify_output(output_path):
    """
    Verifica que el archivo Parquet se escribió correctamente
    y muestra detalles de los datos finales
    """
    print("🔍 Verificando output...")
    
    try:
        # Leer el archivo recién escrito
        df_verify = spark.read.parquet(output_path)
        record_count = df_verify.count()
        column_count = len(df_verify.columns)
        
        print(f"\n✅ VERIFICACIÓN EXITOSA:")
        print(f"   📊 Registros en Parquet: {record_count}")
        print(f"   📋 Columnas en Parquet: {column_count}")
        print(f"   📝 Nombres de columnas: {df_verify.columns}")
        
        # Mostrar esquema final del Parquet
        print(f"\n📋 ESQUEMA FINAL DEL PARQUET (con tipos correctos):")
        df_verify.printSchema()
        
        # Verificar tipos específicos
        print(f"\n🔧 VERIFICACIÓN DE TIPOS:")
        for column_name, column_type in df_verify.dtypes:
            print(f"   📋 {column_name}: {column_type}")
        
        # Mostrar muestra de los datos finales
        print(f"\n📝 MUESTRA DE DATOS FINALES (primeras 3 filas):")
        df_verify.show(3, truncate=False)
        
        # Información adicional del archivo
        print(f"\n📁 INFORMACIÓN DEL ARCHIVO:")
        print(f"   📍 Ubicación: {output_path}")
        print(f"   🗃️ Formato: Parquet (columnar) con tipos correctos")
        print(f"   📦 Compresión: Automática")
        print(f"   🔄 Modo escritura: Overwrite (archivo único)")
        print(f"   ✅ Analytics-ready: Tipos apropiados para consultas")
        
    except Exception as e:
        print(f"❌ Error en verificación: {str(e)}")
        import traceback
        print(f"🔍 TRACEBACK:")
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
    job.commit()
