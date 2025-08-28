import sys
import boto3
import json
import ast
from datetime import datetime, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, when, isnan, isnull, udf, lit, lower, trim
from pyspark.sql.types import IntegerType, DoubleType, DateType, TimestampType, StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Intentar importar tiktoken con instalación dinámica si es necesario
TIKTOKEN_AVAILABLE = False
try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
    print("✅ TIKTOKEN: Importación exitosa")
except ImportError as e:
    print(f"❌ TIKTOKEN: Error de importación - {str(e)}")
    print("🔄 TIKTOKEN: Intentando instalación dinámica...")
    try:
        import subprocess
        import sys
        # Intentar instalar tiktoken dinámicamente
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tiktoken"])
        print("✅ TIKTOKEN: Instalación dinámica exitosa, reintentando importación...")
        import tiktoken
        TIKTOKEN_AVAILABLE = True
        print("✅ TIKTOKEN: Importación exitosa después de instalación dinámica")
    except Exception as install_error:
        print(f"❌ TIKTOKEN: Falló instalación dinámica - {str(install_error)}")
        print("🔄 TIKTOKEN: Continuando con aproximación matemática")
except Exception as e:
    print(f"⚠️ TIKTOKEN: Error inesperado - {str(e)}")
    print("🔄 TIKTOKEN: Continuando con aproximación matemática")

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

# ===================================================================
# FUNCIONES PARA CÁLCULO DE TOKENS CON TIKTOKEN + FALLBACK
# ===================================================================

def get_tiktoken_encoding():
    """
    Obtiene el encoding de tiktoken para GPT-3.5/GPT-4 (cl100k_base)
    Con manejo robusto de errores para AWS Glue
    """
    if not TIKTOKEN_AVAILABLE:
        print("⚠️ TIKTOKEN: No disponible, usando fallback")
        return None
    
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        print("✅ TIKTOKEN: Encoding cl100k_base cargado exitosamente")
        return encoding
    except Exception as e:
        print(f"❌ TIKTOKEN: Error obteniendo encoding - {str(e)}")
        return None

def extract_user_text_from_conversation(conversation_text):
    """
    Extrae todos los textos del 'user' de una conversación completa.
    
    Args:
        conversation_text (str): String con formato de lista de diccionarios
        
    Returns:
        str: Texto concatenado de todas las preguntas del usuario
    """
    if not conversation_text or conversation_text.strip() == "":
        return ""
    try:
        # Normalizar separadores y limpiar saltos de línea/tabulaciones
        text = conversation_text.replace(' || ', '|').replace(' | ', '|').replace('||', '|')
        text = text.replace('\n', ' ').replace('\t', ' ')
        messages = [msg.strip() for msg in text.split('|') if msg.strip()]
        user_texts = []
        user_prefixes = ['user:', 'usuario:', 'usr:', 'u:']
        for msg in messages:
            msg_norm = msg.lstrip().lower()
            for prefix in user_prefixes:
                if msg_norm.startswith(prefix):
                    idx = msg.lower().find(prefix)
                    clean_text = msg[idx+len(prefix):].strip()
                    if clean_text:
                        user_texts.append(clean_text)
                    break
        # Concatenar y limpiar espacios dobles/triples
        result = ' '.join(user_texts)
        while '  ' in result:
            result = result.replace('  ', ' ')
        return result.strip()
    except Exception as e:
        print(f"⚠️ ERROR extrayendo textos de usuario: {str(e)[:100]}")
        return ""

def extract_bot_text_from_conversation(conversation_text):
    """
    Extrae todos los textos del 'bot' de una conversación completa.
    
    Args:
        conversation_text (str): String con formato de lista de diccionarios
        
    Returns:
        str: Texto concatenado de todas las respuestas del bot
    """
    if not conversation_text or conversation_text.strip() == "":
        return ""
    try:
        text = conversation_text.replace(' || ', '|').replace(' | ', '|').replace('||', '|')
        text = text.replace('\n', ' ').replace('\t', ' ')
        messages = [msg.strip() for msg in text.split('|') if msg.strip()]
        bot_texts = []
        bot_prefixes = ['bot:', 'assistant:', 'asistente:', 'b:']
        for msg in messages:
            msg_norm = msg.lstrip().lower()
            for prefix in bot_prefixes:
                if msg_norm.startswith(prefix):
                    idx = msg.lower().find(prefix)
                    clean_text = msg[idx+len(prefix):].strip()
                    if clean_text:
                        bot_texts.append(clean_text)
                    break
        result = ' '.join(bot_texts)
        while '  ' in result:
            result = result.replace('  ', ' ')
        return result.strip()
    except Exception as e:
        print(f"⚠️ ERROR extrayendo textos de bot: {str(e)[:100]}")
        return ""

def calculate_tokens_with_tiktoken(text):
    """
    Calcula tokens usando tiktoken (si disponible) o aproximación matemática robusta
    """
    if not text or text == "":
        return 0
    try:
        # Si el texto es una lista, sumar tokens individuales
        if isinstance(text, list):
            total_tokens = 0
            for t in text:
                total_tokens += calculate_tokens_with_tiktoken(t)
            return total_tokens
        if TIKTOKEN_AVAILABLE:
            encoding = tiktoken.get_encoding("cl100k_base")
            tokens = len(encoding.encode(str(text)))
            print(f"🎯 TIKTOKEN: Calculados {tokens} tokens para texto de {len(text)} caracteres")
            return tokens
        else:
            text_str = str(text)
            char_count = len(text_str)
            base_tokens = char_count / 3.8
            space_ratio = text_str.count(' ') / max(char_count, 1)
            space_adjustment = space_ratio * 0.2
            punct_count = sum(1 for c in text_str if c in '.,;:!?¡¿()[]{}"\'-')
            punct_ratio = punct_count / max(char_count, 1)
            punct_adjustment = punct_ratio * 0.15
            special_count = sum(1 for c in text_str if c.isdigit() or c in '@#$%&*+=/<>')
            special_ratio = special_count / max(char_count, 1)
            special_adjustment = special_ratio * 0.1
            estimated_tokens = int(base_tokens * (1 + space_adjustment + punct_adjustment + special_adjustment))
            final_tokens = max(1, estimated_tokens)
            print(f"📊 MATH_APPROX: Calculados {final_tokens} tokens para texto de {char_count} caracteres")
            print(f"   📈 Base: {base_tokens:.1f}, Espacios: +{space_adjustment:.3f}, Puntuación: +{punct_adjustment:.3f}, Especiales: +{special_adjustment:.3f}")
            return final_tokens
    except Exception as e:
        print(f"❌ TOKEN_CALC: Error calculando tokens - {str(e)}")
        fallback_tokens = max(1, len(str(text)) // 4)
        print(f"🆘 FALLBACK: Usando {fallback_tokens} tokens (1 token/4 chars)")
        return fallback_tokens

def diagnose_tiktoken():
    """
    Función de diagnóstico para verificar el estado de tiktoken
    """
    print("\n🔍 DIAGNÓSTICO DE TIKTOKEN:")
    print(f"   📦 Tiktoken disponible: {TIKTOKEN_AVAILABLE}")
    
    if TIKTOKEN_AVAILABLE:
        try:
            encoding = get_tiktoken_encoding()
            if encoding:
                # Prueba básica
                test_text = "Hola mundo"
                tokens = encoding.encode(test_text)
                print(f"   ✅ Prueba exitosa: '{test_text}' = {len(tokens)} tokens")
                return True
            else:
                print("   ❌ No se pudo obtener encoding")
                return False
        except Exception as e:
            print(f"   ❌ Error en prueba: {str(e)}")
            return False
    else:
        print("   🔄 Usando aproximación matemática como fallback")
        return False

# UDFs para PySpark - Estas funcionan con o sin tiktoken
def extract_user_texts_list(conversation_text):
    """
    Devuelve lista de textos de usuario detectados
    """
    import re
    if not conversation_text or conversation_text.strip() == "":
        print("[DEBUG] Texto de conversación vacío o nulo.")
        return []
    try:
        text = conversation_text.replace(' || ', '|').replace(' | ', '|').replace('||', '|')
        text = text.replace('\n', ' ').replace('\t', ' ')
        messages = [msg.strip() for msg in text.split('|') if msg.strip()]
        user_texts = []
        user_regex = re.compile(r'^[\s\u200b\ufeff]*(user:|usuario:|usr:|u:)', re.IGNORECASE)
        for i, msg in enumerate(messages):
            match = user_regex.match(msg)
            if match:
                clean_text = msg[match.end():].strip()
                if clean_text:
                    user_texts.append(clean_text)
                print(f"[DEBUG] Fila {i}: Detectado user -> '{clean_text}'")
            else:
                print(f"[DEBUG] Fila {i}: No es user -> '{msg[:50]}...'")
        print(f"[DEBUG] Textos de usuario extraídos: {user_texts}")
        return user_texts
    except Exception as e:
        print(f"⚠️ ERROR extrayendo lista de textos de usuario: {str(e)[:100]}")
        return []

def extract_bot_texts_list(conversation_text):
    """
    Devuelve lista de textos de bot detectados
    """
    if not conversation_text or conversation_text.strip() == "":
        return []
    try:
        text = conversation_text.replace(' || ', '|').replace(' | ', '|').replace('||', '|')
        text = text.replace('\n', ' ').replace('\t', ' ')
        messages = [msg.strip() for msg in text.split('|') if msg.strip()]
        bot_texts = []
        bot_prefixes = ['bot:', 'assistant:', 'asistente:', 'b:']
        for msg in messages:
            msg_norm = msg.lstrip().lower()
            for prefix in bot_prefixes:
                if msg_norm.startswith(prefix):
                    idx = msg.lower().find(prefix)
                    clean_text = msg[idx+len(prefix):].strip()
                    if clean_text:
                        bot_texts.append(clean_text)
                    break
        return bot_texts
    except Exception as e:
        print(f"⚠️ ERROR extrayendo lista de textos de bot: {str(e)[:100]}")
        return []

calculate_user_tokens_udf = udf(
    lambda conversation_text: calculate_tokens_with_tiktoken(extract_user_texts_list(conversation_text)),
    IntegerType()
)

calculate_bot_tokens_udf = udf(
    lambda conversation_text: calculate_tokens_with_tiktoken(extract_bot_texts_list(conversation_text)),
    IntegerType()
)

# ===================================================================
# FUNCIÓN PRINCIPAL Y PROCESAMIENTO
# ===================================================================

def main():
    """
    Función principal del job de Glue
    """
    print(f"🚀 INICIANDO ETL-2 PROCESS - PYSPARK + TYPE CONVERSION + TOKEN CALCULATION")
    print(f"   ⚡ Motor: PySpark distribuido con conversión de tipos")
    print(f"   � Nueva funcionalidad: Cálculo de tokens con tiktoken")
    print(f"   �📥 Input: s3://{INPUT_BUCKET}/{INPUT_PREFIX}")
    print(f"   📤 Output: s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}")
    print(f"   🎯 Objetivo: Conversión CSV → Parquet + análisis de tokens")
    print(f"   🔧 Solución: Problema de 'todo como string' resuelto + tokens precisos")
    
    # Diagnóstico de tiktoken
    tiktoken_working = diagnose_tiktoken()
    
    try:
        # 1. Leer CSV más reciente de ETL-1
        input_path = f"s3://{INPUT_BUCKET}/{INPUT_PREFIX}"
        print(f"📖 Leyendo CSV desde: {input_path}")
        
        # Leer usando Spark para mejor rendimiento y soporte de multiline/quoted fields
        df_spark = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("delimiter", ",") \
            .csv(input_path)
        
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
        'token_pregunta': 'integer',      # 🆕 NUEVA COLUMNA - Tokens preguntas usuario
        'token_respuesta': 'integer',     # 🆕 NUEVA COLUMNA - Tokens respuestas bot
        
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
                if column == 'feedback':
                    # Limitar feedback solo a like, dislike, mixed (normalizando)
                    df_processed = df_processed.withColumn(
                        column,
                        when(
                            lower(trim(col(column))).isin(['like', 'dislike', 'mixed']),
                            lower(trim(col(column)))
                        ).otherwise(None)
                    )
                elif column == 'respuesta_feedback':
                    # Solo mostrar respuesta_feedback si feedback es válido, si no dejar en blanco
                    df_processed = df_processed.withColumn(
                        column,
                        when(
                            lower(trim(col('feedback'))).isin(['like', 'dislike', 'mixed']),
                            col(column).cast("string")
                        ).otherwise(None)
                    )
                elif conversion_type == 'date':
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
    
    # 🔥 AGREGAR NUEVAS COLUMNAS DE TOKENS CON TIKTOKEN
    print(f"\n🔥 AGREGANDO COLUMNAS DE TOKENS...")
    print(f"   📚 Usando tiktoken (cl100k_base) si está disponible")
    print(f"   🔄 Fallback a aproximación matemática si es necesario")
    print(f"   🎯 Calculando token_pregunta (textos del user)")
    print(f"   🎯 Calculando token_respuesta (textos del bot)")
    
    # Verificar que existe la columna conversacion_completa
    if 'conversacion_completa' in df_processed.columns:
        print(f"   ✅ Columna 'conversacion_completa' encontrada - procesando...")
        
        try:
            # Agregar columna token_pregunta (tokens de preguntas del usuario)
            print(f"   🔄 Creando columna token_pregunta...")
            df_processed = df_processed.withColumn(
                "token_pregunta",
                calculate_user_tokens_udf(col("conversacion_completa"))
            )
            
            # Agregar columna token_respuesta (tokens de respuestas del bot)
            print(f"   🔄 Creando columna token_respuesta...")
            df_processed = df_processed.withColumn(
                "token_respuesta", 
                calculate_bot_tokens_udf(col("conversacion_completa"))
            )
            
            print(f"   ✅ Columnas de tokens agregadas exitosamente")
            print(f"   📊 token_pregunta: Cuenta tokens de todas las preguntas del user")
            print(f"   📊 token_respuesta: Cuenta tokens de todas las respuestas del bot")
            
        except Exception as e:
            print(f"   ❌ Error creando columnas de tokens: {str(e)}")
            print(f"   🔄 Agregando columnas nulas como fallback...")
            # Agregar columnas con valores nulos como fallback
            df_processed = df_processed.withColumn("token_pregunta", lit(None).cast(IntegerType()))
            df_processed = df_processed.withColumn("token_respuesta", lit(None).cast(IntegerType()))
        
    else:
        print(f"   ⚠️  Columna 'conversacion_completa' no encontrada - saltando cálculo de tokens")
        # Agregar columnas con valores nulos si no existe la fuente
        df_processed = df_processed.withColumn("token_pregunta", lit(None).cast(IntegerType()))
        df_processed = df_processed.withColumn("token_respuesta", lit(None).cast(IntegerType()))

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
    
    print(f"\n🎉 Procesamiento PySpark + Conversión de Tipos + Tokens completado")
    print(f"   ✅ Problema de 'todo como string' resuelto")
    print(f"   ✅ Tipos apropiados aplicados para analytics")
    print(f"   🔥 Columnas de tokens agregadas con tiktoken (precisión GPT)")
    print(f"   📊 token_pregunta: Tokens de preguntas del usuario")
    print(f"   📊 token_respuesta: Tokens de respuestas del bot")
    
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
