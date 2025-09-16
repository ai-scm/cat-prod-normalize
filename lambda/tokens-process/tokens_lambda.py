#!/usr/bin/env python3
"""
AWS Lambda para extraer tokens de DynamoDB y generar CSV con análisis de costos.
Basado en la lógica de Athena:
- Input tokens (token_pregunta): contenido de user + system + instruction + used_chunks
- Output tokens (token_respuesta): contenido de assistant/bot
- Cálculo: LENGTH(texto) / 4 (aproximadamente 4 caracteres por token)
"""

import json
import boto3
import pandas as pd
from datetime import datetime, date
from typing import Tuple, Dict, Any, List
import io
import os
from decimal import Decimal

# Configuración AWS
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cat-prod-normalize-reports')
S3_OUTPUT_PREFIX = os.environ.get('S3_OUTPUT_PREFIX', 'tokens-analysis/')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'cat_prod_analytics_db') 
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'wg-cat-prod-analytics')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', f's3://{S3_BUCKET_NAME}/athena/results/')

# Rango de fechas: desde 4 de agosto hasta el día actual (dinámico)
FILTER_DATE_START = datetime(2025, 8, 4, 0, 0, 0)
FILTER_DATE_END = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
FILTER_TIMESTAMP_START = int(FILTER_DATE_START.timestamp() * 1000)  # Convertir a milisegundos
FILTER_TIMESTAMP_END = int(FILTER_DATE_END.timestamp() * 1000)  # Convertir a milisegundos

# Inicializar clientes AWS
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

def lambda_handler(event, context):
    """
    Función Lambda principal para procesar DynamoDB y generar análisis de tokens
    """
    try:
        print("=== INICIANDO EXTRACCIÓN DE TOKENS ===")
        print(f"Filtro de fecha: desde {FILTER_DATE_START.strftime('%Y-%m-%d %H:%M:%S')} hasta {FILTER_DATE_END.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Extraer datos de DynamoDB
        print("Extrayendo datos de DynamoDB...")
        raw_data = extraer_datos_dynamodb()
        print(f"Registros extraídos: {len(raw_data)}")
        
        if not raw_data:
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No se encontraron datos en DynamoDB',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # 2. Procesar datos y extraer tokens
        print("Procesando tokens...")
        results = procesar_tokens_dynamodb(raw_data)
        print(f"Registros procesados: {len(results['data'])}")
        print(f"Registros filtrados: {results['filtered_count']}")
        
        # 3. Generar CSV y subir a S3
        print("Generando CSV...")
        s3_url = generar_y_subir_csv(results)
        
        # 4. Generar estadísticas finales
        stats = calcular_estadisticas_finales(results['data'])
        
        # 5. Actualizar vista en Athena
        print("Actualizando vista en Athena...")
        query_id = actualizar_vista_athena()
        print(f"Query ejecutada en Athena ID: {query_id}")
        
        print("=== PROCESAMIENTO COMPLETADO ===")
        print(f"Total input tokens: {stats['total_input_tokens']:,}")
        print(f"Total output tokens: {stats['total_output_tokens']:,}")
        print(f"Costo total input tokens: ${stats['total_input_tokens'] * 0.003 / 1000:.6f} USD")
        print(f"Costo total output tokens: ${stats['total_output_tokens'] * 0.015 / 1000:.6f} USD")
        print(f"Costo total: ${stats['total_cost']:.6f} USD")
        print(f"Archivo S3: {s3_url}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extracción de tokens completada exitosamente',
                'statistics': stats,
                'filtered_count': results['filtered_count'],
                'processed_count': results['processed_count'],
                'error_count': results['error_count'],
                'total_cost_usd': stats['total_cost'],
                'input_cost_usd': stats['total_input_cost'],
                'output_cost_usd': stats['total_output_cost'],
                's3_file': s3_url,
                'athena_query_id': query_id,
                'timestamp': datetime.now().isoformat()
            }, default=str)
        }
        
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }

def extraer_datos_dynamodb() -> List[Dict]:
    """
    Extrae todos los datos relevantes de DynamoDB
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Usar scan para obtener todos los datos
        response = table.scan()
        items = response['Items']
        
        # Continuar escaneando si hay más datos
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        return items
        
    except Exception as e:
        print(f"Error extrayendo datos de DynamoDB: {str(e)}")
        raise e

def procesar_tokens_dynamodb(raw_data: List[Dict]) -> Dict:
    """
    Procesa los datos de DynamoDB y extrae tokens
    """
    results = []
    processed_count = 0
    filtered_count = 0
    error_count = 0
    
    for item_num, item in enumerate(raw_data, 1):
        try:
            # Obtener CreateTime y filtrar por fecha
            create_time = item.get('CreateTime')
            create_date_str = ""
            
            if create_time:
                try:
                    # DynamoDB puede devolver Decimal o string
                    if isinstance(create_time, Decimal):
                        create_timestamp = int(create_time)
                    elif isinstance(create_time, str):
                        create_timestamp = int(create_time)
                    else:
                        create_timestamp = int(create_time)
                    
                    # Convertir timestamp a fecha legible
                    create_date = datetime.fromtimestamp(create_timestamp / 1000)
                    create_date_str = create_date.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Filtrar: solo procesar si está en el rango de fechas (4 agosto - 11 septiembre 2025)
                    if create_timestamp < FILTER_TIMESTAMP_START or create_timestamp > FILTER_TIMESTAMP_END:
                        filtered_count += 1
                        continue
                        
                except (ValueError, TypeError):
                    create_date_str = "Fecha inválida"
            
            # Obtener MessageMap
            message_map = item.get('MessageMap')
            
            if not message_map:
                # Sin MessageMap válido, tokens en 0
                input_tokens = 0
                output_tokens = 0
                total_price = item.get('TotalPrice', 0.0)
            else:
                # Procesar MessageMap (puede ser dict o string)
                if isinstance(message_map, str):
                    # Si es string, parsearlo como JSON
                    json_data = clean_and_parse_json(message_map)
                    
                    # SOLUCIÓN DE EMERGENCIA: Para registros con error en char 99
                    # Si falló la deserialización y encontramos el error específico, intentar con una solución manual
                    if not json_data and 'ody": ",' in message_map:
                        fixed_message = message_map.replace('ody": ",', 'ody": "",')
                        try:
                            json_data = json.loads(fixed_message)
                        except:
                            pass
                else:
                    # Si ya es dict (formato DynamoDB), usar la deserialización mejorada
                    json_data = deserializar_dynamodb_item(message_map)
                
                if not json_data:
                    input_tokens = 0
                    output_tokens = 0
                    total_price = 0.0
                else:
                    # Extraer tokens usando la función mejorada
                    input_tokens, output_tokens = extract_tokens_from_json(json_data)
                    
                    # Calcular precios (como en la query de Athena)
                    precio_input_usd = round(input_tokens * 0.003 / 1000, 6)
                    precio_output_usd = round(output_tokens * 0.015 / 1000, 6)
                    total_price = round(precio_input_usd + precio_output_usd, 6)
            
            # Agregar resultado
            results.append({
                'create_date': create_date_str,
                'input_token': input_tokens,
                'output_token': output_tokens,
                'precio_token_input': precio_input_usd,
                'precio_token_output': precio_output_usd,
                'total_price': total_price,
                'pk': item.get('PK', ''),
                'sk': item.get('SK', '')
            })
            
            processed_count += 1
            
            # Mostrar progreso cada 500 items para no saturar logs
            if processed_count % 500 == 0:
                print(f"Procesados {processed_count} items de {len(raw_data)}")
                
        except Exception:
            error_count += 1
            # Agregar item con tokens en 0 para errores
            results.append({
                'create_date': 'Error',
                'input_token': 0,
                'output_token': 0,
                'precio_token_input': 0.0,
                'precio_token_output': 0.0,
                'total_price': 0.0,
                'pk': item.get('PK', ''),
                'sk': item.get('SK', '')
            })
    
    return {
        'data': results,
        'processed_count': processed_count,
        'filtered_count': filtered_count,
        'error_count': error_count
    }

def clean_and_parse_json(json_string: str) -> dict:
    """
    Limpia y parsea un JSON que puede estar escapado incorrectamente
    Maneja específicamente el escape cuádruple del CSV: """"system"""" -> "system"
    """
    if not json_string or str(json_string).strip() == '':
        return {}
    
    try:
        # Limpiar el string
        cleaned = str(json_string).strip()
        
        # Remover comillas externas si las hay
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]
        
        # CORRECCIÓN CRÍTICA: Manejar escape cuádruple específico del CSV
        # El CSV tiene formato: """"system"""" que debe convertirse a "system"
        if '""""' in cleaned:
            cleaned = cleaned.replace('""""', '"')
        
        # También manejar escape doble normal por si acaso
        if '""' in cleaned:
            cleaned = cleaned.replace('""', '"')
            
        # SOLUCIÓN PARA EL ERROR EN CHAR 99: corregir null values
        # DynamoDB tiene formato: "media_type": null en lugar de "media_type": null (sin comillas)
        if '"null"' in cleaned or '": null,' in cleaned:
            # Reemplazar "null" sin escapar por null (valor JSON)
            cleaned = cleaned.replace('"null"', 'null')
            # Asegurar que los null están correctamente formateados
            cleaned = cleaned.replace('": null,', '": null,')
            
        # SOLUCIÓN ADICIONAL: formatear los boolean correctamente
        if '"true"' in cleaned or '"false"' in cleaned:
            cleaned = cleaned.replace('"true"', 'true')
            cleaned = cleaned.replace('"false"', 'false')
        
        # CORRECCIÓN ESPECÍFICA PARA EL ERROR EN CHAR 99:
        # Fix para el patrón: "body": ", "file_name" (falta valor entre comillas)
        if '"body": ",' in cleaned:
            cleaned = cleaned.replace('"body": ",', '"body": "",')
            
        # CORRECCIÓN ADICIONAL: buscar cualquier instancia de ody": ", (errores de body truncados)
        if 'ody": ",' in cleaned:
            cleaned = cleaned.replace('ody": ",', 'ody": "",')
            
        # Intentar cargar JSON directamente
        result = json.loads(cleaned)
        return result
        
    except json.JSONDecodeError as e:
        try:
            # Intento de reparación más agresivo
            # Reemplazar valores problemáticos comunes
            fixed_json = cleaned.replace('": "null",', '": null,')
            fixed_json = fixed_json.replace('": "true",', '": true,')
            fixed_json = fixed_json.replace('": "false",', '": false,')
            
            # CORRECCIÓN ESPECÍFICA PARA CHAR 99: Error común en "body": ", "file_name"
            if 'ody": ",' in fixed_json:
                fixed_json = fixed_json.replace('ody": ",', 'ody": "",')
            
            # Buscar el primer { y el último }
            start = fixed_json.find('{')
            end = fixed_json.rfind('}')
            
            if start >= 0 and end > start:
                cleaned_json = fixed_json[start:end+1]
                result = json.loads(cleaned_json)
                return result
            else:
                # Última alternativa: intentar crear un JSON básico con la información disponible
                try:
                    # Si todo falla, crear un objeto básico con los primeros campos
                    simple_json = '{}'
                    result = json.loads(simple_json)
                    return result
                except:
                    return {}
        except json.JSONDecodeError:
            return {}

def deserializar_dynamodb_item(item: Any) -> Dict:
    """
    Deserializa un item de DynamoDB de formato nativo a Python dict
    """
    if isinstance(item, dict):
        result = {}
        for key, value in item.items():
            result[key] = deserializar_valor_dynamodb(value)
        return result
    elif isinstance(item, list):
        return [deserializar_dynamodb_item(i) for i in item]
    else:
        return item

def deserializar_valor_dynamodb(valor: Any) -> Any:
    """
    Convierte un valor de formato DynamoDB a formato normal de forma recursiva
    """
    if valor is None:
        return None
    
    # Si es un diccionario con formato DynamoDB
    if isinstance(valor, dict):
        if 'S' in valor:  # String
            return valor['S']
        elif 'N' in valor:  # Number
            try:
                return float(valor['N']) if '.' in valor['N'] else int(valor['N'])
            except:
                return valor['N']
        elif 'BOOL' in valor:  # Boolean
            return valor['BOOL']
        elif 'NULL' in valor:  # Null
            return None
        elif 'L' in valor:  # List
            return [deserializar_valor_dynamodb(item) for item in valor['L']]
        elif 'M' in valor:  # Map
            return {k: deserializar_valor_dynamodb(v) for k, v in valor['M'].items()}
        else:
            # Si no tiene formato DynamoDB, procesar como dict normal
            return {k: deserializar_valor_dynamodb(v) for k, v in valor.items()}
    
    # Si no es diccionario, devolver tal como está
    return valor

def calculate_tokens(text: str) -> int:
    """
    Calcula tokens usando la fórmula de Athena: LENGTH(texto) / 4
    """
    if not text or not isinstance(text, str):
        return 0
    
    return max(1, len(text) // 4)  # Mínimo 1 token

def extract_tokens_from_json(data: Dict) -> Tuple[int, int]:
    """
    Extrae tokens de entrada y salida del JSON de conversación
    Lógica basada en Athena: LENGTH(texto) / 4
    """
    input_tokens = 0
    output_tokens = 0
    
    if not data:
        return 1, 1  # Mínimo 1 token para evitar errores
    
    if not isinstance(data, dict):
        if isinstance(data, str) and data:
            # Si es string, contar como input tokens
            return calculate_tokens(data), 0
        return 1, 1  # Mínimo 1 token para evitar errores
    
    try:
        # Primer intento: formato DynamoDB común (system, instruction, user, assistant)
        if any(key in data for key in ['system', 'instruction', 'user', 'assistant']):
            
            # Buscar contenido de mensajes
            for key, value in data.items():
                # Procesar según el formato esperado de DynamoDB
                if isinstance(value, dict):
                    role = value.get('role', key)
                    
                    # Manejar lista de contenidos
                    content_list = value.get('content', [])
                    if isinstance(content_list, list):
                        for content_item in content_list:
                            if isinstance(content_item, dict):
                                # Formato {content_type, media_type, body}
                                body = content_item.get('body', '')
                                if body and isinstance(body, str):
                                    token_count = calculate_tokens(body)
                                    
                                    # Clasificar según el rol
                                    if role in ['user', 'system', 'instruction', 'used_chunks']:
                                        input_tokens += token_count
                                    elif role in ['assistant', 'bot']:
                                        output_tokens += token_count
                    
                    # También manejar contenido directo como string
                    elif isinstance(value.get('content'), str):
                        content = value.get('content')
                        token_count = calculate_tokens(content)
                        
                        if role in ['user', 'system', 'instruction', 'used_chunks']:
                            input_tokens += token_count
                        elif role in ['assistant', 'bot']:
                            output_tokens += token_count
        
        # Segundo intento: formato genérico
        if input_tokens == 0 and output_tokens == 0:
            
            # Buscar contenido de mensajes en formato genérico
            for key, value in data.items():
                if isinstance(value, dict) and 'content' in value:
                    content = value.get('content', '')
                    role = value.get('role', key)
                    
                    # Procesar contenido como string
                    if isinstance(content, str) and content:
                        token_count = calculate_tokens(content)
                        
                        # Clasificar según el rol
                        if role in ['user', 'system', 'instruction', 'used_chunks']:
                            input_tokens += token_count
                        elif role in ['assistant', 'bot']:
                            output_tokens += token_count
                    
                    # Procesar contenido como lista
                    elif isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict) and 'body' in item:
                                body = item.get('body', '')
                                if isinstance(body, str) and body:
                                    token_count = calculate_tokens(body)
                                    
                                    if role in ['user', 'system', 'instruction', 'used_chunks']:
                                        input_tokens += token_count
                                    elif role in ['assistant', 'bot']:
                                        output_tokens += token_count
                
                # Manejar listas de mensajes
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            # Buscar content o body
                            content = item.get('content', item.get('body', ''))
                            role = item.get('role', '')
                            
                            if isinstance(content, str) and content:
                                token_count = calculate_tokens(content)
                                
                                if role in ['user', 'system', 'instruction', 'used_chunks']:
                                    input_tokens += token_count
                                elif role in ['assistant', 'bot']:
                                    output_tokens += token_count
        
        # Si no se encontró ningún token, usar valores mínimos
        if input_tokens == 0 and output_tokens == 0:
            return 1, 1
            
        return input_tokens, output_tokens
        
    except Exception:
        return 1, 1  # Mínimo 1 token para evitar errores

def generar_y_subir_csv(results: Dict) -> str:
    """
    Genera CSV y lo sube a S3 con nombre fijo para sobrescritura diaria
    """
    try:
        # Garantizar que siempre haya datos para el CSV
        # Esto evita el error "No hay datos para generar CSV"
        if not results.get('data') or len(results['data']) == 0:
            # Crear registro mínimo para evitar error
            empty_record = {
                'create_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'input_token': 0,
                'output_token': 0,
                'precio_token_input': 0.0,
                'precio_token_output': 0.0,
                'total_price': 0.0,
                'pk': 'sin_datos',
                'sk': 'sin_datos'
            }
            results['data'] = [empty_record]
            
        # Crear DataFrame
        df = pd.DataFrame(results['data'])
        
        # Usar nombre fijo para sobrescribir cada día
        filename = "tokens_analysis_latest.csv"
        
        # Convertir DataFrame a CSV en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        
        # Subir a S3
        s3_key = f"{S3_OUTPUT_PREFIX}{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        print(f"Archivo CSV subido a: {s3_url}")
        
        # Opcional: También guardar una copia con fecha como archivo histórico
        # Descomentar si se requiere un registro histórico
        # guardar_copia_historica(csv_content)
        
        return s3_url
        
    except Exception as e:
        print(f"Error generando/subiendo CSV: {str(e)}")
        raise e

def calcular_estadisticas_finales(data: List[Dict]) -> Dict:
    """
    Calcula estadísticas finales del procesamiento
    """
    total_input_tokens = sum(
        r['input_token'] for r in data 
        if isinstance(r['input_token'], (int, float))
    )
    total_output_tokens = sum(
        r['output_token'] for r in data 
        if isinstance(r['output_token'], (int, float))
    )
    total_cost = sum(
        r['total_price'] for r in data 
        if isinstance(r['total_price'], (int, float))
    )
    
    # Calcular costos totales de input y output
    total_input_cost = round(total_input_tokens * 0.003 / 1000, 6)
    total_output_cost = round(total_output_tokens * 0.015 / 1000, 6)
    
    return {
        'total_records': len(data),
        'total_input_tokens': total_input_tokens,
        'total_output_tokens': total_output_tokens,
        'total_tokens': total_input_tokens + total_output_tokens,
        'total_input_cost': total_input_cost,
        'total_output_cost': total_output_cost,
        'total_cost': round(total_cost, 6),
        'average_cost_per_record': round(total_cost / len(data), 6) if data else 0,
        'average_input_tokens': round(total_input_tokens / len(data), 2) if data else 0,
        'average_output_tokens': round(total_output_tokens / len(data), 2) if data else 0
    }

def generar_manifest_file(s3_url: str) -> str:
    """
    Genera archivo manifest para compatibilidad con otros procesos
    """
    try:
        manifest_data = {
            "entries": [
                {"url": s3_url, "mandatory": True}
            ]
        }
        
        manifest_filename = "tokens_analysis_manifest_latest.json"
        manifest_key = f"{S3_OUTPUT_PREFIX}manifests/{manifest_filename}"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType='application/json'
        )
        
        manifest_url = f"s3://{S3_BUCKET_NAME}/{manifest_key}"
        print(f"Manifest generado: {manifest_url}")
        
        return manifest_url
        
    except Exception as e:
        print(f"Error generando manifest: {str(e)}")
        return ""

def actualizar_vista_athena() -> str:
    """
    Ejecuta una consulta en Athena para actualizar la vista token_usage_analysis
    """
    try:
        # SQL para crear o reemplazar la vista
        query = """
        CREATE OR REPLACE VIEW token_usage_analysis AS
        SELECT
            create_date,
            input_token AS "token pregunta",
            output_token AS "token respuesta",
            input_token + output_token AS "total tokens",
            precio_token_input AS "precio total pregunta",
            precio_token_output AS "precio total respuesta",
            total_price AS "precio total"
        FROM tokens_table
        WHERE input_token > 0 OR output_token > 0
        ORDER BY "total tokens" DESC;
        """
        
        # Ejecutar la consulta en Athena
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': ATHENA_DATABASE
            },
            ResultConfiguration={
                'OutputLocation': ATHENA_OUTPUT_LOCATION
            },
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Vista Athena actualizada con ID de ejecución: {query_execution_id}")
        return query_execution_id
        
    except Exception as e:
        print(f"Error actualizando vista en Athena: {str(e)}")
        return "error"

