import json
import pandas as pd
import boto3
import ast
from datetime import date, datetime
import io
import os
from collections import Counter
import re
import traceback

# Columnas finales requeridas (orden exacto para el CSV)
COLUMNAS_FINALES_12 = [
    'usuario_id', 'nombre', 'gerencia', 'ciudad', 'fecha_primera_conversacion',
    'numero_conversaciones', 'conversacion_completa', 'feedback_total',
    'numero_feedback', 'pregunta_conversacion', 'feedback', 'respuesta_feedback'
]

def lambda_handler(event, context):
    """
    Función Lambda para procesar datos de DynamoDB y generar archivo CSV normalizado
    """
    try:
        # Inicializar resultado
        result = {
            'statusCode': 200,
            'body': {
                'message': 'Proceso iniciado',
                'usuarios_procesados': 0,
                'archivo_generado': ''
            }
        }
        
        print("🚀 INICIANDO PROCESO DE NORMALIZACIÓN DE DATOS CATIA")
        
        # PASO 1: Extraer datos de DynamoDB
        print("📊 EXTRAYENDO DATOS DE DYNAMODB")
        df = extraer_datos_dynamodb()
        print(f"   • Total filas extraídas: {len(df)}")
        
        # PASO 2: Deserializar datos de DynamoDB
        df = deserializar_datos_dynamodb(df)
        
        # PASO 3: Procesar y normalizar datos
        print("🔗 PROCESANDO MERGE DE CONVERSACIONES Y FEEDBACK")
        df = procesar_merge_conversaciones_feedback(df)
        print(f"   • Después del merge: {len(df)} filas")
        
        # PASO 4: Aplicar filtros
        print("🔧 APLICANDO FILTROS")
        df = aplicar_filtros(df)
        print(f"   • Después de filtros: {len(df)} filas")
        
        # PASO 5: Extraer preguntas
        print("💬 EXTRAYENDO PREGUNTAS DE CONVERSACIONES")
        try:
            # VERSIÓN MEJORADA: Implementa la función del notebook que maneja mejor múltiples preguntas
            # separadas por ' | ' y tiene mejor manejo de errores para JSON complejos
            df = extraer_preguntas_conversaciones(df)
            print(f"   • Preguntas extraídas exitosamente")
        except Exception as e:
            print(f"   ❌ ERROR en extracción de preguntas: {str(e)}")
            raise
        
        # PASO 6: Crear dataset con 12 columnas
        print("📋 CREANDO DATASET CON 12 COLUMNAS")
        try:
            df_12_columnas = crear_dataset_12_columnas(df)
            print(f"   • Dataset 12 columnas creado: {len(df_12_columnas)} filas")
        except Exception as e:
            print(f"   ❌ ERROR en crear dataset 12 columnas: {str(e)}")
            raise
        
        # PASO 7: Agrupar usuarios únicos
        print("🔄 AGRUPANDO USUARIOS ÚNICOS")
        try:
            df_usuarios_unicos = agrupar_usuarios_unicos(df_12_columnas)
            print(f"   • Usuarios únicos agrupados: {len(df_usuarios_unicos)} usuarios")
        except Exception as e:
            print(f"   ❌ ERROR en agrupar usuarios únicos: {str(e)}")
            raise
        
        # PASO 8: Clasificar feedback
        print("🎯 CLASIFICANDO FEEDBACK")
        df_usuarios_unicos = clasificar_feedback(df_usuarios_unicos)

        # PASO 9: Extraer respuestas de feedback
        print("💬 EXTRAYENDO RESPUESTAS DE FEEDBACK")
        df_usuarios_unicos = extraer_respuestas_feedback(df_usuarios_unicos)

        # PASO 10: Validar / ordenar columnas finales
        print("🧪 VALIDANDO COLUMNAS FINALES")
        df_usuarios_unicos = validar_y_ordenar_columnas_finales(df_usuarios_unicos)

        # PASO 11: Generar archivo CSV
        print("💾 GENERANDO ARCHIVO CSV")
        archivo_s3_csv = generar_archivo_csv(df_usuarios_unicos)

        # PASO 12: Generar Manifest File para QuickSight (CSV)
        print("📄 GENERANDO MANIFEST FILE PARA QUICKSIGHT (CSV)")
        manifest_url = generar_manifest_file([archivo_s3_csv])

        # Actualizar resultado (convertir int64 a int para JSON serialization)
        result['body'] = {
            'message': 'Proceso completado exitosamente',
            'usuarios_procesados': int(len(df_usuarios_unicos)),
            'archivo_generado': archivo_s3_csv,
            'manifest_file': manifest_url,
            'estadisticas': {
                'total_conversaciones': int(df_usuarios_unicos['numero_conversaciones'].sum()),
                'usuarios_con_feedback': int((df_usuarios_unicos['feedback'] != '').sum()),
                'usuarios_con_preguntas': int((df_usuarios_unicos['pregunta_conversacion'] != '').sum())
            }
        }

        print(f"✅ PROCESO COMPLETADO - {len(df_usuarios_unicos)} usuarios procesados")
        return result
        
    except Exception as e:
        print(f"❌ ERROR en lambda_handler: {str(e)}")
        print(f"❌ TRACEBACK COMPLETO: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Error durante el procesamiento'
            }
        }

def extraer_datos_dynamodb():
    """Extrae datos de DynamoDB"""
    try:
        # Configurar sesión de DynamoDB
        session = boto3.Session()
        dynamodb = session.resource('dynamodb', region_name='us-east-1')
        table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'cat-prod-catia-conversations-table')
        
        # Obtener referencia a la tabla
        table = dynamodb.Table(table_name)
        
        # Escanear tabla completa con paginación
        response = table.scan()
        items = response['Items']
        
        # Continuar escaneando si hay más items
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        # Convertir a DataFrame
        df = pd.DataFrame(items)
        
        # Filtrar filas que no sean REGISTER
        if 'SK' in df.columns:
            df = df[~df['SK'].str.contains('REGISTER', case=False, na=False)].reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"❌ ERROR en extraer_datos_dynamodb: {str(e)}")
        raise

def deserializar_datos_dynamodb(df):
    """
    Convierte datos de formato DynamoDB JSON a formato normal
    DynamoDB devuelve: {'S': 'valor'} -> 'valor'
    IMPORTANTE: NO deserializa 'Conversation' para que formatear_conversacion_especial 
    pueda manejar el formato DynamoDB original
    """
    try:
        print("   🔄 Deserializando datos de DynamoDB...")
        
        # Columnas que deserializamos (EXCLUIMOS 'Conversation')
        columnas_a_deserializar = ['UserData', 'Feedback', 'CreatedAt']
        
        for columna in columnas_a_deserializar:
            if columna in df.columns:
                df[columna] = df[columna].apply(deserializar_valor_dynamodb)
        
        print("   ✅ Datos deserializados exitosamente (Conversation mantenida en formato DynamoDB)")
        return df
        
    except Exception as e:
        print(f"   ❌ ERROR deserializando datos: {str(e)}")
        return df

def deserializar_valor_dynamodb(valor):
    """
    Convierte un valor de formato DynamoDB a formato normal de forma recursiva
    Maneja todos los tipos de DynamoDB: S, N, BOOL, NULL, L, M, etc.
    """
    # Manejar arrays/listas de pandas de forma segura
    if hasattr(valor, '__len__') and not isinstance(valor, (str, bytes, dict)):
        try:
            if len(valor) == 0:
                return None
            # Si es un array/Series, tomar el primer elemento
            valor = valor[0] if len(valor) > 0 else None
        except:
            pass
    
    # Verificar nulos de forma segura
    try:
        if pd.isna(valor) or valor is None:
            return valor
    except (ValueError, TypeError):
        if valor is None:
            return valor
    
    # Si es un diccionario con formato DynamoDB
    if isinstance(valor, dict):
        # Tipos simples
        if 'S' in valor:  # String
            return valor['S']
        elif 'N' in valor:  # Number
            try:
                # Intentar convertir a int primero, luego float
                if '.' in str(valor['N']):
                    return float(valor['N'])
                else:
                    return int(valor['N'])
            except ValueError:
                return str(valor['N'])
        elif 'BOOL' in valor:  # Boolean
            return valor['BOOL']
        elif 'NULL' in valor:  # Null
            return None
        
        # Tipos complejos - RECURSIVOS
        elif 'L' in valor:  # List/Array
            return [deserializar_valor_dynamodb(item) for item in valor['L']]
        elif 'M' in valor:  # Map/Object
            return {key: deserializar_valor_dynamodb(val) for key, val in valor['M'].items()}
        
        # Tipos adicionales
        elif 'SS' in valor:  # String Set
            return list(valor['SS'])
        elif 'NS' in valor:  # Number Set
            return [float(n) if '.' in n else int(n) for n in valor['NS']]
        elif 'BS' in valor:  # Binary Set
            return list(valor['BS'])
        
        else:
            # Si tiene otras claves, intentar convertir todo
            return str(valor)
    
    # Si no es diccionario, devolver tal como está
    return valor

def procesar_merge_conversaciones_feedback(df):
    """Procesa el merge de conversaciones y feedback"""
    try:
        # Separar tipos de filas
        conversation_rows = df[df['SK'].str.contains('CONVERSATION', case=False, na=False)].copy()
        feedback_rows = df[df['SK'].str.contains('FEEDBACK', case=False, na=False)].copy()
        other_rows = df[~df['SK'].str.contains('CONVERSATION|FEEDBACK', case=False, na=False)].copy()
        
        # Crear mapping de feedback
        feedback_mapping = {}
        for _, row in feedback_rows.iterrows():
            pk = row['PK']
            feedback_value = row['Feedback']
            feedback_mapping[pk] = feedback_value
        
        # Merge feedback en conversaciones
        merged_rows = []
        for _, conv_row in conversation_rows.iterrows():
            pk = conv_row['PK']
            merged_row = conv_row.copy()
            if pk in feedback_mapping:
                merged_row['Feedback'] = feedback_mapping[pk]
            merged_rows.append(merged_row)
        
        # Combinar todo
        merged_df = pd.DataFrame(merged_rows)
        final_df = pd.concat([merged_df, other_rows], ignore_index=True)
        
        # Crear usuario_id
        final_df['usuario_id'] = final_df['PK'].str.replace('USER#', '', regex=False)
        cols = ['usuario_id'] + [col for col in final_df.columns if col != 'usuario_id']
        final_df = final_df[cols]
        
        return final_df
        
    except Exception as e:
        print(f"❌ ERROR en procesar_merge_conversaciones_feedback: {str(e)}")
        raise

def aplicar_filtros(df):
    """Aplica filtros permisivos al dataset"""
    try:
        print(f"   📊 Dataset inicial: {len(df)} filas")
        
        # Procesar UserData
        user_data_parsed = df['UserData'].apply(parse_user_data_clean)
        df_user_data = pd.DataFrame(user_data_parsed.tolist())
        
        df['nombre'] = df_user_data['nombre'].fillna('')
        df['gerencia'] = df_user_data['ciudad'].fillna('')
        
        # Verificar nombres extraídos
        nombres_extraidos = (df['nombre'] != '').sum()
        print(f"   👤 Nombres extraídos del UserData: {nombres_extraidos}/{len(df)}")
        
        # Renombrar columnas
        df = df.rename(columns={
            'CreatedAt': 'fecha_primera_conversacion',
            'Conversation': 'conversacion_completa'
        })
        
        # FORMATEAR CONVERSACIÓN EN FORMATO ESPECIAL
        print(f"   💬 Aplicando formato especial a conversaciones...")
        df['conversacion_completa'] = df['conversacion_completa'].apply(formatear_conversacion_especial)
        conversaciones_formateadas = (df['conversacion_completa'] != '').sum()
        print(f"   • Conversaciones formateadas exitosamente: {conversaciones_formateadas}/{len(df)}")
        
        # Rellenar nombres vacíos SOLO si realmente están vacíos
        nombres_vacios = (df['nombre'] == '') | (df['nombre'].isna()) | (df['nombre'] == 'nan')
        print(f"   🔧 Nombres vacíos a rellenar: {nombres_vacios.sum()}")
        df.loc[nombres_vacios, 'nombre'] = 'Usuario Anónimo'
        
        # Filtro de ciudad PERMISIVO
        print(f"   🌍 Aplicando filtro de ciudades...")
        patron_excluir = r'(?i)(mexico|medell|cali|barranquilla|cartagena|potosí|valle|antioquia)'
        df = df[~df['gerencia'].str.contains(patron_excluir, regex=True, na=False)].copy()
        
        # Rellenar gerencias vacías SOLO si realmente están vacías
        gerencias_vacias = (df['gerencia'] == '') | (df['gerencia'].isna())
        df.loc[gerencias_vacias, 'gerencia'] = 'Bogotá'
        
        print(f"   📊 Después de filtro ciudades: {len(df)} filas")
        
        # Filtro de fechas PERMISIVO
        print(f"   📅 Aplicando filtro de fechas...")
        fecha_inicio = date(2025, 8, 4)
        fecha_fin = date.today()  # Usar fecha actual en lugar de fecha fija
        
        df['fecha_temp'] = pd.to_datetime(df['fecha_primera_conversacion'], errors='coerce')
        
        # Incluir fechas del rango Y fechas nulas
        # Arreglar el problema de arrays ambiguos
        mask_fechas_validas = df['fecha_temp'].notna()
        mask_en_rango = pd.Series([False] * len(df), index=df.index)
        
        # Solo aplicar filtro de fechas a las fechas válidas
        if mask_fechas_validas.any():
            fechas_validas_idx = df[mask_fechas_validas].index
            fechas_como_date = df.loc[fechas_validas_idx, 'fecha_temp'].dt.date
            mask_en_rango.loc[fechas_validas_idx] = (
                (fechas_como_date >= fecha_inicio) & (fechas_como_date <= fecha_fin)
            )
        
        # Combinar: fechas en rango O fechas nulas
        mask_fechas_final = mask_en_rango | df['fecha_temp'].isna()
        
        df = df[mask_fechas_final].copy()
        df['fecha_primera_conversacion'] = df['fecha_temp'].dt.strftime('%d/%m/%Y')
        df.loc[df['fecha_temp'].isna(), 'fecha_primera_conversacion'] = 'Sin fecha'
        df.drop(columns=['fecha_temp'], inplace=True)
        
        print(f"   📊 Dataset final después de filtros: {len(df)} filas")
        return df
        
    except Exception as e:
        print(f"❌ ERROR en aplicar_filtros: {str(e)}")
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise

def parse_user_data_clean(value):
    """Parsear UserData de forma segura"""
    if pd.isna(value) or value is None:
        return {'nombre': '', 'ciudad': ''}
    
    # Si ya es un diccionario Python (como en el Lambda), acceder directamente
    if isinstance(value, dict):
        try:
            nombre = str(value.get('nombre', '')).strip()
            ciudad = str(value.get('ciudad', value.get('gerencia', ''))).strip()
            
            # Limpiar ciudad - remover texto entre paréntesis
            if '(' in ciudad and ')' in ciudad:
                ciudad = ciudad.split('(')[0].strip()
            
            return {
                'nombre': nombre,
                'ciudad': ciudad
            }
        except Exception as e:
            return {'nombre': '', 'ciudad': ''}
    
    # Si es string, parsearlo como antes
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ['nan', 'none', 'null']:
            return {'nombre': '', 'ciudad': ''}
        try:
            # Intentar JSON
            result = json.loads(value)
            if isinstance(result, dict):
                nombre = result.get('nombre', '').strip()
                ciudad = result.get('ciudad', result.get('gerencia', '')).strip()
                
                # Limpiar ciudad - remover texto entre paréntesis
                if '(' in ciudad and ')' in ciudad:
                    ciudad = ciudad.split('(')[0].strip()
                
                parsed_result = {
                    'nombre': nombre,
                    'ciudad': ciudad
                }
                
                return parsed_result
        except:
            try:
                # Intentar literal_eval
                result = ast.literal_eval(value)
                if isinstance(result, dict):
                    nombre = result.get('nombre', '').strip()
                    ciudad = result.get('ciudad', result.get('gerencia', '')).strip()
                    
                    # Limpiar ciudad - remover texto entre paréntesis
                    if '(' in ciudad and ')' in ciudad:
                        ciudad = ciudad.split('(')[0].strip()
                    
                    parsed_result = {
                        'nombre': nombre,
                        'ciudad': ciudad
                    }
                    
                    return parsed_result
            except:
                pass
    return {'nombre': '', 'ciudad': ''}

def limpiar_conversacion_texto(texto):
    """
    Limpia y normaliza el texto de conversación para mejorar el parsing
    """
    if not texto or pd.isna(texto):
        return ''
    
    texto = str(texto).strip()
    
    # Eliminar caracteres problemáticos
    texto = texto.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Normalizar espacios múltiples
    while '  ' in texto:
        texto = texto.replace('  ', ' ')
    
    # Normalizar separadores de pipe
    texto = texto.replace('|', ' | ')  # Asegurar espacios alrededor de pipes
    while ' |  | ' in texto:  # Corregir espacios dobles después de la normalización
        texto = texto.replace(' |  | ', ' | ')
    
    return texto.strip()

def formatear_conversacion_especial(conversacion_data):
    """
    Convierte datos de conversación a formato especial: user: pregunta | bot: respuesta | user: pregunta
    NUEVO: Implementación desde cero para manejar correctamente el formato DynamoDB anidado
    
    Args:
        conversacion_data: Lista de objetos DynamoDB en formato:
                          [{"M": {"from": {"S": "user"}, "text": {"S": "Buenos días"}}}, ...]
        
    Returns:
        str: Conversación formateada como "user: texto | bot: texto | ..."
    """
    try:
        if not conversacion_data:
            return ''
        
        # Debug mejorado: Ver el tipo de datos que recibimos
        debug_type = type(conversacion_data).__name__
        debug_content = str(conversacion_data)[:200] + "..." if len(str(conversacion_data)) > 200 else str(conversacion_data)
        
        # Log de debug para las primeras 5 conversaciones para diagnóstico
        import random
        if random.random() < 0.05:  # 5% de las veces para ver más ejemplos
            print(f"   🔍 DEBUG formatear_conversacion_especial:")
            print(f"      Tipo: {debug_type}")
            print(f"      Contenido: {debug_content}")
        
        mensajes_formateados = []
        
        # CASO 1: Lista de objetos DynamoDB con estructura {"M": {...}}
        if isinstance(conversacion_data, list):
            for elemento in conversacion_data:
                # Verificar si es un objeto DynamoDB con clave 'M'
                if isinstance(elemento, dict) and 'M' in elemento:
                    # Extraer el objeto interno
                    mensaje_obj = elemento['M']
                    
                    # Extraer 'from' y 'text' manejando formato DynamoDB
                    from_val = mensaje_obj.get('from', {})
                    text_val = mensaje_obj.get('text', {})
                    
                    # Obtener valores reales desde formato DynamoDB
                    if isinstance(from_val, dict) and 'S' in from_val:
                        from_key = from_val['S'].lower().strip()
                    else:
                        from_key = str(from_val).lower().strip()
                    
                    if isinstance(text_val, dict) and 'S' in text_val:
                        text_content = text_val['S'].strip()
                    else:
                        text_content = str(text_val).strip()
                    
                    # Limpiar el texto
                    texto_limpio = text_content.replace('\n\n', ' ').replace('\n', ' ').strip()
                    
                    # Limitar longitud para evitar strings excesivos
                    if len(texto_limpio) > 300:
                        texto_limpio = texto_limpio[:300] + "..."
                    
                    # Formatear según el tipo de remitente
                    if from_key in ['user', 'usuario']:
                        mensajes_formateados.append(f"user: {texto_limpio}")
                    elif from_key in ['bot', 'assistant', 'catia']:
                        mensajes_formateados.append(f"bot: {texto_limpio}")
                    else:
                        mensajes_formateados.append(f"{from_key}: {texto_limpio}")
                
                # CASO 2: Lista de objetos normales (ya deserializados)
                elif isinstance(elemento, dict) and ('from' in elemento or 'text' in elemento):
                    from_key = str(elemento.get('from', '')).lower().strip()
                    text_content = str(elemento.get('text', '')).strip()
                    
                    # Limpiar texto
                    texto_limpio = text_content.replace('\n\n', ' ').replace('\n', ' ').strip()
                    if len(texto_limpio) > 300:
                        texto_limpio = texto_limpio[:300] + "..."
                    
                    # Formatear
                    if from_key in ['user', 'usuario']:
                        mensajes_formateados.append(f"user: {texto_limpio}")
                    elif from_key in ['bot', 'assistant', 'catia']:
                        mensajes_formateados.append(f"bot: {texto_limpio}")
                    else:
                        mensajes_formateados.append(f"{from_key}: {texto_limpio}")
            
            # Retornar mensajes unidos
            resultado = ' | '.join(mensajes_formateados)
            return resultado if resultado else ''
        
        # CASO 3: String que necesita ser parseado
        elif isinstance(conversacion_data, str):
            conversacion_str = conversacion_data.strip()
            if not conversacion_str or conversacion_str in ['', 'nan', 'None', 'null']:
                return ''
            
            # Si parece ser JSON, intentar parsearlo
            try:
                if conversacion_str.startswith('[') and conversacion_str.endswith(']'):
                    parsed_data = json.loads(conversacion_str)
                    return formatear_conversacion_especial(parsed_data)  # Recursión
                elif conversacion_str.startswith('{') and conversacion_str.endswith('}'):
                    parsed_data = [json.loads(conversacion_str)]
                    return formatear_conversacion_especial(parsed_data)  # Recursión
                else:
                    # Si no es JSON, retornar como está
                    return conversacion_str
            except json.JSONDecodeError:
                # Si falla JSON, intentar ast.literal_eval
                try:
                    parsed_data = ast.literal_eval(conversacion_str)
                    return formatear_conversacion_especial(parsed_data)  # Recursión
                except:
                    return conversacion_str
        
        # CASO 4: Diccionario DynamoDB individual con clave 'L' (Lista)
        elif isinstance(conversacion_data, dict) and 'L' in conversacion_data:
            # Este es el formato DynamoDB para listas: {"L": [{"M": {...}}, ...]}
            lista_mensajes = conversacion_data['L']
            return formatear_conversacion_especial(lista_mensajes)  # Recursión
        
        # CASO 5: Objeto único - convertir a lista y procesar
        elif isinstance(conversacion_data, dict):
            return formatear_conversacion_especial([conversacion_data])  # Recursión
        
        # CASO 6: Otros tipos - convertir a string
        else:
            return str(conversacion_data)
        
    except Exception as e:
        print(f"   ❌ ERROR en formatear_conversacion_especial: {str(e)}")
        print(f"      Tipo de dato recibido: {type(conversacion_data)}")
        print(f"      Contenido (primeros 100 chars): {str(conversacion_data)[:100]}...")
        return str(conversacion_data) if conversacion_data else ''

# NUEVO (alineado con prueba_local.py): helpers para extracción de preguntas

def extraer_preguntas_implicitas_de_respuesta_bot(respuesta_bot):
    """
    Intenta inferir preguntas a partir de respuestas del bot usando patrones
    (Implementación alineada con prueba_local.py)
    """
    preguntas_inferidas = []
    
    try:
        if not respuesta_bot or len(respuesta_bot.strip()) < 10:
            return preguntas_inferidas
        
        texto_bot_lower = respuesta_bot.lower()
        
        patrones_inferencia = [
            (r'el trámite de ([\w\s]+?) (?:es|se realiza|consiste)', 
             lambda x: f"¿Cómo hago el trámite de {x.strip()}?"),
            (r'el certificado ([\w\s]+?) (?:es|incluye|contiene)', 
             lambda x: f"¿Qué es el certificado {x.strip()}?"),
            (r'el chip', 
             lambda x: "¿Qué es el CHIP?"),
            (r'(?:cuesta|valor|costo)', 
             lambda x: "¿Cuál es el costo del trámite?"),
            (r'(?:documentos? necesarios?|requisitos?)', 
             lambda x: "¿Qué documentos necesito?"),
            (r'(?:tiempo de respuesta|duración|días hábiles)', 
             lambda x: "¿Cuánto tiempo tarda el trámite?"),
            (r'(?:ubicad|direcci[oó]n|sede)', 
             lambda x: "¿Dónde queda ubicado catastro?"),
            (r'horario.*atenci[oó]n', 
             lambda x: "¿Cuál es el horario de atención?"),
        ]
        
        for patron, constructor_pregunta in patrones_inferencia:
            if re.search(patron, texto_bot_lower):
                try:
                    pregunta_inferida = constructor_pregunta("")
                    pregunta_inferida = pregunta_inferida.strip()
                    if pregunta_inferida and pregunta_inferida not in preguntas_inferidas:
                        preguntas_inferidas.append(pregunta_inferida)
                        break
                except Exception as e:
                    continue
        
        if not preguntas_inferidas:
            palabras_clave = re.findall(r'\b(?:catastro|certificado|trámite|avalúo|chip|desenglobe|englobe)\b', texto_bot_lower)
            if palabras_clave:
                primera_palabra = palabras_clave[0]
                pregunta_generica = f"Consulta sobre {primera_palabra}"
                preguntas_inferidas.append(pregunta_generica)
        
    except Exception as e:
        pass
    
    return preguntas_inferidas


def extraer_preguntas_de_dialogo_individual(dialogo_str):
    """
    Extrae preguntas de un diálogo individual en formato "user: pregunta | bot: respuesta"
    (Implementación alineada con prueba_local.py)
    """
    preguntas = []
    
    try:
        segmentos = dialogo_str.split(' | ')
        
        for segmento in segmentos:
            segmento = segmento.strip()
            if not segmento:
                continue
            if segmento.lower().startswith('user:'):
                pregunta = segmento[5:].strip()
                if pregunta and pregunta not in preguntas:
                    preguntas.append(pregunta)
    except Exception as e:
        print(f"   ❌ Error extrayendo preguntas de diálogo: {e}")
    
    return preguntas


def extraer_preguntas_usuario(conversacion_formateada):
    """
    Extrae todas las preguntas del usuario desde una conversación ya formateada
    (Implementación alineada con prueba_local.py)
    
    Args:
        conversacion_formateada (str): Conversación en formato "user: texto | bot: texto | ..."
        
    Returns:
        str: Preguntas del usuario separadas por ' | '
    """
    try:
        if conversacion_formateada is None:
            return ''
        
        if hasattr(conversacion_formateada, '__len__') and not isinstance(conversacion_formateada, (str, bytes)):
            try:
                if hasattr(conversacion_formateada, 'tolist'):
                    conversacion_formateada = conversacion_formateada.tolist()
                elif hasattr(conversacion_formateada, 'iloc'):
                    conversacion_formateada = conversacion_formateada.iloc[0] if len(conversacion_formateada) > 0 else ''
                else:
                    conversacion_formateada = conversacion_formateada[0] if len(conversacion_formateada) > 0 else ''
            except (IndexError, ValueError, TypeError):
                return ''
        
        try:
            if pd.isna(conversacion_formateada):
                return ''
        except (ValueError, TypeError):
            pass
        
        conversacion_str = str(conversacion_formateada).strip()
        if not conversacion_str or conversacion_str in ['', 'nan', 'None', 'null']:
            return ''
        
        preguntas_usuario = []
        
        # ESTRATEGIA 1: Múltiples diálogos separados por " || "
        if ' || ' in conversacion_str:
            dialogos = conversacion_str.split(' || ')
            for dialogo in dialogos:
                preguntas_dialogo = extraer_preguntas_de_dialogo_individual(dialogo.strip())
                preguntas_usuario.extend(preguntas_dialogo)
        
        # ESTRATEGIA 2: Formato con pipes simples
        elif ' | ' in conversacion_str:
            preguntas_dialogo = extraer_preguntas_de_dialogo_individual(conversacion_str)
            preguntas_usuario.extend(preguntas_dialogo)
        
        # ESTRATEGIA 3: Segmento único comenzando con user:
        elif conversacion_str.lower().startswith('user:'):
            pregunta = conversacion_str[5:].strip()
            if pregunta:
                preguntas_usuario.append(pregunta)
        
        # ESTRATEGIA 4: Inferir desde bot:
        elif conversacion_str.lower().startswith('bot:'):
            preguntas_implicitas = extraer_preguntas_implicitas_de_respuesta_bot(conversacion_str)
            if preguntas_implicitas:
                preguntas_usuario.append(preguntas_implicitas[0])
        
        else:
            patrones_user = re.findall(r'user:\s*([^|]+?)(?:\s*\|\s*bot:|$)', conversacion_str, re.IGNORECASE)
            for pregunta in patrones_user:
                pregunta = pregunta.strip()
                if pregunta and pregunta not in preguntas_usuario:
                    preguntas_usuario.append(pregunta)
        
        preguntas_unicas = []
        for pregunta in preguntas_usuario:
            if pregunta and pregunta not in preguntas_unicas:
                preguntas_unicas.append(pregunta)
        
        return ' | '.join(preguntas_unicas) if preguntas_unicas else ''
        
    except Exception as e:
        print(f"   ❌ Error en extraer_preguntas_usuario: {e}")
        return ''

def extraer_preguntas_conversaciones(df):
    """Extrae preguntas de usuario desde conversacion_completa usando la función simplificada del notebook"""
    try:
        print(f"   🔍 Iniciando extracción de preguntas para {len(df)} conversaciones")
        
        # Contar conversaciones antes del procesamiento
        total_conversaciones = len(df)
        conversaciones_con_datos = df['conversacion_completa'].notna().sum()
        
        print(f"   📊 Estado inicial:")
        print(f"      • Total conversaciones: {total_conversaciones}")
        print(f"      • Con datos de conversación: {conversaciones_con_datos}")
        
        # Mostrar algunos ejemplos de formato de conversación para debugging
        print(f"   📝 EJEMPLOS DE FORMATO DE CONVERSACIÓN:")
        ejemplos = df[df['conversacion_completa'].notna()]['conversacion_completa'].head(3)
        for i, ejemplo in enumerate(ejemplos, 1):
            ejemplo_str = str(ejemplo)[:200] + "..." if len(str(ejemplo)) > 200 else str(ejemplo)
            print(f"      Ejemplo {i}: {ejemplo_str}")
        
        # Procesar y extraer preguntas usando la función del notebook
        df['pregunta_conversacion'] = df['conversacion_completa'].apply(extraer_preguntas_usuario)
        
        # Estadísticas del procesamiento
        preguntas_extraidas = (df['pregunta_conversacion'] != '').sum()
        conversaciones_con_preguntas = df[df['pregunta_conversacion'] != '']['pregunta_conversacion'].str.contains('\|', na=False).sum()
        
        print(f"   ✅ EXTRACCIÓN COMPLETADA:")
        print(f"      • Conversaciones con preguntas extraídas: {preguntas_extraidas}")
        print(f"      • Conversaciones con múltiples preguntas: {conversaciones_con_preguntas}")
        
        # Mostrar ejemplos de preguntas extraídas
        if preguntas_extraidas > 0:
            print(f"   📝 EJEMPLOS DE PREGUNTAS EXTRAÍDAS:")
            ejemplos_preguntas = df[df['pregunta_conversacion'] != '']['pregunta_conversacion'].head(3)
            for i, pregunta in enumerate(ejemplos_preguntas, 1):
                pregunta_display = pregunta[:100] + "..." if len(pregunta) > 100 else pregunta
                print(f"      {i}. {pregunta_display}")
        else:
            print(f"   ⚠️  NO SE EXTRAJERON PREGUNTAS - Verificar formato de datos")
            # Intentar un análisis más detallado si no se extrajeron preguntas
            print(f"   🔍 ANÁLISIS DETALLADO DE FORMATOS:")
            ejemplos_detalle = df[df['conversacion_completa'].notna()]['conversacion_completa'].head(5)
            for i, ejemplo in enumerate(ejemplos_detalle, 1):
                ejemplo_str = str(ejemplo)
                print(f"      Conversación {i}:")
                print(f"         Tipo: {type(ejemplo)}")
                print(f"         Longitud: {len(ejemplo_str)}")
                print(f"         Contiene pipe: {' | ' in ejemplo_str}")
                print(f"         Es array JSON: {ejemplo_str.startswith('[') and ejemplo_str.endswith(']')}")
                print(f"         Es objeto JSON: {ejemplo_str.startswith('{') and ejemplo_str.endswith('}')}")
                print(f"         Contenido (primeros 150 chars): {ejemplo_str[:150]}...")
                print("")
        
        return df
    except Exception as e:
        print(f"❌ ERROR en extraer_preguntas_conversaciones: {str(e)}")
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise

def crear_dataset_12_columnas(df):
    """Crea DataFrame con las 12 columnas exactas"""
    try:
        # Procesar feedback
        feedback_data = []
        for feedback in df['Feedback']:
            feedback_data.append(extract_feedback_clean(feedback))
        
        feedback_df = pd.DataFrame(feedback_data)
        
        # Agregar columnas de feedback
        for col in feedback_df.columns:
            if col not in df.columns:
                df[col] = feedback_df[col]
        
        # Contar conversaciones de forma segura
        def contar_conversaciones_seguro(x):
            """Cuenta conversaciones de forma segura evitando array ambiguity"""
            try:
                # Si es NaN, None o vacío, retornar 1
                if x is None or x == '' or str(x) == 'nan':
                    return 1
                
                # Para arrays o listas, usar solo el primer elemento o convertir a string
                if isinstance(x, (list, tuple)):
                    if len(x) == 0:
                        return 1
                    x_str = str(x)
                else:
                    x_str = str(x)
                
                # Contar ocurrencias de bot y user
                bot_count = x_str.count('bot')
                user_count = x_str.count('user')
                
                # Retornar el máximo o 1 si ambos son 0
                return max(bot_count, user_count, 1)
                
            except Exception as e:
                print(f"❌ Error contando conversaciones: {e}")
                return 1
        
        df['numero_conversaciones'] = df['conversacion_completa'].apply(contar_conversaciones_seguro)
        
        # Crear DataFrame con las 12 columnas exactas
        df_12_columnas = pd.DataFrame({
            'usuario_id': df['usuario_id'],
            'nombre': df['nombre'],
            'gerencia': df['gerencia'],
            'ciudad': df['gerencia'],  # ciudad = gerencia
            'fecha_primera_conversacion': df['fecha_primera_conversacion'],
            'numero_conversaciones': df['numero_conversaciones'],
            'conversacion_completa': df['conversacion_completa'],
            'feedback_total': df['feedback_total'] if 'feedback_total' in df.columns else '',
            'numero_feedback': '',  # Se calculará después
            'pregunta_conversacion': df['pregunta_conversacion'],
            'feedback': df['tipo'] if 'tipo' in df.columns else '',
            'respuesta_feedback': df['comentario'] if 'comentario' in df.columns else ''
        })
        
        # Aplicar conteo de feedback
        df_12_columnas['numero_feedback'] = df_12_columnas['feedback_total'].apply(contar_feedback_total)
        
        return df_12_columnas
        
    except Exception as e:
        print(f"❌ ERROR en crear_dataset_12_columnas: {str(e)}")
        raise

def extract_feedback_clean(feedback_str):
    """Extraer información de feedback de forma simple"""
    if pd.isna(feedback_str) or feedback_str == '' or feedback_str is None:
        return {'feedback_total': '', 'tipo': '', 'comentario': ''}
    
    try:
        feedback_str = str(feedback_str).strip()
        if feedback_str.startswith('{') and feedback_str.endswith('}'):
            data = json.loads(feedback_str)
            
            result = {'feedback_total': feedback_str, 'tipo': '', 'comentario': ''}
            for key, value in data.items():
                if isinstance(value, dict) and 'S' in value:
                    result[key] = value['S']
                else:
                    result[key] = value
            return result
        else:
            return {'feedback_total': feedback_str, 'tipo': '', 'comentario': ''}
    except:
        return {'feedback_total': str(feedback_str), 'tipo': '', 'comentario': ''}

def contar_feedback_total(feedback_text):
    """Cuenta la cantidad de likes y dislikes en el texto de feedback"""
    if pd.isna(feedback_text) or feedback_text == '' or feedback_text is None:
        return 0
    
    try:
        feedback_str = str(feedback_text).strip()
        
        if not feedback_str or feedback_str.lower() in ['nan', 'none', 'null']:
            return 0
        
        feedback_lower = feedback_str.lower()
        
        contador_likes = feedback_lower.count("'type': 'like'") + feedback_lower.count('"type": "like"')
        contador_dislikes = feedback_lower.count("'type': 'dislike'") + feedback_lower.count('"type": "dislike"')
        
        total = contador_likes + contador_dislikes
        
        if '|' in feedback_str and total == 0:
            partes = feedback_str.split('|')
            for parte in partes:
                parte_clean = parte.strip().lower()
                if "'type': 'like'" in parte_clean or '"type": "like"' in parte_clean:
                    total += 1
                elif "'type': 'dislike'" in parte_clean or '"type": "dislike"' in parte_clean:
                    total += 1
        
        if total == 0 and len(feedback_str) > 10:
            total = 1
            
        return total
        
    except:
        return 1 if len(str(feedback_text).strip()) > 5 else 0

def agrupar_usuarios_unicos(df_12_columnas):
    """Agrupa por usuarios únicos con información completa"""
    try:
        print(f"   🔧 Iniciando agrupamiento...")
        def safe_first_non_default(series, default_value):
            """Obtiene el primer valor que no sea el valor por defecto"""
            try:
                non_default = series[series != default_value]
                if len(non_default) > 0:
                    return non_default.iloc[0]
                else:
                    return series.iloc[0] if len(series) > 0 else default_value
            except:
                return series.iloc[0] if len(series) > 0 else default_value
        
        def safe_first_non_empty(series, default_value):
            """Obtiene el primer valor que no esté vacío, MANTENIENDO Usuario Anónimo si es necesario"""
            try:
                # Para nombres: Si ya tenemos "Usuario Anónimo", mantenerlo
                # Solo buscar nombres reales si existen, sino mantener "Usuario Anónimo"
                if default_value == 'Usuario Anónimo':
                    # Primero intentar encontrar nombres reales (no vacíos y no "Usuario Anónimo")
                    nombres_reales = series[
                        (series != '') & 
                        (series.notna()) & 
                        (series != 'nan') &
                        (series != 'None') &
                        (series != default_value)
                    ]
                    
                    if len(nombres_reales) > 0:
                        # Si hay nombres reales, usar el primero
                        return nombres_reales.iloc[0]
                    else:
                        # Si no hay nombres reales, mantener "Usuario Anónimo"
                        return default_value
                else:
                    # Para otros campos, solo filtrar valores no vacíos
                    valid_values = series[
                        (series != '') & 
                        (series.notna()) & 
                        (series != 'nan') &
                        (series != 'None')
                    ]
                    
                    if len(valid_values) > 0:
                        return valid_values.iloc[0]
                    else:
                        return default_value
                        
            except Exception as e:
                print(f"   ❌ ERROR en safe_first_non_empty: {str(e)}")
                return default_value
        
        def safe_join_non_empty(series):
            """Une valores no vacíos de forma segura usando separador doble para conversaciones"""
            try:
                non_empty = [str(val) for val in series if str(val) not in ['', 'nan', 'None', 'None']]
                return ' || '.join(non_empty) if non_empty else ''
            except:
                return ''
        
        aggregation_config = {
            'nombre': lambda x: safe_first_non_empty(x, 'Usuario Anónimo'),
            'gerencia': lambda x: safe_first_non_empty(x, 'Bogotá (no especificada)'),
            'ciudad': lambda x: safe_first_non_empty(x, 'Bogotá (no especificada)'),
            'fecha_primera_conversacion': 'first',
            'numero_conversaciones': 'sum',
            'conversacion_completa': safe_join_non_empty,
            'feedback_total': safe_join_non_empty,
            'numero_feedback': 'sum',
            'pregunta_conversacion': safe_join_non_empty,
            'feedback': safe_join_non_empty,
            'respuesta_feedback': safe_join_non_empty
        }
        
        df_usuarios_unicos = df_12_columnas.groupby('usuario_id').agg(aggregation_config).reset_index()
        
        # Verificar algunos nombres
        nombres_reales = (df_usuarios_unicos['nombre'] != 'Usuario Anónimo').sum()
        print(f"   📊 Nombres reales encontrados: {nombres_reales}/{len(df_usuarios_unicos)}")
        
        return df_usuarios_unicos
        
    except Exception as e:
        print(f"❌ ERROR en agrupar_usuarios_unicos: {str(e)}")
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        raise

def clasificar_feedback(df_usuarios_unicos):
    """Clasifica feedback en like, dislike, mixed o vacío"""
    try:
        df_usuarios_unicos['feedback'] = df_usuarios_unicos['feedback_total'].apply(clasificar_feedback_simplificado)
        return df_usuarios_unicos
    except Exception as e:
        print(f"❌ ERROR en clasificar_feedback: {str(e)}")
        raise

def clasificar_feedback_simplificado(feedback_total):
    """Clasifica el feedback en 'like', 'dislike', 'mixed' o ''"""
    if pd.isna(feedback_total) or feedback_total == '' or feedback_total is None:
        return ''
    
    try:
        feedback_str = str(feedback_total).strip()
        
        if not feedback_str or feedback_str.lower() in ['nan', 'none', 'null']:
            return ''
        
        tiene_like = False
        tiene_dislike = False
        
        # Buscar tipos con regex
        tipos_encontrados = re.findall(r"'type':\s*'([^']*)'", feedback_str)
        tipos_encontrados.extend(re.findall(r'"type":\s*"([^"]*)"', feedback_str))
        
        for tipo in tipos_encontrados:
            tipo_limpio = str(tipo).lower().strip()
            if tipo_limpio == 'like':
                tiene_like = True
            elif tipo_limpio == 'dislike':
                tiene_dislike = True
        
        # Si no encontramos tipos, intentar parsing JSON
        if not tiene_like and not tiene_dislike:
            try:
                if feedback_str.startswith('[') and feedback_str.endswith(']'):
                    feedback_data = json.loads(feedback_str)
                    if isinstance(feedback_data, list):
                        for item in feedback_data:
                            if isinstance(item, dict) and 'type' in item:
                                tipo_limpio = str(item['type']).lower().strip()
                                if tipo_limpio == 'like':
                                    tiene_like = True
                                elif tipo_limpio == 'dislike':
                                    tiene_dislike = True
                
                elif feedback_str.startswith('{') and feedback_str.endswith('}'):
                    feedback_data = json.loads(feedback_str)
                    if isinstance(feedback_data, dict) and 'type' in feedback_data:
                        tipo_limpio = str(feedback_data['type']).lower().strip()
                        if tipo_limpio == 'like':
                            tiene_like = True
                        elif tipo_limpio == 'dislike':
                            tiene_dislike = True
                            
            except (json.JSONDecodeError, ValueError):
                try:
                    feedback_data = ast.literal_eval(feedback_str)
                    
                    if isinstance(feedback_data, list):
                        for item in feedback_data:
                            if isinstance(item, dict) and 'type' in item:
                                tipo_limpio = str(item['type']).lower().strip()
                                if tipo_limpio == 'like':
                                    tiene_like = True
                                elif tipo_limpio == 'dislike':
                                    tiene_dislike = True
                    
                    elif isinstance(feedback_data, dict) and 'type' in feedback_data:
                        tipo_limpio = str(feedback_data['type']).lower().strip()
                        if tipo_limpio == 'like':
                            tiene_like = True
                        elif tipo_limpio == 'dislike':
                            tiene_dislike = True
                            
                except (ValueError, SyntaxError):
                    pass
        
        # Aplicar reglas de clasificación
        if tiene_like and tiene_dislike:
            return 'mixed'
        elif tiene_like:
            return 'like'
        elif tiene_dislike:
            return 'dislike'
        else:
            return ''
            
    except Exception:
        return ''

def extraer_respuestas_feedback(df_usuarios_unicos):
    """Extrae respuestas (comments y options) del feedback"""
    try:
        df_usuarios_unicos['respuesta_feedback'] = df_usuarios_unicos['feedback_total'].apply(extraer_respuesta_feedback)
        df_usuarios_unicos['respuesta_feedback'] = df_usuarios_unicos['respuesta_feedback'].apply(limpiar_respuesta_feedback)
        return df_usuarios_unicos
    except Exception as e:
        print(f"❌ ERROR en extraer_respuestas_feedback: {str(e)}")
        raise

def extraer_respuesta_feedback(feedback_total):
    """Extrae los campos 'comment' y 'option' del feedback_total"""
    if pd.isna(feedback_total) or feedback_total == '' or feedback_total is None:
        return ''
    
    try:
        feedback_str = str(feedback_total).strip()
        
        if not feedback_str or feedback_str.lower() in ['nan', 'none', 'null']:
            return ''
        
        respuestas = []
        
        # Buscar patterns con regex
        comments_pattern1 = re.findall(r"'comment':\s*'([^']*)'", feedback_str)
        comments_pattern2 = re.findall(r'"comment":\s*"([^"]*)"', feedback_str)
        
        options_pattern1 = re.findall(r"'option':\s*'([^']*)'", feedback_str)
        options_pattern2 = re.findall(r'"option":\s*"([^"]*)"', feedback_str)
        
        # Agregar comentarios encontrados
        for comment in comments_pattern1 + comments_pattern2:
            comment_clean = str(comment).strip()
            if comment_clean and comment_clean.lower() not in ['', 'none', 'null']:
                respuestas.append(comment_clean)
        
        # Agregar opciones encontradas
        for option in options_pattern1 + options_pattern2:
            option_clean = str(option).strip()
            if option_clean and option_clean.lower() not in ['', 'none', 'null']:
                respuestas.append(option_clean)
        
        # Si no encontramos nada con regex, intentar parsing JSON
        if not respuestas:
            try:
                partes = feedback_str.split('|') if '|' in feedback_str else [feedback_str]
                
                for parte in partes:
                    parte = parte.strip()
                    
                    if parte.startswith('{') and parte.endswith('}'):
                        try:
                            feedback_data = json.loads(parte)
                            if isinstance(feedback_data, dict):
                                if 'comment' in feedback_data:
                                    comment_val = str(feedback_data['comment']).strip()
                                    if comment_val and comment_val.lower() not in ['', 'none', 'null']:
                                        respuestas.append(comment_val)
                                
                                if 'option' in feedback_data:
                                    option_val = str(feedback_data['option']).strip()
                                    if option_val and option_val.lower() not in ['', 'none', 'null']:
                                        respuestas.append(option_val)
                        except json.JSONDecodeError:
                            try:
                                feedback_data = ast.literal_eval(parte)
                                if isinstance(feedback_data, dict):
                                    if 'comment' in feedback_data:
                                        comment_val = str(feedback_data['comment']).strip()
                                        if comment_val and comment_val.lower() not in ['', 'none', 'null']:
                                            respuestas.append(comment_val)
                                    
                                    if 'option' in feedback_data:
                                        option_val = str(feedback_data['option']).strip()
                                        if option_val and option_val.lower() not in ['', 'none', 'null']:
                                            respuestas.append(option_val)
                            except (ValueError, SyntaxError):
                                pass
                
            except Exception:
                pass
        
        if respuestas:
            return ' | '.join(respuestas)
        else:
            return ''
            
    except Exception:
        return ''

def limpiar_respuesta_feedback(respuesta_feedback):
    """Elimina duplicados de las respuestas manteniendo el orden"""
    if pd.isna(respuesta_feedback) or respuesta_feedback == '' or respuesta_feedback is None:
        return ''
    
    try:
        respuestas = str(respuesta_feedback).split(' | ')
        
        respuestas_unicas = []
        for respuesta in respuestas:
            respuesta_limpia = respuesta.strip()
            if respuesta_limpia and respuesta_limpia not in respuestas_unicas:
                respuestas_unicas.append(respuesta_limpia)
        
        return ' | '.join(respuestas_unicas) if respuestas_unicas else ''
        
    except Exception:
        return str(respuesta_feedback) if respuesta_feedback else ''

def validar_y_ordenar_columnas_finales(df):
    """Valida que existan las 12 columnas requeridas, crea faltantes vacías y ordena exactamente."""
    try:
        columnas_actuales = list(df.columns)
        faltantes = [c for c in COLUMNAS_FINALES_12 if c not in columnas_actuales]
        if faltantes:
            print(f"⚠️ Columnas faltantes agregadas vacías: {faltantes}")
            for c in faltantes:
                df[c] = ''
        # Reordenar estrictamente
        df = df[COLUMNAS_FINALES_12]
        # Verificación final
        if list(df.columns) == COLUMNAS_FINALES_12:
            print("✅ Columnas validadas y ordenadas correctamente")
        else:
            print("❌ No se logró ordenar correctamente las columnas")
        return df
    except Exception as e:
        print(f"❌ ERROR en validar_y_ordenar_columnas_finales: {str(e)}")
        raise

def generar_archivo_csv(df_usuarios_unicos):
    """Genera archivo CSV y lo sube a S3"""
    try:
        columnas_finales = [
            'usuario_id', 'nombre', 'gerencia', 'ciudad', 'fecha_primera_conversacion',
            'numero_conversaciones', 'conversacion_completa', 'feedback_total',
            'numero_feedback', 'pregunta_conversacion', 'feedback', 'respuesta_feedback'
        ]
        df_usuarios_unicos = df_usuarios_unicos[columnas_finales]

        nombre_archivo = "Dashboard_Usuarios_Catia_PROCESADO_COMPLETO.csv"

        csv_buffer = io.StringIO()
        # Usar utf-8-sig para que Excel (si se abre manualmente) detecte bien caracteres
        df_usuarios_unicos.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)

        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'cat-prod-normalize-reports')
        s3_key = f"reports/etl-process1/{nombre_archivo}"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv',
            ContentEncoding='utf-8'
        )

        s3_url = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ Archivo CSV subido a S3: {s3_url}")
        return s3_url
    except Exception as e:
        print(f"❌ ERROR en generar_archivo_csv: {str(e)}")
        raise

def generar_manifest_file(file_urls):
    """Genera un manifest file para QuickSight que apunta a archivos CSV"""
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'cat-prod-normalize-reports')
        csv_files = [url for url in file_urls if url.endswith('.csv')]
        print(f"🔍 Archivos CSV encontrados: {csv_files}")
        if not csv_files:
            raise ValueError("No se encontraron archivos CSV para el manifest")

        manifest_content = {
            "fileLocations": [
                {"URIs": csv_files}
            ],
            "globalUploadSettings": {
                "format": "CSV",
                "containsHeader": "true"
            }
        }

        try:
            manifest_json = json.dumps(manifest_content, indent=2, ensure_ascii=False)
            json.loads(manifest_json)
            print("✅ JSON validado correctamente")
        except json.JSONDecodeError as je:
            print(f"❌ Error al generar JSON válido: {je}")
            raise

        s3_client = boto3.client('s3')
        manifest_key = "manifest.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=manifest_key,
            Body=manifest_json.encode('utf-8'),
            ContentType='application/json',
            ContentEncoding='utf-8'
        )

        manifest_url = f"s3://{bucket_name}/{manifest_key}"
        print(f"✅ Manifest file (CSV) subido a S3: {manifest_url}")
        print(f"📄 Manifest content: {manifest_json}")
        return manifest_url
    except Exception as e:
        print(f"❌ ERROR en generar_manifest_file: {str(e)}")
        raise
