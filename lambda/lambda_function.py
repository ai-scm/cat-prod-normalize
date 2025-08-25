import json
import pandas as pd
import boto3
import ast
from datetime import date, datetime
import io
import os
from collections import Counter
import re

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
        import traceback
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
    """
    try:
        print("   🔄 Deserializando datos de DynamoDB...")
        
        # Columnas que típicamente vienen en formato DynamoDB
        columnas_a_deserializar = ['Conversation', 'UserData', 'Feedback', 'CreatedAt']
        
        for columna in columnas_a_deserializar:
            if columna in df.columns:
                df[columna] = df[columna].apply(deserializar_valor_dynamodb)
        
        print("   ✅ Datos deserializados exitosamente")
        return df
        
    except Exception as e:
        print(f"   ❌ ERROR deserializando datos: {str(e)}")
        return df

def deserializar_valor_dynamodb(valor):
    """
    Convierte un valor de formato DynamoDB a formato normal
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
        # Formato típico de DynamoDB: {'S': 'string_value'} o {'N': 'number_value'}
        if 'S' in valor:  # String
            return valor['S']
        elif 'N' in valor:  # Number
            return valor['N']
        elif 'BOOL' in valor:  # Boolean
            return valor['BOOL']
        elif 'NULL' in valor:  # Null
            return None
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
        import traceback
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

def extraer_preguntas_usuario(conversacion_json):
    """
    Extrae todas las preguntas del usuario desde el JSON de conversación
    Maneja tanto formato array JSON como formato pipe-separated objects
    
    Args:
        conversacion_json (str): JSON con la conversación completa o objetos separados por pipe
        
    Returns:
        str: Preguntas del usuario separadas por ' | '
    """
    try:
        # Manejo seguro de valores nulos/vacíos para arrays
        if conversacion_json is None:
            return ''
        
        # Si es un array/Series/numpy array, manejar de forma segura
        if hasattr(conversacion_json, '__len__') and not isinstance(conversacion_json, (str, bytes)):
            try:
                # Convertir a lista si es un array numpy/pandas para evitar errores de ambigüedad
                if hasattr(conversacion_json, 'tolist'):
                    conversacion_json = conversacion_json.tolist()
                elif hasattr(conversacion_json, 'iloc'):
                    # Si es una Serie de pandas, tomar el primer valor
                    conversacion_json = conversacion_json.iloc[0] if len(conversacion_json) > 0 else ''
                else:
                    # Si es una lista/tupla regular
                    conversacion_json = conversacion_json[0] if len(conversacion_json) > 0 else ''
            except (IndexError, ValueError, TypeError) as e:
                print(f"   ⚠️  Error manejando array/series: {e}")
                return ''
        
        # Verificar si es NaN usando numpy/pandas de forma segura
        try:
            if pd.isna(conversacion_json):
                return ''
        except (ValueError, TypeError):
            # Si pd.isna falla, continuar con otras validaciones
            pass
        
        # Convertir a string y verificar si está vacío
        conversacion_str = str(conversacion_json).strip()
        if not conversacion_str or conversacion_str in ['', 'nan', 'None', 'null']:
            return ''
        
        # Limpiar y normalizar el texto de la conversación
        conversacion_str = limpiar_conversacion_texto(conversacion_str)
        
        preguntas_usuario = []
        
        # CASO 1: Formato pipe-separated objects: {'from': 'user', 'text': '...'} | {'from': 'bot', 'text': '...'}
        # Detectar formato pipe-separated: contiene ' | ' pero NO es un array JSON
        if ' | ' in conversacion_str and not (conversacion_str.startswith('[') and conversacion_str.endswith(']')):
            try:
                print(f"   🔍 Procesando formato pipe-separated: {conversacion_str[:100]}...")
                
                # Dividir por pipe y procesar cada objeto
                objetos = conversacion_str.split(' | ')
                
                for i, objeto_str in enumerate(objetos):
                    objeto_str = objeto_str.strip()
                    if not objeto_str:
                        continue
                    
                    print(f"   📦 Procesando objeto {i+1}: {objeto_str[:80]}...")
                    
                    # Intentar múltiples estrategias de parsing
                    objeto_parseado = None
                    
                    # Estrategia 1: JSON estándar (reemplazar comillas simples por dobles)
                    try:
                        objeto_json_str = objeto_str.replace("'", '"')
                        objeto_parseado = json.loads(objeto_json_str)
                    except json.JSONDecodeError:
                        pass
                    
                    # Estrategia 2: ast.literal_eval (maneja comillas simples nativas)
                    if objeto_parseado is None:
                        try:
                            objeto_parseado = ast.literal_eval(objeto_str)
                        except (ValueError, SyntaxError):
                            pass
                    
                    # Estrategia 3: eval como último recurso (con precaución)
                    if objeto_parseado is None and objeto_str.startswith('{') and objeto_str.endswith('}'):
                        try:
                            # Solo usar eval si parece ser un dict simple y seguro
                            if all(char not in objeto_str for char in ['import', 'exec', 'eval', '__']):
                                objeto_parseado = eval(objeto_str)
                        except Exception:
                            pass
                    
                    # Procesar el objeto parseado
                    if isinstance(objeto_parseado, dict):
                        # Verificar diferentes variantes de claves
                        from_key = objeto_parseado.get('from') or objeto_parseado.get('From') 
                        text_key = objeto_parseado.get('text') or objeto_parseado.get('Text') or objeto_parseado.get('message')
                        
                        if from_key == 'user' and text_key:
                            texto_pregunta = str(text_key).strip()
                            if texto_pregunta and texto_pregunta not in preguntas_usuario:
                                preguntas_usuario.append(texto_pregunta)
                                print(f"   ✅ Pregunta extraída: {texto_pregunta[:60]}...")
                    else:
                        print(f"   ⚠️  No se pudo parsear objeto {i+1}: {objeto_str[:50]}...")
                
                resultado = ' | '.join(preguntas_usuario) if preguntas_usuario else ''
                print(f"   📝 Total preguntas extraídas del pipe: {len(preguntas_usuario)}")
                return resultado
                
            except Exception as e:
                print(f"   ❌ Error procesando formato pipe: {e}")
                pass
        
        # CASO 2: Formato JSON array: [{'from': 'user', 'text': '...'}, ...]
        elif conversacion_str.startswith('[') and conversacion_str.endswith(']'):
            try:
                print(f"   🔍 Procesando formato JSON array: {conversacion_str[:100]}...")
                
                conversacion_data = None
                
                # Intentar parsing JSON estándar
                try:
                    conversacion_data = json.loads(conversacion_str)
                except json.JSONDecodeError:
                    # Si falla, intentar con ast.literal_eval
                    try:
                        conversacion_data = ast.literal_eval(conversacion_str)
                    except (ValueError, SyntaxError):
                        pass
                
                # Procesar la lista de conversaciones
                if isinstance(conversacion_data, list):
                    for i, mensaje in enumerate(conversacion_data):
                        if isinstance(mensaje, dict):
                            # Verificar diferentes variantes de claves
                            from_key = mensaje.get('from') or mensaje.get('From')
                            text_key = mensaje.get('text') or mensaje.get('Text') or mensaje.get('message')
                            
                            if from_key == 'user' and text_key:
                                texto_pregunta = str(text_key).strip()
                                if texto_pregunta and texto_pregunta not in preguntas_usuario:
                                    preguntas_usuario.append(texto_pregunta)
                                    print(f"   ✅ Pregunta extraída del array: {texto_pregunta[:60]}...")
                    
                    resultado = ' | '.join(preguntas_usuario) if preguntas_usuario else ''
                    print(f"   📝 Total preguntas extraídas del array: {len(preguntas_usuario)}")
                    return resultado
                    
            except Exception as e:
                print(f"   ❌ Error procesando formato JSON array: {e}")
                pass
        
        # CASO 3: Intentar detectar si es un solo objeto JSON (sin array ni pipe)
        elif conversacion_str.startswith('{') and conversacion_str.endswith('}'):
            try:
                print(f"   🔍 Procesando objeto JSON individual: {conversacion_str[:100]}...")
                
                objeto_parseado = None
                
                # Intentar múltiples estrategias de parsing
                try:
                    objeto_json_str = conversacion_str.replace("'", '"')
                    objeto_parseado = json.loads(objeto_json_str)
                except json.JSONDecodeError:
                    try:
                        objeto_parseado = ast.literal_eval(conversacion_str)
                    except (ValueError, SyntaxError):
                        pass
                
                if isinstance(objeto_parseado, dict):
                    from_key = objeto_parseado.get('from') or objeto_parseado.get('From')
                    text_key = objeto_parseado.get('text') or objeto_parseado.get('Text') or objeto_parseado.get('message')
                    
                    if from_key == 'user' and text_key:
                        texto_pregunta = str(text_key).strip()
                        if texto_pregunta:
                            preguntas_usuario.append(texto_pregunta)
                            print(f"   ✅ Pregunta extraída del objeto: {texto_pregunta[:60]}...")
                
                resultado = ' | '.join(preguntas_usuario) if preguntas_usuario else ''
                print(f"   📝 Total preguntas extraídas del objeto: {len(preguntas_usuario)}")
                return resultado
                
            except Exception as e:
                print(f"   ❌ Error procesando objeto JSON individual: {e}")
                pass
        
        # Si ningún formato funciona, retornar vacío
        print(f"   ❌ No se pudo procesar formato de conversación: {conversacion_str[:100]}...")
        return ''
        
    except Exception as e:
        # Capturar cualquier otro error y retornar vacío
        print(f"   ❌ Error general en extraer_preguntas_usuario: {e}")
        import traceback
        print(f"   ❌ TRACEBACK: {traceback.format_exc()}")
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
        import traceback
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
            """Une valores no vacíos de forma segura"""
            try:
                non_empty = [str(val) for val in series if str(val) not in ['', 'nan', 'None', 'None']]
                return ' | '.join(non_empty) if non_empty else ''
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
        import traceback
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
        s3_key = f"reports/{nombre_archivo}"

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
