import json
import pandas as pd
import boto3
import ast
from datetime import date, datetime
import io
import os
from collections import Counter
import re

def lambda_handler(event, context):
    """
    Función Lambda para procesar datos de DynamoDB y generar archivo Excel normalizado
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
        
        # PASO 2: Procesar y normalizar datos
        print("🔗 PROCESANDO MERGE DE CONVERSACIONES Y FEEDBACK")
        df = procesar_merge_conversaciones_feedback(df)
        print(f"   • Después del merge: {len(df)} filas")
        
        # PASO 3: Aplicar filtros
        print("🔧 APLICANDO FILTROS")
        df = aplicar_filtros(df)
        print(f"   • Después de filtros: {len(df)} filas")
        
        # PASO 4: Extraer preguntas
        print("💬 EXTRAYENDO PREGUNTAS DE CONVERSACIONES")
        df = extraer_preguntas_conversaciones(df)
        
        # PASO 5: Crear dataset con 12 columnas
        print("📋 CREANDO DATASET CON 12 COLUMNAS")
        df_12_columnas = crear_dataset_12_columnas(df)
        
        # PASO 6: Agrupar usuarios únicos
        print("🔄 AGRUPANDO USUARIOS ÚNICOS")
        df_usuarios_unicos = agrupar_usuarios_unicos(df_12_columnas)
        
        # PASO 7: Clasificar feedback
        print("🎯 CLASIFICANDO FEEDBACK")
        df_usuarios_unicos = clasificar_feedback(df_usuarios_unicos)
        
        # PASO 8: Extraer respuestas de feedback
        print("💬 EXTRAYENDO RESPUESTAS DE FEEDBACK")
        df_usuarios_unicos = extraer_respuestas_feedback(df_usuarios_unicos)
        
        # PASO 9: Generar archivo Excel
        print("💾 GENERANDO ARCHIVO EXCEL")
        archivo_s3 = generar_archivo_excel(df_usuarios_unicos)
        
        # Actualizar resultado
        result['body'] = {
            'message': 'Proceso completado exitosamente',
            'usuarios_procesados': len(df_usuarios_unicos),
            'archivo_generado': archivo_s3,
            'estadisticas': {
                'total_conversaciones': df_usuarios_unicos['numero_conversaciones'].sum(),
                'usuarios_con_feedback': (df_usuarios_unicos['feedback'] != '').sum(),
                'usuarios_con_preguntas': (df_usuarios_unicos['pregunta_conversacion'] != '').sum()
            }
        }
        
        print(f"✅ PROCESO COMPLETADO - {len(df_usuarios_unicos)} usuarios procesados")
        return result
        
    except Exception as e:
        print(f"❌ ERROR en lambda_handler: {str(e)}")
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
        table_name = 'cat-prod-catia-conversations-table'
        
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
        # Procesar UserData
        user_data_parsed = df['UserData'].apply(parse_user_data_clean)
        df_user_data = pd.DataFrame(user_data_parsed.tolist())
        
        df['nombre'] = df_user_data['nombre'].fillna('')
        df['gerencia'] = df_user_data['ciudad'].fillna('')
        
        # Renombrar columnas
        df = df.rename(columns={
            'CreatedAt': 'fecha_primera_conversacion',
            'Conversation': 'conversacion_completa'
        })
        
        # Rellenar nombres vacíos
        df.loc[df['nombre'] == '', 'nombre'] = 'Usuario Anónimo'
        
        # Filtro de ciudad PERMISIVO
        patron_excluir = r'(?i)(mexico|medell|cali|barranquilla|cartagena|potosí|valle|antioquia)'
        df = df[~df['gerencia'].str.contains(patron_excluir, regex=True, na=False)].copy()
        df.loc[df['gerencia'] == '', 'gerencia'] = 'Bogotá (no especificada)'
        
        # Filtro de fechas PERMISIVO
        fecha_inicio = date(2025, 8, 4)
        fecha_fin = date(2025, 8, 20)
        
        df['fecha_temp'] = pd.to_datetime(df['fecha_primera_conversacion'], errors='coerce')
        
        # Incluir fechas del rango Y fechas nulas
        mask_fechas = (
            ((df['fecha_temp'].dt.date >= fecha_inicio) & (df['fecha_temp'].dt.date <= fecha_fin)) |
            df['fecha_temp'].isna()
        )
        
        df = df[mask_fechas].copy()
        df['fecha_primera_conversacion'] = df['fecha_temp'].dt.strftime('%d/%m/%Y')
        df.loc[df['fecha_temp'].isna(), 'fecha_primera_conversacion'] = 'Sin fecha'
        df.drop(columns=['fecha_temp'], inplace=True)
        
        return df
        
    except Exception as e:
        print(f"❌ ERROR en aplicar_filtros: {str(e)}")
        raise

def parse_user_data_clean(value):
    """Parsear UserData de forma segura"""
    if pd.isna(value) or value is None:
        return {'nombre': '', 'ciudad': ''}
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ['nan', 'none', 'null']:
            return {'nombre': '', 'ciudad': ''}
        try:
            # Intentar JSON
            result = json.loads(value)
            if isinstance(result, dict):
                return {
                    'nombre': result.get('nombre', ''),
                    'ciudad': result.get('ciudad', result.get('gerencia', ''))
                }
        except:
            try:
                # Intentar literal_eval
                result = ast.literal_eval(value)
                if isinstance(result, dict):
                    return {
                        'nombre': result.get('nombre', ''),
                        'ciudad': result.get('ciudad', result.get('gerencia', ''))
                    }
            except:
                pass
    return {'nombre': '', 'ciudad': ''}

def extraer_preguntas_conversaciones(df):
    """Extrae preguntas de usuario desde conversacion_completa"""
    try:
        df['pregunta_conversacion'] = df['conversacion_completa'].apply(extraer_preguntas_usuario)
        return df
    except Exception as e:
        print(f"❌ ERROR en extraer_preguntas_conversaciones: {str(e)}")
        raise

def extraer_preguntas_usuario(conversacion_json):
    """Extrae todas las preguntas del usuario desde el JSON de conversación"""
    if pd.isna(conversacion_json) or conversacion_json == '' or conversacion_json is None:
        return ''
    
    try:
        conversacion_str = str(conversacion_json).strip()
        
        if not conversacion_str.startswith('[') or not conversacion_str.endswith(']'):
            return ''
        
        conversacion_data = json.loads(conversacion_str)
        
        if not isinstance(conversacion_data, list):
            return ''
        
        preguntas_usuario = []
        
        for mensaje in conversacion_data:
            if isinstance(mensaje, dict) and 'from' in mensaje and 'text' in mensaje:
                if mensaje['from'] == 'user':
                    texto_pregunta = str(mensaje['text']).strip()
                    if texto_pregunta:
                        preguntas_usuario.append(texto_pregunta)
        
        return ' | '.join(preguntas_usuario) if preguntas_usuario else ''
        
    except (json.JSONDecodeError, ValueError, KeyError, TypeError):
        try:
            conversacion_data = ast.literal_eval(conversacion_str)
            if isinstance(conversacion_data, list):
                preguntas_usuario = []
                for mensaje in conversacion_data:
                    if isinstance(mensaje, dict) and mensaje.get('from') == 'user' and 'text' in mensaje:
                        texto_pregunta = str(mensaje['text']).strip()
                        if texto_pregunta:
                            preguntas_usuario.append(texto_pregunta)
                return ' | '.join(preguntas_usuario) if preguntas_usuario else ''
        except:
            pass
        
        return ''

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
        
        # Contar conversaciones
        df['numero_conversaciones'] = df['conversacion_completa'].apply(
            lambda x: max(str(x).count('bot'), str(x).count('user')) if pd.notna(x) else 1
        )
        
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
        aggregation_config = {
            'nombre': lambda x: x[x != 'Usuario Anónimo'].iloc[0] if len(x[x != 'Usuario Anónimo']) > 0 else 'Usuario Anónimo',
            'gerencia': lambda x: x[x != 'Bogotá (no especificada)'].iloc[0] if len(x[x != 'Bogotá (no especificada)']) > 0 else x.iloc[0],
            'ciudad': lambda x: x[x != 'Bogotá (no especificada)'].iloc[0] if len(x[x != 'Bogotá (no especificada)']) > 0 else x.iloc[0],
            'fecha_primera_conversacion': 'first',
            'numero_conversaciones': 'sum',
            'conversacion_completa': lambda x: ' | '.join([str(conv) for conv in x if str(conv) not in ['', 'nan', 'None']]),
            'feedback_total': lambda x: ' | '.join([str(f) for f in x if str(f) not in ['', 'nan', 'None']]) if any(str(f) not in ['', 'nan', 'None'] for f in x) else '',
            'numero_feedback': 'sum',
            'pregunta_conversacion': lambda x: ' | '.join([str(p) for p in x if str(p) not in ['', 'nan', 'None']]) if any(str(p) not in ['', 'nan', 'None'] for p in x) else '',
            'feedback': lambda x: ' | '.join([str(f) for f in x if str(f) not in ['', 'nan', 'None']]) if any(str(f) not in ['', 'nan', 'None'] for f in x) else '',
            'respuesta_feedback': lambda x: ' | '.join([str(r) for r in x if str(r) not in ['', 'nan', 'None']]) if any(str(r) not in ['', 'nan', 'None'] for r in x) else ''
        }
        
        df_usuarios_unicos = df_12_columnas.groupby('usuario_id').agg(aggregation_config).reset_index()
        
        return df_usuarios_unicos
        
    except Exception as e:
        print(f"❌ ERROR en agrupar_usuarios_unicos: {str(e)}")
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

def generar_archivo_excel(df_usuarios_unicos):
    """Genera archivo Excel y lo sube a S3"""
    try:
        # Asegurar orden de columnas
        columnas_finales = [
            'usuario_id', 'nombre', 'gerencia', 'ciudad', 'fecha_primera_conversacion',
            'numero_conversaciones', 'conversacion_completa', 'feedback_total',
            'numero_feedback', 'pregunta_conversacion', 'feedback', 'respuesta_feedback'
        ]
        
        df_usuarios_unicos = df_usuarios_unicos[columnas_finales]
        
        # Crear archivo Excel en memoria
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        nombre_archivo = f"Dashboard_Usuarios_Catia_{timestamp}_PROCESADO_COMPLETO.xlsx"
        
        # Crear buffer en memoria
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df_usuarios_unicos.to_excel(writer, sheet_name='Dashboard_Usuarios_Procesado', index=False)
        
        excel_buffer.seek(0)
        
        # Subir a S3
        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'cat-prod-normalize-reports')
        s3_key = f"reports/{nombre_archivo}"
        
        s3_client.upload_fileobj(
            excel_buffer,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ Archivo subido a S3: {s3_url}")
        
        return s3_url
        
    except Exception as e:
        print(f"❌ ERROR en generar_archivo_excel: {str(e)}")
        raise
