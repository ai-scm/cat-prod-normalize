import json
import boto3
import os
import urllib.parse
from datetime import datetime
import logging

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Handler principal que se ejecuta cuando se sube un archivo a S3
    """
    try:
        # 📊 Configuración desde variables de entorno
        quicksight_dataset_id = os.environ.get('QUICKSIGHT_DATASET_ID', 'Dataset_prueba')
        quicksight_dataset_name = os.environ.get('QUICKSIGHT_DATASET_NAME', 'Dataset_prueba')
        aws_account_id = os.environ.get('AWS_ACCOUNT_ID')
        s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
        
        logger.info(f"🚀 Iniciando actualización de QuickSight dataset: {quicksight_dataset_name}")
        logger.info(f"📊 Dataset ID: {quicksight_dataset_id}")
        logger.info(f"🪣 S3 Bucket: {s3_bucket_name}")
        
        # 🔍 Procesar evento de S3
        if 'Records' in event:
            for record in event['Records']:
                # Verificar que es un evento de S3
                if record.get('eventSource') == 'aws:s3':
                    bucket = record['s3']['bucket']['name']
                    key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
                    
                    logger.info(f"📁 Archivo detectado: s3://{bucket}/{key}")
                    
                    # ✅ Verificar que es un archivo Excel en la carpeta reports/
                    if key.startswith('reports/') and key.endswith('.xlsx'):
                        logger.info(f"✅ Archivo Excel válido detectado: {key}")
                        
                        # 🔄 Actualizar dataset de QuickSight
                        result = refresh_quicksight_dataset(
                            aws_account_id=aws_account_id,
                            dataset_id=quicksight_dataset_id,
                            dataset_name=quicksight_dataset_name,
                            s3_file_path=f"s3://{bucket}/{key}"
                        )
                        
                        return {
                            'statusCode': 200,
                            'body': json.dumps({
                                'message': '✅ Dataset de QuickSight actualizado exitosamente',
                                'dataset_id': quicksight_dataset_id,
                                'dataset_name': quicksight_dataset_name,
                                's3_file': f"s3://{bucket}/{key}",
                                'refresh_result': result,
                                'timestamp': datetime.utcnow().isoformat()
                            })
                        }
                    else:
                        logger.info(f"⏭️ Archivo ignorado (no es Excel o no está en reports/): {key}")
        
        # 🔄 Ejecutión manual (para testing)
        else:
            logger.info("🧪 Ejecución manual detectada - actualizando dataset")
            result = refresh_quicksight_dataset(
                aws_account_id=aws_account_id,
                dataset_id=quicksight_dataset_id,
                dataset_name=quicksight_dataset_name,
                s3_file_path=f"s3://{s3_bucket_name}/reports/"
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': '✅ Dataset actualizado manualmente',
                    'dataset_id': quicksight_dataset_id,
                    'refresh_result': result,
                    'timestamp': datetime.utcnow().isoformat()
                })
            }
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'ℹ️ No se encontraron archivos Excel para procesar',
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"❌ ERROR en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': '❌ Error al actualizar dataset de QuickSight',
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def refresh_quicksight_dataset(aws_account_id, dataset_id, dataset_name, s3_file_path):
    """
    Actualiza un dataset de QuickSight usando la API
    """
    try:
        # 🔧 Cliente de QuickSight
        quicksight = boto3.client('quicksight')
        
        logger.info(f"🔄 Iniciando refresh del dataset: {dataset_name}")
        logger.info(f"📍 Dataset ID: {dataset_id}")
        logger.info(f"📁 Archivo S3: {s3_file_path}")
        
        # 🕐 Crear ingestion (refresh) del dataset
        ingestion_id = f"etl-refresh-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        
        response = quicksight.create_ingestion(
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
            AwsAccountId=aws_account_id
        )
        
        logger.info(f"✅ Ingestion creada exitosamente:")
        logger.info(f"   📊 Ingestion ID: {ingestion_id}")
        logger.info(f"   🔗 ARN: {response.get('Arn', 'N/A')}")
        logger.info(f"   📍 Status: {response.get('IngestionStatus', 'Unknown')}")
        
        # 🔍 Verificar el estado del refresh (opcional)
        ingestion_status = check_ingestion_status(
            quicksight, aws_account_id, dataset_id, ingestion_id
        )
        
        return {
            'ingestion_id': ingestion_id,
            'ingestion_arn': response.get('Arn'),
            'status': response.get('IngestionStatus'),
            'detailed_status': ingestion_status,
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            's3_file': s3_file_path
        }
        
    except Exception as e:
        logger.error(f"❌ ERROR en refresh_quicksight_dataset: {str(e)}")
        raise

def check_ingestion_status(quicksight_client, aws_account_id, dataset_id, ingestion_id):
    """
    Verifica el estado de la ingestion de QuickSight
    """
    try:
        response = quicksight_client.describe_ingestion(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id
        )
        
        ingestion = response.get('Ingestion', {})
        status = ingestion.get('IngestionStatus', 'UNKNOWN')
        
        logger.info(f"📊 Estado del refresh: {status}")
        
        if 'ErrorInfo' in ingestion:
            error_info = ingestion['ErrorInfo']
            logger.warning(f"⚠️ Error en ingestion: {error_info}")
        
        return {
            'status': status,
            'created_time': ingestion.get('CreatedTime', '').isoformat() if ingestion.get('CreatedTime') else None,
            'ingestion_size_in_bytes': ingestion.get('IngestionSizeInBytes', 0),
            'row_info': ingestion.get('RowInfo', {}),
            'error_info': ingestion.get('ErrorInfo', None)
        }
        
    except Exception as e:
        logger.warning(f"⚠️ No se pudo verificar estado de ingestion: {str(e)}")
        return {'status': 'UNKNOWN', 'error': str(e)}

def list_datasets(aws_account_id):
    """
    Función auxiliar para listar datasets disponibles (útil para debugging)
    """
    try:
        quicksight = boto3.client('quicksight')
        
        response = quicksight.list_data_sets(
            AwsAccountId=aws_account_id,
            MaxResults=50
        )
        
        datasets = response.get('DataSetSummaries', [])
        logger.info(f"📊 Datasets disponibles: {len(datasets)}")
        
        for dataset in datasets:
            logger.info(f"   🔹 {dataset.get('Name')} (ID: {dataset.get('DataSetId')})")
        
        return datasets
        
    except Exception as e:
        logger.warning(f"⚠️ No se pudieron listar datasets: {str(e)}")
        return []

# 🧪 Para testing local
if __name__ == "__main__":
    # Evento de prueba simulando S3
    test_event = {
        "Records": [
            {
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": "cat-prod-normalize-reports"},
                    "object": {"key": "reports/Dashboard_Usuarios_Catia_20250821_0800_PROCESADO_COMPLETO.xlsx"}
                }
            }
        ]
    }
    
    # Variables de entorno de prueba
    os.environ['QUICKSIGHT_DATASET_ID'] = 'Dataset_prueba'
    os.environ['QUICKSIGHT_DATASET_NAME'] = 'Dataset_prueba'
    os.environ['AWS_ACCOUNT_ID'] = '123456789012'  # Reemplazar con tu Account ID
    os.environ['S3_BUCKET_NAME'] = 'cat-prod-normalize-reports'
    
    result = lambda_handler(test_event, None)
    print("🧪 Resultado del test:")
    print(json.dumps(result, indent=2))
