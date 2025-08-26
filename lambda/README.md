# 📁 Estructura de Carpetas ETL - Cat Prod Normalize

## 🏗️ Organización General

```
lambda/
├── etl-process1/           # 🔄 ETL-1: Lambda → CSV
│   ├── lambda_function.py  # Función Lambda principal
│   └── requirements.txt    # Dependencias de Lambda
└── etl-process2/           # 🔄 ETL-2: Glue → Parquet  
    ├── glue_job_script.py  # Script de AWS Glue
    └── requirements.txt    # Dependencias de Glue
```

## 📊 ETL-1 (Lambda Process)

**Ubicación**: `lambda/etl-process1/`

**Propósito**: 
- Extrae datos de DynamoDB
- Procesa y normaliza información de usuarios
- Genera archivos CSV en S3

**Componentes**:
- `lambda_function.py`: Lógica principal de extracción y transformación
- `requirements.txt`: Dependencias Python (pandas, boto3, etc.)

**Output**: `s3://cat-prod-normalize-reports/reports/etl-process1/data_YYYYMMDD.csv`

## ⚡ ETL-2 (Glue Process)

**Ubicación**: `lambda/etl-process2/`

**Propósito**:
- Lee archivos CSV generados por ETL-1
- Transforma a formato Parquet optimizado
- Archivo único para consultas eficientes

**Componentes**:
- `glue_job_script.py`: Script de Glue con lógica de transformación
- `requirements.txt`: Dependencias adicionales para Glue

**Input**: `s3://cat-prod-normalize-reports/reports/etl-process1/`
**Output**: `s3://cat-prod-normalize-reports/reports/etl-process2/data.parquet`

## 🔧 Configuración de Paths

### CDK Stack Configuration

Los paths se configuran automáticamente en:

1. **ETL-1 Lambda**: `lib/stacks/cat-prod-normalize-stack.ts`
   ```typescript
   code: lambda.Code.fromAsset('lambda/etl-process1', {
     exclude: ['requirements.txt', '*.pyc', '__pycache__']
   })
   ```

2. **ETL-2 Glue Script**: `bin/cat-prod-normalize.ts`
   ```typescript
   glueScriptS3Uri: 's3://cat-prod-normalize-reports/scripts/etl-process2/glue_job_script.py'
   ```

### S3 Bucket Structure

```
s3://cat-prod-normalize-reports/
├── reports/
│   ├── etl-process1/           # CSV files (Lambda output)
│   └── etl-process2/           # Parquet files (Glue output)
├── scripts/
│   └── etl-process2/           # Glue script storage
└── athena/
    └── results/                # Query results
```

## 🚀 Deployment

1. **Compilar proyecto**:
   ```bash
   npm run build
   ```

2. **Verificar síntesis**:
   ```bash
   npx cdk synth
   ```

3. **Deploy ETL-1**:
   ```bash
   npx cdk deploy cat-prod-normalize-stack
   ```

4. **Deploy ETL-2**:
   ```bash
   npx cdk deploy cat-prod-etl2-stack
   ```

## 📋 Logs y Monitoreo

- **ETL-1 Logs**: CloudWatch Logs - `/aws/lambda/cat-prod-lambda-normalize`
- **ETL-2 Logs**: CloudWatch Logs - `/aws-glue/jobs/`
- **S3 Events**: EventBridge para orquestación automática

## 🔄 Flujo de Datos

1. **ETL-1**: DynamoDB → Lambda → CSV (S3)
2. **Trigger**: S3 Event → EventBridge → Glue Job Start
3. **ETL-2**: CSV → Glue → Parquet (S3)
4. **Analysis**: Parquet → Athena → Analytics

## 📝 Notas de Desarrollo

- **Separación clara**: Cada ETL tiene su propia carpeta y dependencias
- **Reutilización**: Bucket S3 compartido entre procesos
- **Escalabilidad**: ETL-2 usa Spark para grandes volúmenes
- **Mantenimiento**: Scripts independientes para cada proceso
