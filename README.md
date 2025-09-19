# 🚀 Cat Prod Normalize - Multi-Stack Data Pipeline

Sistema ETL completo para el procesamiento automatizado de conversaciones del chatbot **Catia**, implementado con AWS CDK como pipeline de datos empresarial.

Este proyecto implementa un **sistema de análisis multi-fuente** para conversaciones del chatbot Catia, con dos pipelines independientes: uno para análisis de conversaciones y feedback, y otro para análisis de costos de tokens de Amazon Bedrock Claude Sonnet 3.5.

### 🎯 **Objetivos del Sistema**
- **Pipeline ETL Principal**: Conversaciones DynamoDB → S3 → Athena (Stacks 1-2)
- **Pipeline Tokens Independiente**: Análisis de costos Claude Sonnet 3.5 (Stack 3)
- **Análisis de Costos Bedrock**: Cálculo de tokens y estimaciones de costos AWS
- **Optimización de Datos**: Conversión CSV → Parquet para consultas eficientes
- **Escalabilidad**: Arquitectura serverless multi-stack independiente
- **Monitoreo**: Tags detallados para Cost Explorer y billing por componente

## 🏗️ Arquitectura Multi-Stack Independiente

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        🔄 PIPELINE PRINCIPAL (Stacks 1-2)                   │
│                           Análisis de Conversaciones                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ DynamoDB-1  │───▶│ Lambda ETL-1│───▶│   S3 CSV    │───▶│ Glue ETL-2  │
│Conversations│    │(Normalize)  │    │ Raw Reports │    │(Transform)  │
│    Table    │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                                        │
                           ▼                                        ▼
                   ┌─────────────┐                          ┌─────────────┐
                   │EventBridge  │                          │ S3 Parquet  │───▶ Athena
                   │ Scheduler   │                          │Optimized DB │     Analytics
                   └─────────────┘                          └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │Glue Crawler │
                                                            │Auto-Schema  │
                                                            └─────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          PIPELINE Tokens (Stack 3)                 │
│                    Análisis de Tokens Amazon Bedrock                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ DynamoDB-2  │───▶│   Lambda    │───▶│ S3 Tokens   │───▶ Athena
│   Bedrock   │    │   Tokens    │    │  Reports    │     Analytics
│Conversations│    │ (Claude)    │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │EventBridge  │
                   │Independent  │
                   │ Scheduler   │
                   └─────────────┘
```

## 📁 Estructura del Proyecto

```
cat-prod-normalize/
├── 📓 notebook/                                    # Código origen de referencia
│   ├── cat-prod-normalize-data.ipynb               # Notebook original
│   └── cat_prod_normalize_script.py                # Script convertido
├── 🐍 lambda/                                      # Funciones Lambda multi-ETL
│   ├── README.md                                   # Documentación ETL específica
│   ├── etl-process1/                               # 🔄 ETL-1: Extracción y normalización
│   │   ├── lambda_function.py                      # Core: DynamoDB → CSV
│   │   └── requirements.txt                        # pandas, boto3, numpy
│   ├── etl-process2/                               # 🔄 ETL-2: Transformación a Parquet
│   │   ├── glue_job_script.py                      # Glue: CSV → Parquet + tokens
│   │   └── requirements.txt                        # tiktoken, pyspark
│   └── tokens-process/                             # 💰 Análisis de costos
│       ├── tokens_lambda.py                        # Tokens GPT + cálculo costos
│       └── requirements.txt                        # pandas, boto3
├── 📚 lib/                                         # Definiciones CDK (3 stacks)
│   ├── constructs/                                 # Componentes reutilizables
│   │   ├── athena-construct.ts                     # WorkGroup Athena
│   │   ├── catalog-construct.ts                    # Glue Database + Crawler
│   │   ├── orchestrator-construct.ts               # EventBridge automation
│   │   └── transform-job-construct.ts              # Glue Job ETL-2
│   └── stacks/                                     # 🏗️ 3 Stacks principales
│       ├── cat-prod-normalize-stack.ts             # Stack 1: ETL-1 (Lambda)
│       ├── cad-prod-etl-stack.ts                   # Stack 2: ETL-2 (Glue)
│       └── cat-prod-tokens-stack.ts                # Stack 3: Tokens (Lambda)
├── 🎯 bin/                                         # Punto de entrada
│   └── cat-prod-normalize.ts                       # App CDK multi-stack
├── ⚙️ config/                                      # Configuración centralizada
│   ├── accountConfig.json                          # Cuenta AWS (081899001252)
│   ├── config.json                                 # Namespace (cat-prod)
│   └── tags.json                                   # Tags estándar (P0260)
├── 🧪 test/                                        # Tests unitarios
│   └── cat-prod-normalize.test.ts                  # Tests CDK
└── 📋 archivos raíz
    ├── README.md                                   # Este archivo
    ├── package.json                                # Dependencias Node.js
    ├── cdk.json                                    # Configuración CDK
    ├── tsconfig.json                               # TypeScript config
    └── test_token_functions.py                     # Test tokens local
```

## 🏭 Stacks y Recursos Desplegados

### **🔄 Stack 1: `cat-prod-normalize-stack` (ETL-1)**
**Propósito**: Extracción y normalización desde DynamoDB

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| 🐍 **Lambda** | `cat-prod-lambda-normalize` | ETL-1: DynamoDB → CSV (12 columnas) |
| 📦 **S3 Bucket** | `cat-prod-normalize-reports` | Data Lake central |
| 🔐 **IAM Role** | `CatProdNormalizeETLLambdaRole` | Permisos DynamoDB + S3 |
| ⏰ **EventBridge** | `cat-prod-daily-etl-schedule` | Trigger diario 11:30 PM COL |
| 📊 **Layer** | `AWSSDKPandas-Python39:13` | Dependencias (pandas, boto3) |

### **🔄 Stack 2: `cat-prod-etl2-stack` (ETL-2)**
**Propósito**: Transformación a formato analítico optimizado

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| ⚡ **Glue Job** | `cat-prod-etl2-parquet` | ETL-2: CSV → Parquet + tokens |
| 🕷️ **Glue Crawler** | `curated-crawler` | Auto-detección de esquemas |
| 🗄️ **Glue Database** | `cat_prod_analytics_db` | Catálogo de metadatos |
| 🔍 **Athena WorkGroup** | `wg-cat-prod-analytics` | Consultas SQL optimizadas |
| 🔐 **IAM Roles** | Multiple | Permisos Glue + S3 + EventBridge |
| 🎯 **EventBridge** | `S3 Object Created` | Trigger automático ETL-2 |

### **💰 Stack 3: `cat-prod-tokens-stack` (Análisis tokens)**
**Propósito**: Análisis de tokens Amazon Bedrock Claude Sonnet 3.5

| Recurso | Nombre | Descripción |
|---------|---------|-------------|
| 🐍 **Lambda** | `cat-prod-lambda-tokens` | Análisis tokens Claude + costos |
| 📊 **Layer** | `cat-prod-pandas-numpy-layer` | pandas, numpy, boto3 |
| 🔐 **IAM Role** | `CatProdTokensLambdaRole` | DynamoDB Bedrock + S3 + Athena |
| ⏰ **EventBridge** | `cat-prod-daily-tokens-schedule` | Análisis diario independiente |
| 🗄️ **Data Source** | `BedrockChatStack-DatabaseConversationTable` | **Tabla independiente** |

### **🎯 Fase 1: ETL-1 (Lambda Normalize)**
1. **Trigger**: EventBridge Schedule `cron(30 4 * * ? *)` (UTC)
2. **Fuente**: DynamoDB `BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF`
3. **Procesamiento**: 
   - Normalización de usuarios únicos por `user_id`
   - Extracción de preguntas desde JSON `conversation_history`
   - Clasificación de feedback: `like/dislike/mixed`
   - Merge de tablas conversations + feedback
4. **Salida**: `s3://cat-prod-normalize-reports/reports/etl-process1/data_YYYYMMDD.csv`

### **🎯 Fase 2: ETL-2 (Glue Transform)**
1. **Trigger**: S3 Event `ObjectCreated` en `/etl-process1/`
2. **Motor**: Glue Job con Spark (2 workers G.1X)
3. **Procesamiento**:
   - Lectura CSV más reciente con PySpark
   - Conversión de tipos de datos optimizada
   - **Cálculo de tokens** con biblioteca `tiktoken`
   - Generación de archivo Parquet único
4. **Salida**: `s3://cat-prod-normalize-reports/reports/etl-process2/data.parquet`

### **🎯 Fase 3: Catalogación Automática**
1. **Trigger**: Glue Job State Change → `SUCCEEDED`
2. **Acción**: Crawler escanea `/etl-process2/data.parquet/`
3. **Resultado**: Schema actualizado en `cat_prod_analytics_db`
4. **Disponibilidad**: Tabla lista para consultas Athena

### **🎯 Fase 4: Análisis de Tokens Bedrock (Lambda Independiente)**
1. **Trigger**: EventBridge Schedule independiente `cron(30 4 * * ? *)`
2. **Fuente**: DynamoDB `BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF` **(independiente)**
3. **Procesamiento**:
   - Análisis de consumo tokens **Claude Sonnet 3.5** por conversación
   - Cálculo aproximado: `LENGTH(text) / 4` (estándar Amazon Bedrock)
   - Estimación de costos **Amazon Bedrock Claude Sonnet 3.5**
   - Estadísticas agregadas por usuario/fecha
4. **Salida**: `s3://cat-prod-normalize-reports/tokens-analysis/tokens_YYYYMMDD.csv`

> **📝 Nota Importante**: El Stack 3 (Tokens) es **completamente independiente** de los Stacks 1-2. Usa una tabla DynamoDB diferente y se enfoca únicamente en análisis de costos de Amazon Bedrock Claude Sonnet 3.5, no en el procesamiento de conversaciones del chatbot Catia.

## 📊 Esquema de Datos Final

### **🗂️ Columnas del Dataset Principal (12 campos)**

| # | Columna | Tipo | Descripción | Origen |
|---|---------|------|-------------|--------|
| 1 | `usuario_id` | String | ID único del usuario | DynamoDB |
| 2 | `nombre` | String | Nombre completo del usuario | DynamoDB |
| 3 | `gerencia` | String | Gerencia/departamento | DynamoDB |
| 4 | `ciudad` | String | Ciudad del usuario | DynamoDB |
| 5 | `fecha_primera_conversacion` | Date | Primera interacción (DD/MM/YYYY) | Calculado |
| 6 | `numero_conversaciones` | Integer | Total conversaciones del usuario | Calculado |
| 7 | `conversacion_completa` | JSON | Historia completa de mensajes | DynamoDB |
| 8 | `feedback_total` | JSON | Datos brutos de feedback | DynamoDB |
| 9 | `numero_feedback` | Integer | Cantidad de feedbacks dados | Calculado |
| 10 | `pregunta_conversacion` | String | Preguntas extraídas del usuario | Extraído |
| 11 | `feedback` | String | Clasificación: like/dislike/mixed | Calculado |
| 12 | `respuesta_feedback` | String | Comments y options del feedback | Extraído |

### **💰 Dataset de Análisis de Tokens Amazon Bedrock Claude Sonnet 3.5**

| Campo | Descripción | Cálculo |
|-------|-------------|---------|
| `conversation_id` | ID único de conversación Bedrock | DynamoDB PK |
| `user_id` | Usuario que realizó la conversación | DynamoDB |
| `fecha` | Fecha de la conversación | timestamp |
| `token_pregunta` | Tokens de entrada (input) | `LENGTH(user + system + chunks) / 4` |
| `token_respuesta` | Tokens de salida (output) | `LENGTH(assistant) / 4` |
| `tokens_total` | Total tokens consumidos | input + output |
| `costo_estimado_usd` | Costo estimado en USD | **Tarifa Amazon Bedrock Claude Sonnet 3.5** * tokens |
| `modelo` | Modelo utilizado | `claude-3-5-sonnet-20240620-v1:0` |
| `region` | Región AWS | `us-east-1` |

> **⚠️ Fuente de Datos**: Este análisis usa la tabla DynamoDB de **Amazon Bedrock** (`BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF`), **NO** la tabla del chatbot Catia (`cat-prod-catia-conversations-table`).

## 🚀 Instalación y Despliegue

### **📋 Prerrequisitos**
- **Node.js** >= 18.0.0
- **AWS CLI** configurado con cuenta `081899001252`
- **CDK CLI** instalado: `npm install -g aws-cdk`
- **Permisos IAM** para crear recursos Lambda, S3, Glue, Athena

### **⚙️ Configuración (Ya incluida)**
Los archivos están preconfigurados en `config/`:
- `accountConfig.json`: Cuenta AWS `081899001252` + región `us-east-1`
- `config.json`: Namespace `cat-prod` para nomenclatura
- `tags.json`: Tags corporativos (ProjectId: P0260, Env: PROD, Client: CAT)

### **🔧 Pasos de Instalación**

```bash
# 1. Clonar y preparar dependencias
npm install

# 2. Compilar TypeScript
npm run build

# 3. Verificar síntesis (sin desplegar)
npx cdk synth

# 4. Bootstrap CDK (solo primera vez)
npx cdk bootstrap

# 5. Desplegar en orden específico
npx cdk deploy cat-prod-normalize-stack      # Stack 1: ETL-1 Lambda
npx cdk deploy cat-prod-etl2-stack           # Stack 2: ETL-2 Glue  
npx cdk deploy cat-prod-tokens-stack         # Stack 3: Tokens Analysis

# 6. Verificar recursos creados
npx cdk list
```

### **🏗️ Recursos Desplegados por Stack**

**Stack 1** (`cat-prod-normalize-stack`):
- ✅ S3 Bucket: `cat-prod-normalize-reports`
- ✅ Lambda: `cat-prod-lambda-normalize` (1024MB, 15min timeout)
- ✅ EventBridge: `cat-prod-daily-etl-schedule` (11:30 PM COL)
- ✅ IAM Role: Permisos DynamoDB + S3

**Stack 2** (`cat-prod-etl2-stack`):
- ✅ Glue Job: `cat-prod-etl2-parquet` (Spark 2x G.1X)
- ✅ Glue Database: `cat_prod_analytics_db`
- ✅ Glue Crawler: `curated-crawler` (auto-schema)
- ✅ Athena WorkGroup: `wg-cat-prod-analytics`
- ✅ EventBridge: S3 Object Created trigger

**Stack 3** (`cat-prod-tokens-stack`) - **INDEPENDIENTE**:
- ✅ Lambda: `cat-prod-lambda-tokens` (512MB, 1min timeout)
- ✅ Lambda Layer: `cat-prod-pandas-numpy-layer`
- ✅ EventBridge: `cat-prod-daily-tokens-schedule` (independiente)
- ✅ IAM Role: DynamoDB Bedrock + S3 + Athena permisos
- ✅ **Data Source**: `BedrockChatStack-DatabaseConversationTable` (diferente a Stacks 1-2)

## 🔧 Configuración Avanzada

### **🌍 Variables de Entorno**

**Lambda ETL-1**:
```bash
S3_BUCKET_NAME=cat-prod-normalize-reports
DYNAMODB_TABLE_NAME=cat-prod-catia-conversations-table  
PROJECT_ID=P0260
ENVIRONMENT=PROD
CLIENT=CAT
```

**Lambda Tokens** (Stack 3 - Independiente):
```bash
S3_BUCKET_NAME=cat-prod-normalize-reports
S3_OUTPUT_PREFIX=tokens-analysis/
DYNAMODB_TABLE_NAME=BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF
ATHENA_DATABASE=cat_prod_analytics_db
ATHENA_WORKGROUP=wg-cat-prod-analytics
ATHENA_OUTPUT_LOCATION=s3://cat-prod-normalize-reports/athena/results/
BEDROCK_MODEL=claude-3-5-sonnet-20240620-v1:0
AWS_REGION=us-east-1
```

**Glue Job ETL-2**:
```bash
--INPUT_PREFIX=reports/etl-process1/
--OUTPUT_PREFIX=reports/etl-process2/
--BUCKET_NAME=cat-prod-normalize-reports
```

### **📊 Configuración de Horarios**

```typescript
// EventBridge Schedule - Ambos stacks
schedule: events.Schedule.expression('cron(30 4 * * ? *)') // 11:30 PM Colombia
```

### **🏷️ Sistema de Tags para Cost Explorer**

| Tag | Valor | Propósito |
|-----|-------|----------|
| `BillingTag` | `ETL-LAMBDA-ETL1`, `ETL-GLUE-ETL2`, `TOKENS-BEDROCK-CLAUDE` | Separación de costos por componente |
| `CostCenter` | `DATA-ANALYTICS`, `BEDROCK-ANALYTICS` | Centro de costos |
| `Project` | `CAT-PROD-NORMALIZE`, `CAT-PROD-TOKENS-BEDROCK` | Identificación proyecto |
| `Environment` | `PROD` | Ambiente |
| `ETLComponent` | `ETL-1`, `ETL-2`, `TOKENS-BEDROCK-ANALYSIS` | Componente del pipeline |
| `DataSource` | `DynamoDB-CATIA`, `DynamoDB-BEDROCK`, `S3-CSV` | Fuente de datos |
| `DataTarget` | `S3-CSV`, `S3-PARQUET`, `S3-TOKENS` | Destino de datos |
| `Owner` | `DataEngineering`, `BedrockAnalytics` | Propietario técnico |

## 🧪 Testing y Validación

### **🔍 Tests Locales**

```bash
# Tests unitarios CDK
npm run test

# Test función tokens Bedrock local
python test_token_functions.py

# Test Lambda ETL-1 local (requiere credenciales AWS)
cd lambda/etl-process1
python -c "
import lambda_function
result = lambda_function.lambda_handler({}, {})
print(result)
"

# Test Lambda Tokens Bedrock local (requiere credenciales AWS)
cd lambda/tokens-process
python -c "
import tokens_lambda
result = tokens_lambda.lambda_handler({}, {})
print(result)
"

# Validar script Glue local (requiere Spark)
cd lambda/etl-process2  
python glue_job_script.py
```

### **📊 Monitoreo en Producción**

#### **CloudWatch Logs**
```bash
# Logs ETL-1 Lambda
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/cat-prod-lambda-normalize"

# Logs ETL-2 Glue
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/jobs/cat-prod-etl2-parquet"

# Logs Tokens Lambda  
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/cat-prod-lambda-tokens"
```

#### **Métricas Clave por Stack**
| Métrica | ETL-1 Lambda | ETL-2 Glue | Tokens Bedrock Lambda |
|---------|--------------|------------|----------------------|
| **Duration** | < 15 min | < 10 min | < 1 min |
| **Memory** | < 1024 MB | N/A (Spark) | < 512 MB |
| **Errors** | 0% | 0% | 0% |
| **Cost/día** | ~$0.10 | ~$0.50 | ~$0.02 |
| **Data Source** | DynamoDB Catia | S3 CSV | **DynamoDB Bedrock** |

#### **Validación de Datos**
```sql
-- Athena: Validar ETL-2 output
SELECT 
    COUNT(*) as total_usuarios,
    MIN(fecha_primera_conversacion) as fecha_min,
    MAX(fecha_primera_conversacion) as fecha_max,
    AVG(numero_conversaciones) as promedio_conversaciones
FROM cat_prod_analytics_db.data;

-- Athena: Validar tokens analysis Bedrock
SELECT 
    DATE(fecha) as dia,
    COUNT(*) as conversaciones_bedrock,
    SUM(tokens_total) as tokens_totales_claude,
    SUM(costo_estimado_usd) as costo_diario_bedrock,
    modelo
FROM cat_prod_analytics_db.tokens_analysis 
GROUP BY DATE(fecha), modelo
ORDER BY dia DESC;
```

## ⚙️ Comandos de Gestión

### **📋 Comandos CDK Principales**

| Comando | Descripción | Uso |
|---------|-------------|-----|
| `npm run build` | Compilar TypeScript | Antes de deploy |
| `npm run watch` | Compilación automática | Desarrollo |
| `npx cdk synth` | Generar CloudFormation | Validación |
| `npx cdk deploy --all` | Desplegar todos los stacks | Deploy completo |
| `npx cdk diff <stack>` | Ver cambios pendientes | Pre-deploy |
| `npx cdk destroy --all` | Eliminar todos los recursos | Cleanup |

### **🔄 Operaciones por Stack**

```bash
# Deploy selectivo
npx cdk deploy cat-prod-normalize-stack    # Solo ETL-1
npx cdk deploy cat-prod-etl2-stack         # Solo ETL-2  
npx cdk deploy cat-prod-tokens-stack       # Solo Tokens

# Logs en tiempo real
aws logs tail /aws/lambda/cat-prod-lambda-normalize --follow
aws logs tail /aws-glue/jobs/cat-prod-etl2-parquet --follow

# Trigger manual de prueba
aws lambda invoke \
  --function-name cat-prod-lambda-normalize \
  --payload '{}' \
  response.json

# Estado del Glue Job
aws glue get-job-runs --job-name cat-prod-etl2-parquet

# Verificar Crawler
aws glue get-crawler --name curated-crawler
```

### **📊 Consultas Athena de Validación**

```sql
-- Verificar integridad datos ETL-1 → ETL-2
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'cat_prod_analytics_db'
ORDER BY table_name, ordinal_position;

-- KPIs del pipeline
SELECT 
    'Total Usuarios' as metrica,
    COUNT(DISTINCT usuario_id) as valor
FROM cat_prod_analytics_db.data
UNION ALL
SELECT 
    'Total Conversaciones',
    SUM(numero_conversaciones)
FROM cat_prod_analytics_db.data;
```

---
