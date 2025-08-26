# 🚀 Cat Prod Normalize - CDK Project

Este proyecto CDK convierte el notebook de procesamiento de datos de Catia en una función Lambda serverless.

## 📋 Descripción

El proyecto toma el notebook `cat-prod-normalize-data.ipynb` que procesa datos de conversaciones del chatbot Catia almacenados en DynamoDB y lo convierte en una función Lambda que:

1. 📊 **Extrae datos** de DynamoDB (tabla: `cat-prod-catia-conversations-table`)
2. 🔗 **Procesa y normaliza** datos (merge de conversaciones y feedback)
3. 🔧 **Aplica filtros** (fechas, ciudades, usuarios válidos)
4. 💬 **Extrae preguntas** de las conversaciones en formato JSON
5. 🎯 **Clasifica feedback** (like/dislike/mixed)
6. 💬 **Extrae respuestas** (comments y options del feedback)
7. 📊 **Genera archivos CSV/Parquet** con 12 columnas para análisis
8. ☁️ **Sube archivos a S3** para procesamiento ETL-2

## 🏗️ Arquitectura

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   DynamoDB  │───▶│   Lambda    │───▶│     S3      │───▶│   Athena    │
│ Conversations│    │ ETL-1       │    │  Reports    │    │ Analysis    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                              │
                                              ▼
                                      ┌─────────────┐
                                      │  Glue ETL-2 │
                                      │  (Parquet)  │
                                      └─────────────┘
```

## 📁 Estructura del Proyecto

```
cat-prod-normalize/
├── notebook/                           # 📓 Notebook original
│   └── cat-prod-normalize-data.ipynb   # Notebook de Python a convertir
├── lambda/                             # 🐍 Código Lambda
│   ├── lambda_function.py              # Función principal
│   └── requirements.txt                # Dependencias Python
├── lib/                                # 📚 Definiciones CDK
│   └── cat-prod-normalize-stack.ts     # Stack principal
├── bin/                                # 🎯 Punto de entrada
│   └── cat-prod-normalize.ts           # App principal
├── cdk.json                           # ⚙️ Configuración CDK
├── package.json                       # 📦 Dependencias Node.js
└── README.md                          # 📖 Este archivo
```

## 🛠️ Recursos Creados

### 🐍 Lambda Function
- **Name**: `cat-prod-lambda-normalize-prod`
- **Runtime**: Python 3.9
- **Memoria**: 1024 MB
- **Timeout**: 15 minutos
- **Layer**: Dependencias Python incluidas (pandas, boto3, openpyxl, numpy)
- **Tags**: ProjectId: P0260, Env: PROD, Client: CAT

### 📦 S3 Bucket
- **Nombre**: `cat-prod-normalize-reports-prod`
- **Versionado**: Habilitado
- **Acceso público**: Bloqueado
- **Tags**: Aplicados automáticamente

### 📚 Lambda Layer
- **Nombre**: `cat-prod-lambda-deps-layer-prod`
- **Runtime**: Python 3.9 compatible
- **Contenido**: Todas las dependencias Python empaquetadas
- **Dependencias**: pandas==2.0.3, boto3==1.34.162, openpyxl==3.1.2, numpy==1.24.3

### 🔐 IAM Role
- **Nombre**: `cat-prod-lambda-normalize-role-prod`
- **DynamoDB**: Permisos de lectura en `cat-prod-catia-conversations-table`
- **S3**: Permisos de escritura en el bucket de reportes
- **CloudWatch**: Logs básicos (retención 2 años)

## 🚀 Instalación y Despliegue

### Prerrequisitos
- Node.js >= 18.0.0
- AWS CLI configurado con cuenta `081899001252`
- CDK CLI instalado (`npm install -g aws-cdk`)

### 1. Instalar dependencias
```bash
npm install
```

### 2. Configurar archivos (ya incluidos)
Los archivos de configuración están en `config/`:
- `accountConfig.json`: Cuenta AWS específica (081899001252)
- `config.json`: Namespace del proyecto (cat-prod)  
- `tags.json`: Tags estándar (ProjectId: P0260, Env: PROD, Client: CAT)

### 3. Compilar TypeScript
```bash
npm run build
```

### 4. Sintetizar template
```bash
npx cdk synth
```

### 5. Desplegar stack
```bash
npx cdk deploy
```

**Recursos creados con nomenclatura estándar:**
- 🐍 **Lambda**: `cat-prod-lambda-normalize-prod` (con Layer para dependencias)
- 📦 **S3**: `cat-prod-normalize-reports-prod`
- 🔐 **IAM Role**: `cat-prod-lambda-normalize-role-prod`
- 📚 **Layer**: `cat-prod-lambda-deps-layer-prod`

## 🔧 Configuración

### Variables de Entorno (Lambda)
- `S3_BUCKET_NAME`: Bucket donde se guardan los reportes
- `DYNAMODB_TABLE_NAME`: Tabla de DynamoDB source

### Configuración en CDK
```typescript
// En lib/cat-prod-normalize-stack.ts
const catProdNormalizeLambda = new lambda.Function(this, 'CatProdNormalizeLambda', {
  runtime: lambda.Runtime.PYTHON_3_9,
  handler: 'lambda_function.lambda_handler',
  code: lambda.Code.fromAsset('lambda'),
  timeout: cdk.Duration.minutes(15),
  memorySize: 1024,
  // ...
});
```

## 📊 Datos de Salida

La Lambda genera un archivo Excel con 12 columnas:

| Columna | Descripción |
|---------|-------------|
| `usuario_id` | ID único del usuario |
| `nombre` | Nombre del usuario |
| `gerencia` | Gerencia/ciudad del usuario |
| `ciudad` | Ciudad (igual que gerencia) |
| `fecha_primera_conversacion` | Fecha en formato DD/MM/YYYY |
| `numero_conversaciones` | Cantidad total de conversaciones |
| `conversacion_completa` | JSON completo de conversaciones |
| `feedback_total` | Datos brutos de feedback |
| `numero_feedback` | Cantidad de feedbacks dados |
| `pregunta_conversacion` | Preguntas extraídas del usuario |
| `feedback` | Clasificación: like/dislike/mixed |
| `respuesta_feedback` | Comments y options extraídos |

## 🧪 Testing

### Ejecutar tests
```bash
npm run test
```

### Test de la Lambda (local)
```bash
cd lambda
python -c "
import lambda_function
result = lambda_function.lambda_handler({}, {})
print(result)
"
```

## 🔍 Monitoreo

### CloudWatch Logs
```bash
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/CatProdNormalizeLambda"
```

### Métricas clave
- **Duration**: Tiempo de ejecución
- **Memory**: Uso de memoria
- **Errors**: Errores durante la ejecución
- **Invocations**: Número de invocaciones

## ⚙️ Comandos Útiles

| Comando | Descripción |
|---------|-------------|
| `npm run build` | Compilar TypeScript |
| `npm run watch` | Modo watch (recompila automáticamente) |
| `npm run test` | Ejecutar tests |
| `npx cdk deploy` | Desplegar stack |
| `npx cdk diff` | Ver diferencias con estado actual |
| `npx cdk synth` | Sintetizar CloudFormation |
| `npx cdk destroy` | Eliminar stack |

## 🚨 Consideraciones Importantes

### Límites de Lambda
- **Timeout máximo**: 15 minutos
- **Memoria máxima**: 1024 MB configurada
- **Payload de respuesta**: 6 MB máximo

### Costos Estimados
- **Lambda**: $0.20 por 1M de requests + tiempo de ejecución
- **S3**: $0.023 por GB/mes de almacenamiento
- **DynamoDB**: Según RCU consumidos (modo On-Demand)

### Optimizaciones Futuras
- 🔄 **EventBridge**: Programar ejecución automática
- 📈 **CloudWatch**: Alarmas personalizadas
- 🗜️ **Compresión**: Comprimir archivos Excel
- 🔄 **Step Functions**: Para procesos más complejos

## 📞 Soporte

Para dudas o problemas:
1. Revisar los logs de CloudWatch
2. Verificar permisos IAM
3. Comprobar configuración de variables de entorno

## 🔄 Próximos Pasos

Como indicaste, este es solo el stack básico. Más adelante podemos agregar:
- ⏰ EventBridge para programación automática
- 📧 SNS para notificaciones
- 🔍 CloudWatch Dashboards
- 🔄 Step Functions para workflows complejos
- 🌐 API Gateway para trigger HTTP

¡El notebook ha sido exitosamente convertido a Lambda! 🎉
