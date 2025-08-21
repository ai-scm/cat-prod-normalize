# 🔍 Validación del Dataset de QuickSight - Guía Completa

## 📊 **Puntos de validación implementados**

### ✅ **1. Validación automática en AWS CloudWatch Logs**

#### **A. Logs de la Lambda ETL principal:**
```bash
# Ver logs de la Lambda que genera el Excel
aws logs tail /aws/lambda/cat-prod-lambda-normalize --follow

# Logs que deberías ver:
# ✅ Archivo subido a S3: s3://cat-prod-normalize-reports/reports/Dashboard_Usuarios_Catia_20250821_0800_PROCESADO_COMPLETO.xlsx
```

#### **B. Logs de la Lambda QuickSight Updater:**
```bash
# Ver logs de la Lambda que actualiza QuickSight
aws logs tail /aws/lambda/QuickSightUpdaterLambda --follow

# Logs que deberías ver:
# 🚀 Iniciando actualización de QuickSight dataset: reporte_conversaciones_catia_prod_20250815(Datos2)
# 📁 Archivo detectado: s3://cat-prod-normalize-reports/reports/Dashboard_Usuarios_Catia_20250821_0800_PROCESADO_COMPLETO.xlsx
# ✅ Ingestion creada exitosamente: etl-refresh-20250821-080015
# 📊 Estado del refresh: INITIALIZED/RUNNING/COMPLETED
```

### ✅ **2. Validación en S3 (Archivos generados)**

```bash
# Verificar que se están generando archivos diariamente
aws s3 ls s3://cat-prod-normalize-reports/reports/ --recursive --human-readable

# Ejemplo de output esperado:
# 2025-08-21 08:00:15  152.6 KiB reports/Dashboard_Usuarios_Catia_20250821_0800_PROCESADO_COMPLETO.xlsx
# 2025-08-22 08:00:12  148.3 KiB reports/Dashboard_Usuarios_Catia_20250822_0800_PROCESADO_COMPLETO.xlsx
```

### ✅ **3. Validación en QuickSight Console**

#### **A. Verificar estado del dataset:**
1. **🔗 Ir a QuickSight Console**: https://quicksight.aws.amazon.com/
2. **📊 Datasets** → `reporte_conversaciones_catia_prod_20250815(Datos2)`
3. **🔍 Ver "Last refresh"** y "Refresh status"
4. **✅ Verificar fecha/hora** de última actualización

#### **B. Revisar historial de refresh:**
```
Datasets → Tu dataset → Refresh tab
- Refresh ID: etl-refresh-20250821-080015
- Status: Completed ✅
- Started: 21/08/2025 08:00:45
- Completed: 21/08/2025 08:01:12
- Rows processed: 128
```

### ✅ **4. Validación mediante API de QuickSight**

```bash
# Verificar estado del dataset
aws quicksight describe-data-set \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --data-set-id reporte_conversaciones_catia_prod_20250815

# Listar ingestions recientes
aws quicksight list-ingestions \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --data-set-id reporte_conversaciones_catia_prod_20250815

# Ver detalles de una ingestion específica
aws quicksight describe-ingestion \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --data-set-id reporte_conversaciones_catia_prod_20250815 \
  --ingestion-id etl-refresh-20250821-080015
```

### ✅ **5. Validación en EventBridge**

```bash
# Verificar que la regla está activa
aws events describe-rule --name cat-prod-etl-daily-schedule

# Ver histórico de ejecuciones
aws events list-rule-names-by-target \
  --target-arn $(aws lambda get-function --function-name cat-prod-lambda-normalize --query Configuration.FunctionArn --output text)

# Verificar métricas de EventBridge
aws cloudwatch get-metric-statistics \
  --namespace AWS/Events \
  --metric-name SuccessfulInvocations \
  --dimensions Name=RuleName,Value=cat-prod-etl-daily-schedule \
  --start-time $(date -d '7 days ago' -u +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 86400 \
  --statistics Sum
```

## 🕐 **Cronograma de validación diaria**

### **8:00 AM Colombia (1:00 PM UTC):**
1. **⚡ EventBridge** ejecuta Lambda ETL
2. **📊 Lambda** procesa DynamoDB → genera Excel
3. **☁️ S3** recibe archivo nuevo
4. **🔔 S3 Event** dispara Lambda QuickSight
5. **📈 QuickSight** dataset se actualiza automáticamente

### **Tiempos estimados:**
- **ETL Process**: 2-5 minutos
- **S3 Upload**: 10-30 segundos  
- **QuickSight Refresh**: 1-3 minutos
- **🎯 Total**: 4-9 minutos

## 📊 **Dashboard de monitoreo (CloudWatch)**

### **Métricas clave a monitorear:**

#### **Lambda ETL Principal:**
- **Duration**: < 300 segundos (5 min)
- **Errors**: = 0
- **Memory utilization**: < 80%
- **Concurrent executions**: = 1

#### **Lambda QuickSight Updater:**
- **Duration**: < 60 segundos (1 min)
- **Errors**: = 0
- **Invocations**: = 1 por día

#### **S3 Bucket:**
- **NumberOfObjects**: Incremental diario
- **BucketSizeBytes**: Crecimiento diario

#### **QuickSight (via API):**
- **Ingestion Status**: COMPLETED
- **Ingestion Duration**: < 180 segundos
- **Rows Ingested**: > 0

## 🚨 **Alertas y troubleshooting**

### **Posibles problemas y validaciones:**

#### **❌ Error 1: Lambda ETL falla**
```bash
# Validar logs
aws logs filter-log-events \
  --log-group-name /aws/lambda/cat-prod-lambda-normalize \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000

# Validar permisos DynamoDB
aws dynamodb describe-table --table-name cat-prod-catia-conversations-table
```

#### **❌ Error 2: Archivo no se sube a S3**
```bash
# Verificar permisos S3
aws s3api get-bucket-policy --bucket cat-prod-normalize-reports

# Validar que la carpeta reports/ existe
aws s3 ls s3://cat-prod-normalize-reports/
```

#### **❌ Error 3: QuickSight no se actualiza**
```bash
# Verificar permisos QuickSight
aws quicksight describe-user \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --namespace default \
  --user-name $(aws sts get-caller-identity --query Arn --output text | cut -d'/' -f2)

# Validar que el dataset existe
aws quicksight list-data-sets \
  --aws-account-id $(aws sts get-caller-identity --query Account --output text) \
  --query 'DataSetSummaries[?contains(Name, `reporte_conversaciones_catia`)]'
```

#### **❌ Error 4: EventBridge no ejecuta**
```bash
# Verificar estado de la regla
aws events describe-rule --name cat-prod-etl-daily-schedule --query State

# Ver targets de la regla
aws events list-targets-by-rule --rule cat-prod-etl-daily-schedule
```

## 🧪 **Testing manual**

### **Ejecutar proceso completo manualmente:**

```bash
# 1. Ejecutar Lambda ETL manualmente
aws lambda invoke \
  --function-name cat-prod-lambda-normalize \
  --payload '{"source":"manual-test","detail-type":"Manual Execution"}' \
  /tmp/etl-response.json

# 2. Verificar que se creó archivo en S3
aws s3 ls s3://cat-prod-normalize-reports/reports/ --recursive

# 3. Ejecutar Lambda QuickSight manualmente (simular S3 event)
aws lambda invoke \
  --function-name QuickSightUpdaterLambda \
  --payload '{
    "Records": [{
      "eventSource": "aws:s3",
      "s3": {
        "bucket": {"name": "cat-prod-normalize-reports"},
        "object": {"key": "reports/Dashboard_Usuarios_Catia_20250821_0800_PROCESADO_COMPLETO.xlsx"}
      }
    }]
  }' \
  /tmp/quicksight-response.json

# 4. Ver resultado
cat /tmp/quicksight-response.json | jq .
```

## 📈 **Métricas de éxito**

### **KPIs diarios:**
- ✅ **ETL Success Rate**: 100%
- ✅ **S3 Upload Success**: 100%  
- ✅ **QuickSight Refresh Success**: 100%
- ✅ **Data Freshness**: < 30 minutos desde creación
- ✅ **Error Rate**: 0%

### **Validación semanal:**
- 📊 **7 archivos Excel** generados en S3
- 🔄 **7 refreshes exitosos** en QuickSight
- 📈 **Datos actualizados** en dashboards
- 🕐 **Ejecución puntual** a las 8:00 AM cada día

## 🎯 **Próximos pasos de validación**

1. **🚀 Desplegar el stack** completo
2. **⏰ Esperar primera ejecución** (mañana 8:00 AM)
3. **🔍 Monitorear logs** en tiempo real
4. **✅ Verificar dataset** en QuickSight Console
5. **📊 Validar dashboards** actualizados
6. **🔧 Ajustar configuración** si es necesario

¿Todo claro para el monitoreo y validación? 🚀
