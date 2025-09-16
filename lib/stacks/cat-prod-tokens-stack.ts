import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

interface CatProdTokensStackProps extends cdk.StackProps {
  namespace: string;
  tags?: Record<string, string>;
}

export class CatProdTokensStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: CatProdTokensStackProps) {
    super(scope, id, props);

    // 📊 Configuración centralizada
    const config = {
      s3BucketName: 'cat-prod-normalize-reports',
      dynamoTableName: 'BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF', // Tabla existente
      tokensLambdaName: 'cat-prod-lambda-tokens',
      layerName: 'cat-prod-pandas-numpy-layer',
      schedule: {
        minute: '30',
        hour: '4', // 11:30 PM Colombia = 4:30 AM UTC (UTC-5)
        cronExpression: 'cron(30 4 * * ? *)',
        description: 'Todos los días a las 11:30 PM Colombia (4:30 AM UTC)'
      },
      athena: {
        database: 'cat_prod_analytics_db',
        workgroup: 'wg-cat-prod-analytics',
        outputLocation: 's3://cat-prod-normalize-reports/athena/results/'
      }
    };

    // 🏷️ Usar bucket S3 existente
    const reportsBucket = s3.Bucket.fromBucketName(
      this, 
      'ExistingReportsBucket', 
      config.s3BucketName
    );

    // 🔐 IAM Roles para la Lambda de Tokens
    const tokensLambdaRole = this.createTokensLambdaRole(
      reportsBucket, 
      config.dynamoTableName,
      config.athena.database,
      config.athena.workgroup
    );

    // 📦 Crear Lambda Layer para dependencias de Python
    const pythonLayer = this.createPythonLayer(config.layerName);

    // ⚡ Lambda de Tokens
    const tokensLambda = this.createTokensLambda(
      tokensLambdaRole, 
      reportsBucket, 
      config, 
      pythonLayer
    );

    // 🏷️ Tags básicos
    cdk.Tags.of(tokensLambda).add('Name', config.tokensLambdaName);
    cdk.Tags.of(tokensLambda).add('Component', 'lambda-function');
    cdk.Tags.of(tokensLambda).add('Purpose', 'tokens-analysis');
    cdk.Tags.of(tokensLambda).add('Runtime', 'python3.9');

    // 🏷️ Tags para Cost Explorer y Billing
    cdk.Tags.of(tokensLambda).add('BillingTag', 'TOKENS-LAMBDA');
    cdk.Tags.of(tokensLambda).add('CostCenter', 'DATA-ANALYTICS');
    cdk.Tags.of(tokensLambda).add('Project', 'CAT-PROD-TOKENS');
    cdk.Tags.of(tokensLambda).add('Environment', 'PROD');
    cdk.Tags.of(tokensLambda).add('ETLComponent', 'TOKENS-ANALYSIS');
    cdk.Tags.of(tokensLambda).add('DataSource', 'DynamoDB');
    cdk.Tags.of(tokensLambda).add('DataTarget', 'S3-CSV');
    cdk.Tags.of(tokensLambda).add('Owner', 'DataEngineering');
    cdk.Tags.of(tokensLambda).add('BusinessUnit', 'CATIA-OPERATIONS');
    cdk.Tags.of(tokensLambda).add('ResourceType', 'COMPUTE');

    // 🕐 EventBridge Schedule - Ejecutar Tokens diariamente a las 11:30 PM Colombia
    const dailyTokensSchedule = new events.Rule(this, 'DailyTokensSchedule', {
      ruleName: 'cat-prod-daily-tokens-schedule',
      description: config.schedule.description,
      schedule: events.Schedule.expression(config.schedule.cronExpression),
      enabled: true
    });

    // Agregar el Lambda Tokens como target del schedule
    dailyTokensSchedule.addTarget(new targets.LambdaFunction(tokensLambda, {
      maxEventAge: cdk.Duration.hours(2),
      retryAttempts: 2
    }));

    // 🏷️ Tags para EventBridge Schedule
    cdk.Tags.of(dailyTokensSchedule).add('BillingTag', 'TOKENS-SCHEDULER');
    cdk.Tags.of(dailyTokensSchedule).add('CostCenter', 'DATA-ANALYTICS');
    cdk.Tags.of(dailyTokensSchedule).add('Project', 'CAT-PROD-TOKENS');
    cdk.Tags.of(dailyTokensSchedule).add('Environment', 'PROD');
    cdk.Tags.of(dailyTokensSchedule).add('ETLComponent', 'TOKENS-SCHEDULER');
    cdk.Tags.of(dailyTokensSchedule).add('ResourceType', 'EVENT-ORCHESTRATION');

    // 📤 Stack Outputs
    new cdk.CfnOutput(this, 'TokensLambdaFunctionName', {
      value: tokensLambda.functionName,
      description: 'Nombre de la función Lambda de análisis de tokens'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: reportsBucket.bucketName,
      description: 'Nombre del bucket S3 para reportes (existente)'
    });

    new cdk.CfnOutput(this, 'TokensPythonLayerArn', {
      value: pythonLayer.layerVersionArn,
      description: 'ARN del Lambda Layer con dependencias Python (pandas, boto3, numpy)'
    });

    new cdk.CfnOutput(this, 'DailyTokensScheduleRule', {
      value: dailyTokensSchedule.ruleName,
      description: 'EventBridge Rule para ejecutar análisis de tokens diariamente'
    });

    new cdk.CfnOutput(this, 'AutomationWorkflow', {
      value: 'EventBridge Schedule → Lambda Tokens → S3 → Athena',
      description: 'Flujo de automatización configurado'
    });
  }

  // 🔐 Crear IAM Role para Lambda de Tokens
  private createTokensLambdaRole(
    bucket: s3.IBucket, 
    dynamoTableName: string,
    athenaDatabase: string,
    athenaWorkgroup: string
  ): iam.Role {
    return new iam.Role(this, 'CatProdTokensLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        // Acceso a DynamoDB para leer conversaciones
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan'],
              resources: [`arn:aws:dynamodb:*:*:table/${dynamoTableName}`],
            }),
          ],
        }),
        // Acceso a S3 para escribir resultados
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:ListBucket'],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
            }),
          ],
        }),
        // Acceso a Athena para crear vistas
        AthenaAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults'
              ],
              resources: ['*'],
            }),
            // Permisos para Glue Data Catalog
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:GetTable',
                'glue:GetPartitions',
                'glue:GetDatabase'
              ],
              resources: [
                `arn:aws:glue:*:*:catalog`,
                `arn:aws:glue:*:*:database/${athenaDatabase}`,
                `arn:aws:glue:*:*:table/${athenaDatabase}/*`
              ]
            })
          ],
        }),
      },
    });
  }

  // 📦 Crear Lambda Layer para dependencias Python
  private createPythonLayer(layerName: string): lambda.LayerVersion {
    return new lambda.LayerVersion(this, 'PythonDependenciesLayer', {
      layerVersionName: layerName,
      code: lambda.Code.fromAsset('lambda/tokens-process', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: [
            'bash', '-c', [
              'pip install -r requirements.txt -t /asset-output/python',
              'echo "Layer creada con dependencias: $(cat requirements.txt)"'
            ].join(' && ')
          ],
          user: 'root'
        }
      }),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      description: 'Layer con pandas, boto3 y numpy para análisis de tokens'
    });
  }

  // ⚡ Crear Lambda para análisis de tokens
  private createTokensLambda(
    role: iam.Role, 
    bucket: s3.IBucket, 
    config: any, 
    pythonLayer: lambda.LayerVersion
  ): lambda.Function {
    return new lambda.Function(this, 'CatProdTokensLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      functionName: config.tokensLambdaName,
      handler: 'tokens_lambda.lambda_handler',
      code: lambda.Code.fromAsset('lambda/tokens-process', {
        exclude: ['requirements.txt', '*.pyc', '__pycache__']
      }),
      layers: [pythonLayer],
      role: role,
      timeout: cdk.Duration.minutes(1),
      memorySize: 512,
      environment: {
        S3_BUCKET_NAME: config.s3BucketName,
        S3_OUTPUT_PREFIX: 'tokens-analysis/',
        DYNAMODB_TABLE_NAME: config.dynamoTableName,
        ATHENA_DATABASE: config.athena.database,
        ATHENA_WORKGROUP: config.athena.workgroup,
        ATHENA_OUTPUT_LOCATION: config.athena.outputLocation,
        PROJECT_ID: 'P0260',
        ENVIRONMENT: 'PROD',
        CLIENT: 'CAT'
      },
      description: 'Función Lambda para analizar uso de tokens en conversaciones de Catia'
    });
  }
}
