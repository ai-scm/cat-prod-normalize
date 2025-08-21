import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';

export class CatProdNormalizeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 📦 S3 Bucket para almacenar los reportes generados
    const reportsBucket = new s3.Bucket(this, 'CatProdNormalizeReportsBucket', {
      bucketName: 'cat-prod-normalize-reports',
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN, // Mantener el bucket al eliminar el stack
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    //  IAM Role para la función Lambda
    const lambdaRole = new iam.Role(this, 'CatProdNormalizeLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'dynamodb:Scan',
                'dynamodb:Query',
                'dynamodb:GetItem',
                'dynamodb:BatchGetItem'
              ],
              resources: [
                'arn:aws:dynamodb:us-east-1:*:table/cat-prod-catia-conversations-table'
              ]
            })
          ]
        }),
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:PutObject',
                's3:PutObjectAcl',
                's3:GetObject'
              ],
              resources: [
                reportsBucket.bucketArn,
                `${reportsBucket.bucketArn}/*`
              ]
            })
          ]
        })
      }
    });

    // 🐍 Función Lambda para procesar datos de Catia
    const catProdNormalizeLambda = new lambda.Function(this, 'CatProdNormalizeLambda', {
      functionName: 'cat-prod-lambda-normalize', // ← Nombre específico de la Lambda
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      role: lambdaRole,
      timeout: cdk.Duration.minutes(1), // 15 minutos máximo
      memorySize: 1024, // 1 GB de memoria para procesamiento de datos
      environment: {
        S3_BUCKET_NAME: reportsBucket.bucketName,
        DYNAMODB_TABLE_NAME: 'cat-prod-catia-conversations-table'
      },
      description: 'Función Lambda para normalizar y procesar datos de conversaciones de Catia'
    });

    // � EventBridge Rule para ejecutar la Lambda todos los días a las 8:00 AM (UTC-5 Colombia = 1:00 PM UTC)
    const dailyScheduleRule = new events.Rule(this, 'DailyETLScheduleRule', {
      ruleName: 'cat-prod-etl-daily-schedule',
      description: 'Ejecuta el proceso ETL de Catia todos los días a las 8:00 AM Colombia',
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '13', // 8:00 AM Colombia = 1:00 PM UTC (UTC-5)
        day: '*',
        month: '*',
        year: '*'
      }),
      enabled: true
    });

    // 🎯 Configurar la Lambda como target del EventBridge
    dailyScheduleRule.addTarget(new targets.LambdaFunction(catProdNormalizeLambda, {
      event: events.RuleTargetInput.fromObject({
        source: 'aws.events',
        'detail-type': 'Scheduled Event',
        detail: {
          description: 'Ejecución programada diaria del ETL de Catia - 8:00 AM Colombia'
        }
      })
    }));

    // 🔐 Permisos para que EventBridge pueda invocar la Lambda
    catProdNormalizeLambda.addPermission('AllowEventBridgeInvoke', {
      principal: new iam.ServicePrincipal('events.amazonaws.com'),
      sourceArn: dailyScheduleRule.ruleArn,
      action: 'lambda:InvokeFunction'
    });

    // 🔐 IAM Role para Lambda QuickSight Updater
    const quicksightLambdaRole = new iam.Role(this, 'QuickSightUpdaterLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        QuickSightAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'quicksight:CreateIngestion',
                'quicksight:DescribeIngestion',
                'quicksight:ListIngestions',
                'quicksight:DescribeDataSet'
              ],
              resources: ['*']
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:ListBucket'],
              resources: [reportsBucket.bucketArn, `${reportsBucket.bucketArn}/*`]
            })
          ]
        })
      }
    });

    // ⚡ Lambda Function para actualizar QuickSight
    const quicksightUpdaterLambda = new lambda.Function(this, 'QuickSightUpdaterLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'quicksight_updater.lambda_handler',
      code: lambda.Code.fromAsset('lambda-quicksight'),
      role: quicksightLambdaRole,
      timeout: cdk.Duration.minutes(5),
      memorySize: 256,
      environment: {
        QUICKSIGHT_DATASET_ID: 'Dataset_prueba',
        QUICKSIGHT_DATASET_NAME: 'Dataset_prueba',
        AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
        S3_BUCKET_NAME: reportsBucket.bucketName
      },
      description: 'Actualiza dataset Dataset_prueba cuando se sube archivo Excel a S3'
    });

    // 🔔 Configurar S3 Event Notifications para trigger QuickSight
    reportsBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(quicksightUpdaterLambda),
      { prefix: 'reports/', suffix: '.xlsx' }
    );

    // �📤 Outputs para referencia
    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: catProdNormalizeLambda.functionName,
      description: 'Nombre de la función Lambda'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: reportsBucket.bucketName,
      description: 'Nombre del bucket S3 para reportes'
    });

    new cdk.CfnOutput(this, 'LambdaFunctionArn', {
      value: catProdNormalizeLambda.functionArn,
      description: 'ARN de la función Lambda'
    });

    new cdk.CfnOutput(this, 'EventBridgeRuleName', {
      value: dailyScheduleRule.ruleName,
      description: 'Nombre de la regla de EventBridge para ejecución diaria'
    });

    new cdk.CfnOutput(this, 'ScheduleExpression', {
      value: 'cron(0 13 * * ? *)',
      description: 'Expresión cron: Todos los días a las 8:00 AM Colombia (1:00 PM UTC)'
    });

    new cdk.CfnOutput(this, 'QuickSightUpdaterLambdaName', {
      value: quicksightUpdaterLambda.functionName,
      description: 'Nombre de la función Lambda que actualiza QuickSight'
    });

    new cdk.CfnOutput(this, 'QuickSightDatasetName', {
      value: 'Dataset_prueba',
      description: 'Nombre del dataset de QuickSight que se actualiza automáticamente'
    });
  }
}
