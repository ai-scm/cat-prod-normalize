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

    // 📊 Configuración centralizada
    const config = {
      s3BucketName: 'cat-prod-normalize-reports',
      dynamoTableName: 'cat-prod-catia-conversations-table',
      etlLambdaName: 'cat-prod-lambda-normalize',
      quicksightDatasetId: 'Dashboard_Pruebas', // ← Nombre actualizado del dataset
      quicksightDatasetName: 'Dashboard_Pruebas',
      schedule: {
        minute: '0',
        hour: '13', // 8:00 AM Colombia = 1:00 PM UTC (UTC-5)
        cronExpression: 'cron(0 13 * * ? *)',
        description: 'Todos los días a las 8:00 AM Colombia (1:00 PM UTC)'
      }
    };

    // 📦 S3 Bucket para reportes
    const reportsBucket = this.createS3Bucket(config.s3BucketName);

    // 🔐 IAM Roles
    const etlLambdaRole = this.createETLLambdaRole(reportsBucket, config.dynamoTableName);
    const quicksightLambdaRole = this.createQuickSightLambdaRole(reportsBucket);

    // ⚡ Lambda Functions
    const etlLambda = this.createETLLambda(etlLambdaRole, reportsBucket, config);
    const quicksightLambda = this.createQuickSightLambda(quicksightLambdaRole, reportsBucket, config);

    // ⏰ EventBridge Schedule
    const scheduleRule = this.createScheduleRule(etlLambda, config);

    // 🔔 S3 Event Notifications
    this.createS3EventNotifications(reportsBucket, quicksightLambda);

    // 📤 Stack Outputs
    this.createOutputs(etlLambda, reportsBucket, scheduleRule, quicksightLambda, config);
  }

  // 📦 Crear S3 Bucket
  private createS3Bucket(bucketName: string): s3.Bucket {
    return new s3.Bucket(this, 'CatProdNormalizeReportsBucket', {
      bucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });
  }

  // 🔐 Crear IAM Role para Lambda ETL
  private createETLLambdaRole(bucket: s3.Bucket, dynamoTableName: string): iam.Role {
    return new iam.Role(this, 'CatProdNormalizeLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:BatchGetItem', 'dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan'],
              resources: [`arn:aws:dynamodb:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/${dynamoTableName}`],
            }),
          ],
        }),
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:PutObjectAcl'],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
            }),
          ],
        }),
      },
    });
  }

  // 🔐 Crear IAM Role para Lambda QuickSight
  private createQuickSightLambdaRole(bucket: s3.Bucket): iam.Role {
    return new iam.Role(this, 'QuickSightUpdaterLambdaRole', {
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
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`]
            })
          ]
        })
      }
    });
  }

  // ⚡ Crear Lambda ETL principal
  private createETLLambda(role: iam.Role, bucket: s3.Bucket, config: any): lambda.Function {
    return new lambda.Function(this, 'CatProdNormalizeLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      functionName: config.etlLambdaName,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset('lambda'),
      role,
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      environment: {
        S3_BUCKET_NAME: bucket.bucketName,
        DYNAMODB_TABLE_NAME: config.dynamoTableName
      },
      description: 'Función Lambda para normalizar y procesar datos de conversaciones de Catia'
    });
  }

  // ⚡ Crear Lambda QuickSight Updater
  private createQuickSightLambda(role: iam.Role, bucket: s3.Bucket, config: any): lambda.Function {
    return new lambda.Function(this, 'QuickSightUpdaterLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'quicksight_updater.lambda_handler',
      code: lambda.Code.fromAsset('lambda-quicksight'),
      role,
      timeout: cdk.Duration.minutes(5),
      memorySize: 256,
      environment: {
        QUICKSIGHT_DATASET_ID: config.quicksightDatasetId,
        QUICKSIGHT_DATASET_NAME: config.quicksightDatasetName,
        AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
        S3_BUCKET_NAME: bucket.bucketName
      },
      description: `Actualiza dataset ${config.quicksightDatasetName} cuando se sube archivo Excel a S3`
    });
  }

  // ⏰ Crear EventBridge Schedule Rule
  private createScheduleRule(etlLambda: lambda.Function, config: any): events.Rule {
    const rule = new events.Rule(this, 'DailyETLScheduleRule', {
      ruleName: 'cat-prod-etl-daily-schedule',
      description: `Ejecuta el proceso ETL de Catia ${config.schedule.description}`,
      schedule: events.Schedule.cron({
        minute: config.schedule.minute,
        hour: config.schedule.hour,
        day: '*',
        month: '*',
        year: '*'
      }),
      enabled: true
    });

    // 🎯 Configurar Lambda como target
    rule.addTarget(new targets.LambdaFunction(etlLambda, {
      event: events.RuleTargetInput.fromObject({
        source: 'aws.events',
        'detail-type': 'Scheduled Event',
        detail: {
          description: `Ejecución programada diaria del ETL de Catia - ${config.schedule.description}`
        }
      })
    }));

    // 🔐 Permisos para EventBridge
    etlLambda.addPermission('AllowEventBridgeInvoke', {
      principal: new iam.ServicePrincipal('events.amazonaws.com'),
      sourceArn: rule.ruleArn,
      action: 'lambda:InvokeFunction'
    });

    return rule;
  }

  // 🔔 Crear S3 Event Notifications
  private createS3EventNotifications(bucket: s3.Bucket, quicksightLambda: lambda.Function): void {
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(quicksightLambda),
      { prefix: 'reports/', suffix: '.xlsx' }
    );
  }

  // 📤 Crear Stack Outputs
  private createOutputs(
    etlLambda: lambda.Function,
    bucket: s3.Bucket,
    scheduleRule: events.Rule,
    quicksightLambda: lambda.Function,
    config: any
  ): void {
    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: etlLambda.functionName,
      description: 'Nombre de la función Lambda ETL'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: bucket.bucketName,
      description: 'Nombre del bucket S3 para reportes'
    });

    new cdk.CfnOutput(this, 'LambdaFunctionArn', {
      value: etlLambda.functionArn,
      description: 'ARN de la función Lambda ETL'
    });

    new cdk.CfnOutput(this, 'EventBridgeRuleName', {
      value: scheduleRule.ruleName,
      description: 'Nombre de la regla de EventBridge para ejecución diaria'
    });

    new cdk.CfnOutput(this, 'ScheduleExpression', {
      value: config.schedule.cronExpression,
      description: `Expresión cron: ${config.schedule.description}`
    });

    new cdk.CfnOutput(this, 'QuickSightUpdaterLambdaName', {
      value: quicksightLambda.functionName,
      description: 'Nombre de la función Lambda que actualiza QuickSight'
    });

    new cdk.CfnOutput(this, 'QuickSightDatasetId', {
      value: config.quicksightDatasetId,
      description: 'ID del dataset de QuickSight que se actualiza automáticamente'
    });
  }
}
