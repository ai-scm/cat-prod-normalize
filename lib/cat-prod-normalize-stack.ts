import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

interface CatProdNormalizeStackProps extends cdk.StackProps {
  namespace: string;
  tags?: Record<string, string>;
}

export class CatProdNormalizeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 📊 Configuración centralizada
    const config = {
      s3BucketName: 'cat-prod-normalize-reports',
      dynamoTableName: 'cat-prod-catia-conversations-table',
      etlLambdaName: 'cat-prod-lambda-normalize',
      schedule: {
  minute: '30',
  hour: '4', // 11:30 PM Colombia = 4:30 AM UTC (UTC-5)
  cronExpression: 'cron(30 4 * * ? *)',
  description: 'Todos los días a las 11:30 PM Colombia (4:30 AM UTC)'
      }
    };

    // 📦 S3 Bucket para reportes
    const reportsBucket = this.createS3Bucket(config.s3BucketName);

    // 🔐 IAM Roles
    const etlLambdaRole = this.createETLLambdaRole(reportsBucket, config.dynamoTableName);

    // 📦 Create Lambda Layer for Python dependencies using public ARN
    const pythonLayer = lambda.LayerVersion.fromLayerVersionArn(
      this, 
      'AWSSDKPandasLayer', 
      'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13'
    );

    // ⚡ Lambda ETL
    const etlLambda = this.createETLLambda(etlLambdaRole, reportsBucket, config, pythonLayer);

  // Apply tags to Lambda ETL and layer
    cdk.Tags.of(etlLambda).add('Name', 'cat-prod-lambda-normalize');
    cdk.Tags.of(etlLambda).add('Component', 'lambda-function');
    cdk.Tags.of(etlLambda).add('Purpose', 'data-processing');
    cdk.Tags.of(etlLambda).add('Runtime', 'python3.9');

  // 🕐 TRIGGER 1: EventBridge Schedule - Ejecutar ETL diariamente a las 11:30 PM Colombia
    const dailyETLSchedule = new events.Rule(this, 'DailyETLSchedule', {
      ruleName: 'cat-prod-daily-etl-schedule',
      description: config.schedule.description,
      schedule: events.Schedule.expression(config.schedule.cronExpression),
      enabled: true
    });

    // Agregar el Lambda ETL como target del schedule
    dailyETLSchedule.addTarget(new targets.LambdaFunction(etlLambda, {
      maxEventAge: cdk.Duration.hours(2),
      retryAttempts: 2
    }));


    // 📤 Stack Outputs básicos
    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: etlLambda.functionName,
      description: 'Nombre de la función Lambda ETL'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: reportsBucket.bucketName,
      description: 'Nombre del bucket S3 para reportes'
    });

    new cdk.CfnOutput(this, 'PythonLayerArn', {
      value: 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13',
      description: 'ARN del Lambda Layer público con dependencias Python (AWS SDK, Pandas, Numpy)'
    });

    new cdk.CfnOutput(this, 'DailyScheduleRule', {
      value: dailyETLSchedule.ruleName,
  description: 'EventBridge Rule para ejecutar ETL diariamente a las 11:30 PM Colombia'
    });

    new cdk.CfnOutput(this, 'AutomationWorkflow', {
      value: 'EventBridge Schedule → Lambda ETL → S3 Update',
      description: 'Flujo de automatización configurado (QuickSight removido)'
    });
  }

  // 📦 Crear S3 Bucket
  private createS3Bucket(bucketName: string): s3.Bucket {
    return new s3.Bucket(this, 'CatProdNormalizeReportsBucket', {
      bucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'DeleteOldVersions',
          enabled: true,
          noncurrentVersionExpiration: cdk.Duration.days(30)
        }
      ]
    });
  }

  // 🔐 Crear IAM Role para Lambda ETL
  private createETLLambdaRole(bucket: s3.Bucket, dynamoTableName: string): iam.Role {
    return new iam.Role(this, 'CatProdNormalizeETLLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan'],
              resources: [`arn:aws:dynamodb:*:*:table/${dynamoTableName}`],
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


  // ⚡ Crear Lambda ETL principal
  private createETLLambda(role: iam.Role, bucket: s3.Bucket, config: any, pythonLayer: lambda.ILayerVersion): lambda.Function {
    return new lambda.Function(this, 'CatProdNormalizeLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      functionName: config.etlLambdaName,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset('lambda', {
        exclude: ['requirements.txt', '*.pyc', '__pycache__']
      }),
      layers: [pythonLayer],
      role: role,
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      environment: {
        S3_BUCKET_NAME: bucket.bucketName,
        DYNAMODB_TABLE_NAME: config.dynamoTableName,
        PROJECT_ID: 'P0260',
        ENVIRONMENT: 'PROD',
        CLIENT: 'CAT'
      },
      description: 'Función Lambda para normalizar y procesar datos de conversaciones de Catia'
    });
  }
}
