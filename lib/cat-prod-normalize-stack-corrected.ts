import * as cdk from 'aws-cdk-lib';
import * as path from 'path';
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
      quicksightDatasetId: 'Dataset_prueba',
      quicksightDatasetName: 'Dataset_prueba',
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

    // 📦 Reference public Lambda Layer for Python dependencies
    const pythonLayer = lambda.LayerVersion.fromLayerVersionArn(
      this, 
      'AWSSDKPandasLayer', 
      'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13'
    );

    // ⚡ Lambda ETL
    const etlLambda = this.createETLLambda(etlLambdaRole, reportsBucket, config, pythonLayer);

    // ⚡ Lambda QuickSight Updater
    const quicksightLambda = new lambda.Function(this, 'QuickSightUpdaterLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      functionName: 'cat-prod-quicksight-updater',
      handler: 'quicksight_updater.lambda_handler',
      code: lambda.Code.fromAsset('lambda-quicksight', {
        exclude: ['requirements.txt', '*.pyc', '__pycache__']
      }),
      layers: [pythonLayer],
      role: quicksightLambdaRole,
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: {
        QUICKSIGHT_DATASET_ID: config.quicksightDatasetId,
        QUICKSIGHT_DATASET_NAME: config.quicksightDatasetName,
        QUICKSIGHT_DASHBOARD_ID: 'Dashboard_prueba',
        AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
        S3_BUCKET_NAME: reportsBucket.bucketName
      },
      description: 'Función Lambda para actualizar dataset y dashboard de QuickSight automáticamente'
    });

    // 📤 Stack Outputs básicos
    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: etlLambda.functionName,
      description: 'Nombre de la función Lambda ETL'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: reportsBucket.bucketName,
      description: 'Nombre del bucket S3 para reportes'
    });

    new cdk.CfnOutput(this, 'QuickSightLambdaName', {
      value: quicksightLambda.functionName,
      description: 'Nombre de la función Lambda QuickSight Updater'
    });

    new cdk.CfnOutput(this, 'PythonLayerArn', {
      value: 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13',
      description: 'ARN del Lambda Layer público con dependencias Python (AWS SDK, Pandas, Numpy)'
    });
  }

  // 📦 Crear S3 Bucket
  private createS3Bucket(bucketName: string): s3.Bucket {
    return new s3.Bucket(this, 'CatProdNormalizeReportsBucket', {
      bucketName: bucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [{
        id: 'DeleteOldVersions',
        enabled: true,
        noncurrentVersionExpiration: cdk.Duration.days(30)
      }]
    });
  }

  // 🔐 Crear rol IAM para Lambda ETL
  private createETLLambdaRole(bucket: s3.Bucket, dynamoTableName: string): iam.Role {
    return new iam.Role(this, 'CatProdNormalizeETLLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ],
      inlinePolicies: {
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Scan', 'dynamodb:Query', 'dynamodb:GetItem'],
              resources: [`arn:aws:dynamodb:*:*:table/${dynamoTableName}`]
            })
          ]
        }),
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:PutObject', 's3:PutObjectAcl', 's3:GetObject'],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`]
            })
          ]
        })
      }
    });
  }

  // 🔐 Crear rol IAM para Lambda QuickSight
  private createQuickSightLambdaRole(bucket: s3.Bucket): iam.Role {
    return new iam.Role(this, 'CatProdNormalizeQuickSightLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
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
                'quicksight:DescribeDataSet',
                'quicksight:DescribeDashboard'
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
