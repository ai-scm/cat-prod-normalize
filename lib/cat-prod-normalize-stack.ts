import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';

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

    // 📤 Outputs para referencia
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
  }
}
