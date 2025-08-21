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
  constructor(scope: Construct, id: string, props: CatProdNormalizeStackProps) {
    super(scope, id, props);

    const { namespace, tags = {} } = props;
    const env = tags.Env?.toLowerCase() || 'prod';
    const region = cdk.Stack.of(this).region;
    const account = cdk.Stack.of(this).account;

    // 🏷️ Crear nombres de recursos con nomenclatura estándar
    const bucketName = `${namespace}-normalize-reports`;
    const lambdaName = `${namespace}-lambda-normalize`;
    const roleName = `${namespace}-lambda-normalize-role`;
    const layerName = `${namespace}-lambda-deps-layer`;

    // 📦 S3 Bucket para almacenar los reportes generados
    const reportsBucket = new s3.Bucket(this, 'CatProdNormalizeReportsBucket', {
      bucketName: bucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN, // Mantener el bucket al eliminar el stack
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    // 🐍 Lambda Layer para dependencias Python
    const pythonDepsLayer = new lambda.LayerVersion(this, 'CatProdNormalizePythonDepsLayer', {
      layerVersionName: layerName,
      code: lambda.Code.fromAsset('lambda', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: [
            'bash', '-c',
            [
              'pip install -r requirements.txt -t /asset-output/python',
              'cp -r . /asset-output/python'
            ].join(' && ')
          ],
        },
      }),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      description: 'Dependencias Python para función Lambda de normalización de datos Catia',
    });

    // 🔐 IAM Role para la función Lambda
    const lambdaRole = new iam.Role(this, 'CatProdNormalizeLambdaRole', {
      roleName: roleName,
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
      functionName: lambdaName,
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_function.lambda_handler',
      code: lambda.Code.fromAsset('lambda', {
        exclude: ['requirements.txt', '*.pyc', '__pycache__']
      }),
      layers: [pythonDepsLayer],
      role: lambdaRole,
      timeout: cdk.Duration.minutes(5), // Aumentar timeout para procesamiento completo
      memorySize: 1024, // 1 GB de memoria para procesamiento de datos
      environment: {
        S3_BUCKET_NAME: reportsBucket.bucketName,
        DYNAMODB_TABLE_NAME: `${namespace}-catia-conversations-table`,
        PROJECT_ID: tags.ProjectId || 'P0260',
        ENVIRONMENT: tags.Env || 'PROD',
        CLIENT: tags.Client || 'CAT'
      },
      description: `Función Lambda para normalizar y procesar datos de conversaciones de Catia - ${tags.ProjectId || 'P0000'}`
    });

    // ⏰ EventBridge Rule para ejecutar Lambda diariamente a medianoche
    const dailyScheduleRule = new events.Rule(this, 'CatProdNormalizeDailySchedule', {
      ruleName: `${namespace}-daily-normalize-schedule`,
      description: 'Ejecuta la Lambda de normalización de datos Catia diariamente a las 12:00 AM (medianoche) UTC',
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '0',
        day: '*',
        month: '*',
        year: '*'
      }),
      enabled: true
    });

    // 🎯 Agregar la función Lambda como target del evento programado
    dailyScheduleRule.addTarget(new targets.LambdaFunction(catProdNormalizeLambda, {
      event: events.RuleTargetInput.fromObject({
        source: 'eventbridge-schedule',
        timestamp: events.EventField.fromPath('$.time'),
        detail: 'Daily automated data normalization execution'
      })
    }));

    // 🏷️ Aplicar tags a todos los recursos
    if (tags && Object.keys(tags).length > 0) {
      Object.keys(tags).forEach(key => {
        cdk.Tags.of(reportsBucket).add(key, tags[key]);
        cdk.Tags.of(pythonDepsLayer).add(key, tags[key]);
        cdk.Tags.of(lambdaRole).add(key, tags[key]);
        cdk.Tags.of(catProdNormalizeLambda).add(key, tags[key]);
        cdk.Tags.of(dailyScheduleRule).add(key, tags[key]);
      });
    }

    // 📤 Outputs para referencia
    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: catProdNormalizeLambda.functionName,
      description: 'Nombre de la función Lambda',
      exportName: `${namespace}-lambda-normalize-name`
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: reportsBucket.bucketName,
      description: 'Nombre del bucket S3 para reportes',
      exportName: `${namespace}-s3-normalize-bucket-name`
    });

    new cdk.CfnOutput(this, 'LambdaFunctionArn', {
      value: catProdNormalizeLambda.functionArn,
      description: 'ARN de la función Lambda',
      exportName: `${namespace}-lambda-normalize-arn`
    });

    new cdk.CfnOutput(this, 'LambdaLayerArn', {
      value: pythonDepsLayer.layerVersionArn,
      description: 'ARN de la Lambda Layer con dependencias Python',
      exportName: `${namespace}-lambda-layer-normalize-arn`
    });

    new cdk.CfnOutput(this, 'ScheduleRuleName', {
      value: dailyScheduleRule.ruleName,
      description: 'Nombre de la regla de EventBridge para ejecución diaria',
      exportName: `${namespace}-schedule-rule-name`
    });
  }
}
