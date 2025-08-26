import { Construct } from "constructs";
import {
  aws_glue as glue,
  aws_iam as iam,
  aws_s3 as s3,
  aws_events as events,
  aws_events_targets as targets,
  CfnOutput,
} from "aws-cdk-lib";
import * as cdk from 'aws-cdk-lib';

export interface CatalogProps {
  dataBucket: s3.IBucket;
  curatedPrefix: string;   // "curated/"
  databaseName: string;    // "analytics_db"
  crawlerName?: string;    // "curated-crawler"
  glueJobName: string;     // Nombre del Glue Job ETL-2 para trigger
}

export class CatalogConstruct extends Construct {
  public readonly databaseName: string;

  constructor(scope: Construct, id: string, props: CatalogProps) {
    super(scope, id);

    const {
      dataBucket,
      curatedPrefix,
      databaseName,
      crawlerName = "curated-crawler",
      glueJobName,
    } = props;

    // Database L1
    const accountId = cdk.Stack.of(this).account || process.env.CDK_DEFAULT_ACCOUNT || '000000000000';
    const db = new glue.CfnDatabase(this, "AnalyticsDb", {
      catalogId: accountId,
      databaseInput: { name: databaseName },
    });

    // Role del Crawler
    const crawlerRole = new iam.Role(this, "CrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      description: "Role for Glue Crawler to read curated/ parquet",
    });
    crawlerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
    );
    dataBucket.grantRead(crawlerRole, `${curatedPrefix}*`);

    // Crawler sobre curated/ - SOLO carpeta data.parquet
    const crawler = new glue.CfnCrawler(this, "CuratedCrawler", {
      name: crawlerName,
      role: crawlerRole.roleArn,
      databaseName: databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${dataBucket.bucketName}/${curatedPrefix}data.parquet/`,
            // 🎯 Apunta específicamente a la carpeta del Parquet, no a scripts
          },
        ],
      },
      schemaChangePolicy: {
        deleteBehavior: "LOG",
        updateBehavior: "UPDATE_IN_DATABASE",
      },
      // 🚫 Sin schedule - se ejecuta por eventos
    }).addDependency(db);

    // 🎯 EventBridge Rule: Trigger crawler cuando ETL-2 Job completa exitosamente
    const crawlerTriggerRule = new events.Rule(this, "CrawlerTriggerRule", {
      description: "Trigger crawler when ETL-2 Glue Job completes successfully",
      eventPattern: {
        source: ["aws.glue"],
        detailType: ["Glue Job State Change"],
        detail: {
          jobName: [glueJobName],
          state: ["SUCCEEDED"]
        }
      }
    });

    // 🎯 Target: Ejecutar crawler
    crawlerTriggerRule.addTarget(new targets.AwsApi({
      service: "Glue",
      action: "startCrawler",
      parameters: {
        Name: crawlerName
      },
      policyStatement: new iam.PolicyStatement({
        actions: ["glue:StartCrawler"],
        resources: [`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:crawler/${crawlerName}`]
      })
    }));

    this.databaseName = databaseName;

    // 📊 Outputs informativos
    new CfnOutput(this, "GlueDatabaseName", { value: databaseName });
    new CfnOutput(this, "CrawlerName", { value: crawlerName });
    new CfnOutput(this, "CrawlerTriggerInfo", { 
      value: `Crawler se ejecuta automáticamente cuando Job '${glueJobName}' completa exitosamente`,
      description: "Información del trigger automático del crawler"
    });
  }
}
