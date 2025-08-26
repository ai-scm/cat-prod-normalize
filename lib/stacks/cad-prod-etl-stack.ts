import { Stack, StackProps, aws_s3 as s3, Tags } from "aws-cdk-lib";
import { Construct } from "constructs";
import { TransformJobConstruct } from "../constructs/transform-job-construct";
import { CatalogConstruct } from "../constructs/catalog-construct";
import { AthenaConstruct } from "../constructs/athena-construct";
import { OrchestratorConstruct } from "../constructs/orchestrator-construct";

export interface NewEtlStackProps extends StackProps {
  /** Nombre del bucket de datos ya existente (del ETL-1) */
  dataBucketName: string;
  /** Ruta S3 al script de Glue (opcional, usa Asset automático si no se especifica) */
  glueScriptS3Uri?: string; // ej: s3://<bucket>/scripts/cat-prod-normalize-data-glue.py
  /** Prefijos */
  cleanPrefix?: string;     // default "reports/etl-process1/" (CSV del ETL-1)
  curatedPrefix?: string;   // default "reports/etl-process2/" (Parquet del ETL-2)
  athenaResultsPrefix?: string; // default "athena/results/"
  /** Catalog/Athena names */
  glueDatabaseName?: string; // default "analytics_db"
  athenaWorkGroup?: string;  // default "wg-analytics"
}

export class NewEtlStack extends Stack {
  constructor(scope: Construct, id: string, props: NewEtlStackProps) {
    super(scope, id, props);

    const cleanPrefix = props.cleanPrefix ?? "reports/etl-process1/";
    const curatedPrefix = props.curatedPrefix ?? "reports/etl-process2/";
    const athenaResults = props.athenaResultsPrefix ?? "athena/results/";
    const dbName = props.glueDatabaseName ?? "analytics_db";
    const wgName = props.athenaWorkGroup ?? "wg-analytics";

    // Importa el bucket existente (no lo recrea)
    const dataBucket = s3.Bucket.fromBucketName(
      this,
      "DataLakeBucket",
      props.dataBucketName,
    );

    // 1) Glue Job ETL-2: Lee CSV de etl-process1/ y escribe Parquet único en etl-process2/
    const transform = new TransformJobConstruct(this, "TransformJob", {
      dataBucket,
      inputPrefix: cleanPrefix,   // reports/etl-process1/ (CSV del Lambda)
      outputPrefix: curatedPrefix, // reports/etl-process2/ (Parquet único, actualizado diariamente)
      scriptS3Uri: props.glueScriptS3Uri, // Opcional: usa Asset si no se especifica
      glueVersion: "4.0",
      numberOfWorkers: 2,
      workerType: "G.1X",
    });

    // 🏷️ Tags para Glue Job ETL-2
    Tags.of(transform).add('BillingTag', 'ETL-GLUE-ETL2');
    Tags.of(transform).add('CostCenter', 'DATA-ANALYTICS');
    Tags.of(transform).add('Project', 'CAT-PROD-NORMALIZE');
    Tags.of(transform).add('Environment', 'PROD');
    Tags.of(transform).add('ETLComponent', 'ETL-2');
    Tags.of(transform).add('ETLStage', 'TRANSFORM-LOAD');
    Tags.of(transform).add('DataSource', 'S3-CSV');
    Tags.of(transform).add('DataTarget', 'S3-PARQUET');
    Tags.of(transform).add('Owner', 'DataEngineering');
    Tags.of(transform).add('ResourceType', 'COMPUTE-GLUE');

    // 2) Catálogo + Crawler: Escanea etl-process2/ para detectar esquema del archivo Parquet único
    new CatalogConstruct(this, "Catalog", {
      dataBucket,
      curatedPrefix,
      databaseName: dbName,
      glueJobName: transform.jobName, // 🎯 Para trigger automático del crawler
    });

    // 3) Athena WorkGroup
    new AthenaConstruct(this, "Athena", {
      dataBucket,
      resultsPrefix: athenaResults,
      workGroupName: wgName,
    });

    // 4) Orquestación: Detecta archivos nuevos en etl-process1/ -> Dispara Glue Job
    new OrchestratorConstruct(this, "Orchestrator", {
      dataBucket,
      cleanPrefix, // reports/etl-process1/ (trigger cuando Lambda deposita CSV)
      glueJobName: transform.jobName,
    });
  }
}
