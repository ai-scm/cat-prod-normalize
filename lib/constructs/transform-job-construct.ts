import { Construct } from "constructs";
import {
  aws_glue as glue,
  aws_iam as iam,
  aws_s3 as s3,
  aws_s3_deployment as s3deploy,
  Duration,
  CfnOutput,
} from "aws-cdk-lib";
import * as path from "path";

export interface TransformJobProps {
  /** Bucket de datos existente (el mismo donde el ETL-1 dejó CSV) */
  dataBucket: s3.IBucket;
  /** Prefijo de entrada (CSV) p.ej. "clean/" */
  inputPrefix: string;
  /** Prefijo de salida (Parquet) p.ej. "curated/" */
  outputPrefix: string;
  /** Ruta S3 del script de Glue, p.ej. s3://<bucket>/scripts/cat-prod-normalize-data-glue.py (opcional, usa Asset si no se especifica) */
  scriptS3Uri?: string;
  /** Glue version: "4.0" recomendado */
  glueVersion?: string;
  /** Workers y tipo */
  numberOfWorkers?: number;
  workerType?: "G.1X" | "G.2X" | "G.4X";
}

export class TransformJobConstruct extends Construct {
  public readonly jobName: string;
  public readonly role: iam.Role;

  constructor(scope: Construct, id: string, props: TransformJobProps) {
    super(scope, id);

    const {
      dataBucket,
      inputPrefix,
      outputPrefix,
      scriptS3Uri,
      glueVersion = "4.0",
      numberOfWorkers = 2,
      workerType = "G.1X",
    } = props;

    // Crear script deployment en bucket específico o usar URI existente
    let scriptLocation: string;
    
    if (scriptS3Uri) {
      // Usar script S3 URI proporcionado (backward compatibility)
      scriptLocation = scriptS3Uri;
    } else {
      // Deployar script al bucket específico con estructura organizada
      const scriptsPrefix = `${outputPrefix}scripts/`;
      const scriptKey = `${scriptsPrefix}glue_job_script.py`;
      
      new s3deploy.BucketDeployment(this, "GlueScriptDeployment", {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../../lambda/etl-process2"), {
          exclude: ["requirements.txt", "**/__pycache__/**"]
        })],
        destinationBucket: dataBucket,
        destinationKeyPrefix: scriptsPrefix,
        retainOnDelete: false, // Limpia al eliminar stack
      });
      
      scriptLocation = `s3://${dataBucket.bucketName}/${scriptKey}`;
      
      // Output para debugging
      new CfnOutput(this, "ScriptDeploymentS3Url", { 
        value: scriptLocation,
        description: "Deployed Glue script location in organized structure" 
      });
    }

    // Role del Job (mínimo privilegio)
    const role = new iam.Role(this, "GlueEtl2Role", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      description:
        "Role for Glue ETL-2: read CSV from clean/ and write Parquet to curated/",
    });

    role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
    );

    // Permisos S3 mínimos
    dataBucket.grantRead(role, `${inputPrefix}*`); // leer CSV
    dataBucket.grantReadWrite(role, `${outputPrefix}*`); // escribir Parquet
    
    // 🔧 PERMISOS ADICIONALES EXPLÍCITOS para Glue Job
    role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        "s3:GetObject",
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      resources: [
        dataBucket.bucketArn,
        `${dataBucket.bucketArn}/*`,
        `${dataBucket.bucketArn}/${inputPrefix}*`,
        `${dataBucket.bucketArn}/${outputPrefix}*`
      ]
    }));
    
    // Si el bucket está cifrado con KMS, agrega permisos kms:Decrypt/Encrypt aquí.

    // Job de Glue (Spark)
    const job = new glue.CfnJob(this, "ParquetEtlJob", {
      name: "cat-prod-etl2-parquet",
      role: role.roleArn,
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: scriptLocation, // Usa Asset automáticamente o URI manual
      },
      glueVersion,
      numberOfWorkers,
      workerType,
      defaultArguments: {
        "--region": this.node.tryGetContext("aws:cdk:region") ?? process.env.CDK_DEFAULT_REGION ?? "",
        "--input_bucket": dataBucket.bucketName,
        "--input_prefix": inputPrefix,
        "--output_bucket": dataBucket.bucketName,
        "--output_prefix": outputPrefix,
        "--write_csv": "false",
        "--enable-continuous-cloudwatch-log": "true",
        // Configuración para archivo único sin particionado
        "--partition_mode": "none",
        "--write_mode": "overwrite",
        "--single_file": "true",
        "--file_name": "dashboard_usuarios_catia_consolidated.parquet",
        // 🎯 Instalar tiktoken para cálculo de tokens GPT-4/GPT-3.5-turbo
        "--additional-python-modules": "tiktoken>=0.5.0"
      },
      // opcional: tiempo máx / notificación
      timeout: Duration.hours(2).toMinutes(), // Glue espera minutos
    });

    this.jobName = job.name!;
    this.role = role;

    new CfnOutput(this, "GlueJobName", { value: this.jobName });
  }
}
