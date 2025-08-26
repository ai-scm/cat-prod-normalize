import { Construct } from "constructs";
import { aws_events as events, aws_events_targets as targets, aws_iam as iam, aws_s3 as s3 } from "aws-cdk-lib";

export interface OrchestratorProps {
  dataBucket: s3.IBucket;
  cleanPrefix: string; // "clean/"
  glueJobName: string;
}

export class OrchestratorConstruct extends Construct {
  constructor(scope: Construct, id: string, props: OrchestratorProps) {
    super(scope, id);

    const { dataBucket, cleanPrefix, glueJobName } = props;

    const rule = new events.Rule(this, "OnCsvArrived", {
      eventPattern: {
        source: ["aws.s3"],
        detailType: ["Object Created"],
        detail: {
          bucket: { name: [dataBucket.bucketName] },
          object: { key: [{ prefix: cleanPrefix }] },
        },
      },
    });

    // Create an IAM role for EventBridge to invoke Glue
    const eventBridgeGlueRole = new iam.Role(this, "EventBridgeGlueRole", {
      assumedBy: new iam.ServicePrincipal("events.amazonaws.com"),
    });

    eventBridgeGlueRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["glue:StartJobRun"],
        resources: ["*"],
      })
    );

  rule.addTarget(new targets.AwsApi({
      service: "Glue",
      action: "startJobRun",
      parameters: { JobName: glueJobName },
      policyStatement: new iam.PolicyStatement({
        actions: ["glue:StartJobRun"],
        resources: ["*"],
      }),
    }));
  }
}
