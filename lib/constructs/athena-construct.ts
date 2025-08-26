import { Construct } from "constructs";
import { aws_athena as athena, aws_s3 as s3, CfnOutput } from "aws-cdk-lib";

export interface AthenaProps {
  dataBucket: s3.IBucket;
  resultsPrefix: string; // "athena/results/"
  workGroupName: string; // "wg-analytics"
}

export class AthenaConstruct extends Construct {
  public readonly workGroupName: string;

  constructor(scope: Construct, id: string, props: AthenaProps) {
    super(scope, id);
    const { dataBucket, resultsPrefix, workGroupName } = props;

    new athena.CfnWorkGroup(this, "AnalyticsWg", {
      name: workGroupName,
      state: "ENABLED",
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: false,
        resultConfiguration: {
          outputLocation: `s3://${dataBucket.bucketName}/${resultsPrefix}`,
        },
      },
    });

    this.workGroupName = workGroupName;
    new CfnOutput(this, "AthenaWorkGroup", { value: this.workGroupName });
  }
}
