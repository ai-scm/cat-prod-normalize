#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CatProdNormalizeStack } from '../lib/stacks/cat-prod-normalize-stack';
import { NewEtlStack } from '../lib/stacks/cad-prod-etl-stack';
import * as fs from 'fs';
import * as path from 'path';

const app = new cdk.App();
const configPath = path.join(__dirname, '../config');
const accountConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'accountConfig.json'), 'utf8'));
const appConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'config.json'), 'utf8'));
const tagsConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'tags.json'), 'utf8'));

// 🏷️ Crear nombre del stack con nomenclatura estándar
const stackName = `${appConfig.namespace}-normalize-stack`;

interface StackProps extends cdk.StackProps {
  namespace: string;
  tags?: Record<string, string>;
}

const stack = new CatProdNormalizeStack(app, stackName, {
  env: { 
    account: accountConfig.accountId, 
    region: accountConfig.region 
  },
  tags: tagsConfig,
  stackName: stackName,
  namespace: appConfig.namespace
} as StackProps);

// Nuevo Stack ETL-2 (Glue + Athena + Orquestación)
new NewEtlStack(app, `${appConfig.namespace}-etl2-stack`, {
  env: { account: accountConfig.accountId, region: accountConfig.region },
  dataBucketName: 'cat-prod-normalize-reports', // mismo bucket que ETL-1
  // glueScriptS3Uri: Ya no es necesario, usa Asset automáticamente
  cleanPrefix: 'reports/etl-process1/',        // Input: CSV del ETL-1
  curatedPrefix: 'reports/etl-process2/',      // Output: Parquet del ETL-2
  athenaResultsPrefix: 'athena/results/',
  glueDatabaseName: 'cat_prod_analytics_db',
  athenaWorkGroup: 'wg-cat-prod-analytics'
});

// 🏷️ Aplicar tags a nivel de aplicación
Object.keys(tagsConfig).forEach(key => {
  cdk.Tags.of(app).add(key, tagsConfig[key]);
});