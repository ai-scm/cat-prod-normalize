#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CatProdNormalizeStack } from '../lib/cat-prod-normalize-stack';
import * as fs from 'fs';
import * as path from 'path';

// 📋 Cargar configuraciones
const configPath = path.join(__dirname, '../config');
const accountConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'accountConfig.json'), 'utf8'));
const appConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'config.json'), 'utf8'));
const tagsConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'tags.json'), 'utf8'));

const app = new cdk.App();

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

// 🏷️ Aplicar tags a nivel de aplicación
Object.keys(tagsConfig).forEach(key => {
  cdk.Tags.of(app).add(key, tagsConfig[key]);
});