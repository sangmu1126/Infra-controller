require('dotenv').config();
const express = require('express');
const { S3Client } = require("@aws-sdk/client-s3");
const { SQSClient } = require("@aws-sdk/client-sqs");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const Redis = require("ioredis");
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 8080;

// AWS Clients 초기화
const s3 = new S3Client({ region: process.env.AWS_REGION });
const sqs = new SQSClient({ region: process.env.AWS_REGION });
const db = new DynamoDBClient({ region: process.env.AWS_REGION });


app.use(express.json());

// 서버 시작
app.listen(PORT, () => {
    console.log(`🚀 NanoGrid Controller running on port ${PORT}`);
    console.log(`   - Mode: EC2 Native (No Lambda)`);
});