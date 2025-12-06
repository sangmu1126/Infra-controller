require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const multerS3 = require('multer-s3');
const { S3Client } = require("@aws-sdk/client-s3");
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { DynamoDBClient, PutItemCommand, GetItemCommand, ScanCommand } = require("@aws-sdk/client-dynamodb");
const Redis = require("ioredis");
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
const PORT = 8080; 
const VERSION = "v2.4";

const logger = {
    info: (msg, context = {}) => console.log(JSON.stringify({ level: 'INFO', timestamp: new Date(), msg, ...context })),
    warn: (msg, context = {}) => console.warn(JSON.stringify({ level: 'WARN', timestamp: new Date(), msg, ...context })),
    error: (msg, error = {}) => console.error(JSON.stringify({ level: 'ERROR', timestamp: new Date(), msg, error: error.message || error, stack: error.stack }))
};

// Fail-Fast
const REQUIRED_ENV = ['AWS_REGION', 'BUCKET_NAME', 'TABLE_NAME', 'SQS_URL', 'REDIS_HOST', 'NANOGRID_API_KEY'];
const missingEnv = REQUIRED_ENV.filter(key => !process.env[key]);
if (missingEnv.length > 0) {
    logger.error(`FATAL: Missing environment variables`, { missing: missingEnv });
    process.exit(1);
}

// AWS Clients
const s3 = new S3Client({ region: process.env.AWS_REGION });
const sqs = new SQSClient({ region: process.env.AWS_REGION });
const db = new DynamoDBClient({ region: process.env.AWS_REGION });

// Redis Client
const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000),
    maxRetriesPerRequest: null
});

let isRedisConnected = false;
redis.on('error', (err) => { isRedisConnected = false; logger.error("Global Redis Connection Error", err); });
redis.on('connect', () => { isRedisConnected = true; logger.info("Global Redis Connected Successfully"); });

app.use(express.json());

// Auth Middleware
const authenticate = (req, res, next) => {
    const clientKey = req.headers['x-api-key'];
    if (!clientKey || clientKey !== process.env.NANOGRID_API_KEY) {
        logger.warn("Unauthorized access", { ip: req.ip });
        return res.status(401).json({ error: "Unauthorized" });
    }
    next();
};

// Rate Limiting (Lua)
const RATE_LIMIT_SCRIPT = `
    local current = redis.call("INCR", KEYS[1])
    if current == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
    return current
`;
const rateLimiter = async (req, res, next) => {
    try {
        const ip = req.ip || req.connection.remoteAddress;
        const key = `ratelimit:${ip}`;
        const current = await redis.eval(RATE_LIMIT_SCRIPT, 1, key, 60); // 1 min
        res.set('X-RateLimit-Limit', 100);
        res.set('X-RateLimit-Remaining', Math.max(0, 100 - current));
        if (current > 100) return res.status(429).json({ error: "Too Many Requests" });
        next();
    } catch (error) { next(); }
};

// Multer S3
const upload = multer({
    storage: multerS3({
        s3: s3,
        bucket: process.env.BUCKET_NAME,
        key: function (req, file, cb) {
            const functionId = uuidv4();
            req.functionId = functionId; 
            cb(null, `functions/${functionId}/v1.zip`); 
        }
    })
});

// 0. Health Check
app.get('/health', (req, res) => {
    const status = isRedisConnected ? 200 : 503;
    res.status(status).json({ status: isRedisConnected ? 'OK' : 'ERROR', version: VERSION });
});

// 1. Upload
app.post('/upload', authenticate, rateLimiter, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: "No file provided" });
        const functionId = req.functionId || uuidv4();
        
        await db.send(new PutItemCommand({
            TableName: process.env.TABLE_NAME,
            Item: {
                functionId: { S: functionId },
                s3Key: { S: req.file.key },
                originalName: { S: req.file.originalname },
                runtime: { S: req.body.runtime || "python" },
                uploadedAt: { S: new Date().toISOString() }
            }
        }));
        logger.info(`Upload Success`, { functionId });
        res.json({ success: true, functionId });
    } catch (error) {
        logger.error("Upload Error", error);
        res.status(500).json({ error: error.message });
    }
});

// 2. Run
app.post('/run', authenticate, rateLimiter, async (req, res) => {
    const { functionId, inputData } = req.body || {};
    const isAsync = req.headers['x-async'] === 'true'; 
    if (!functionId) return res.status(400).json({ error: "functionId is required" });

    const requestId = uuidv4();
    logger.info(`Run Request`, { requestId, functionId, mode: isAsync ? 'ASYNC' : 'SYNC' });

    try {
        const { Item } = await db.send(new GetItemCommand({
            TableName: process.env.TABLE_NAME, Key: { functionId: { S: functionId } }
        }));
        if (!Item) return res.status(404).json({ error: "Function not found" });

        const taskPayload = {
            requestId,
            functionId,
            runtime: Item.runtime ? Item.runtime.S : "python", 
            s3Bucket: process.env.BUCKET_NAME,
            s3Key: Item.s3Key ? Item.s3Key.S : "",
            timeoutMs: 300000, // Worker Timeout: 5 min
            input: inputData || {}
        };

        await sqs.send(new SendMessageCommand({
            QueueUrl: process.env.SQS_URL, MessageBody: JSON.stringify(taskPayload)
        }));
        
        if (isAsync) {
            return res.status(202).json({ 
                status: "ACCEPTED", 
                message: "Job submitted.", 
                jobId: requestId 
            });
        }
        
        const result = await waitForResult(requestId);
        res.json(result);

    } catch (error) {
        logger.error("Run Error", error);
        res.status(500).json({ error: error.message });
    }
});

// 3. Status Check (Polling)
app.get('/status/:jobId', authenticate, async (req, res) => {
    try {
        // Worker가 완료 후 'job:{jobId}' 키에 결과를 저장해야 함
        const result = await redis.get(`job:${req.params.jobId}`);
        if (!result) return res.json({ status: "pending", message: "Running or not found" });
        res.json(JSON.parse(result));
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

function waitForResult(requestId) {
    return new Promise((resolve) => {
        const sub = new Redis({ host: process.env.REDIS_HOST, port: 6379 });
        const channel = `result:${requestId}`;
        let completed = false;

        // Controller Wait Timeout: 290s (Worker Timeout 300s보다 약간 짧게)
        const timeout = setTimeout(() => { 
            if (!completed) {
                cleanup();
                logger.warn("Sync Wait Timed Out", { requestId });
                resolve({ status: "TIMEOUT", message: "Processing timed out." });
            }
        }, 290000); 

        function cleanup() {
            completed = true;
            clearTimeout(timeout);
            try { sub.disconnect(); } catch (e) {}
        }

        sub.subscribe(channel);
        sub.on('message', (chn, msg) => {
            if (chn === channel) {
                cleanup();
                try { resolve(JSON.parse(msg)); } catch (e) { resolve({ raw: msg }); }
            }
        });
    });
}

// 1. 함수 목록 조회 (GET /functions)
app.get(['/functions', '/api/functions'], cors(), async (req, res) => {
    try {
        const command = new ScanCommand({ TableName: process.env.TABLE_NAME });
        const response = await db.send(command);
        
        const items = response.Items.map(item => ({
            functionId: item.functionId.S,
            name: item.originalName ? item.originalName.S : "Unknown",
            runtime: item.runtime ? item.runtime.S : "python",
            description: item.description ? item.description.S : "",
            uploadedAt: item.uploadedAt ? item.uploadedAt.S : new Date().toISOString()
        }));
        res.json(items);
    } catch (error) {
        logger.error("List Functions Error", error);
        res.status(500).json([]); 
    }
});

// 2. 로그 조회 (GET /logs)
// (일단 에러 안 나게 빈 데이터라도 줌)
app.get(['/logs', '/api/logs'], cors(), (req, res) => {
    res.json([
        { id: "1", timestamp: new Date().toISOString(), message: "NanoGrid Controller is running." },
        { id: "2", timestamp: new Date().toISOString(), message: "Waiting for jobs..." }
    ]);
});

const server = app.listen(PORT, () => {
    logger.info(`NanoGrid Controller ${VERSION} Started`, { port: PORT });
});
server.setTimeout(300000); // Socket Timeout: 5 min

process.on('SIGTERM', () => { server.close(() => process.exit(0)); });