require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const multerS3 = require('multer-s3');

const { S3Client, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient, PutItemCommand, GetItemCommand, ScanCommand, DeleteItemCommand, UpdateItemCommand } = require("@aws-sdk/client-dynamodb");
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const Redis = require("ioredis");
const { v4: uuidv4 } = require('uuid');
const { EventEmitter } = require('events');
const client = require('prom-client');

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

// Global Redis Subscriber (Optimization)
const redisSub = new Redis({
    host: process.env.REDIS_HOST,
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000)
});
const responseEmitter = new EventEmitter();

redisSub.on('connect', () => {
    logger.info("Global Redis Subscriber Connected");
    // Listen for all result channels
    redisSub.psubscribe('result:*', (err, count) => {
        if (err) logger.error("Failed to subscribe to result:*", err);
        else logger.info(`Subscribed to result channels. Count: ${count}`);
    });
});

redisSub.on('pmessage', (pattern, channel, message) => {
    // Channel format: result:{requestId}
    if (pattern === 'result:*') {
        const parts = channel.split(':');
        const requestId = parts[1];
        if (requestId) {
            // Dispatch to the specific request promise
            responseEmitter.emit(requestId, message);
        }
    }
});

app.use(express.json({ limit: '10mb' }));

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

// Prometheus Metrics
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const httpRequestDurationMicroseconds = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'Duration of HTTP requests in seconds',
    labelNames: ['method', 'route', 'status_code'],
    buckets: [0.1, 0.5, 1, 2, 5]
});
register.registerMetric(httpRequestDurationMicroseconds);

// Metrics Middleware
app.use((req, res, next) => {
    const end = httpRequestDurationMicroseconds.startTimer();
    res.on('finish', () => {
        if (req.route) {
            end({ method: req.method, route: req.route.path, status_code: res.statusCode });
        }
    });
    next();
});

// Multer S3
const upload = multer({
    storage: multerS3({
        s3: s3,
        bucket: process.env.BUCKET_NAME, // Must match S3_CODE_BUCKET in Worker
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

// 0.1 Metrics Endpoint
app.get('/metrics', async (req, res) => {
    try {
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (err) {
        res.status(500).end(err);
    }
});


// 1. Upload
app.post('/upload', authenticate, rateLimiter, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: "No file provided" });

        // Input Validation
        const memoryMb = parseInt(req.body.memoryMb || "128");
        if (isNaN(memoryMb) || memoryMb < 128 || memoryMb > 10240) {
            return res.status(400).json({ error: "Invalid memoryMb. Must be between 128 and 10240." });
        }

        const functionId = req.functionId || uuidv4();

        await db.send(new PutItemCommand({
            TableName: process.env.TABLE_NAME,
            Item: {
                functionId: { S: functionId },
                s3Key: { S: req.file.key },
                originalName: { S: req.file.originalname },
                runtime: { S: req.body.runtime || "python" },
                memoryMb: { N: memoryMb.toString() }, // Auto-Tuner
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
            memoryMb: Item.memoryMb ? parseInt(Item.memoryMb.N) : 128, // Auto-Tuner
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
        let completed = false;

        // Listener for the result
        const onResult = (msg) => {
            if (completed) return;
            cleanup();
            try { resolve(JSON.parse(msg)); } catch (e) { resolve({ raw: msg }); }
        };

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
            responseEmitter.removeListener(requestId, onResult);
        }

        // Register the one-time listener
        responseEmitter.once(requestId, onResult);
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

// 1. 함수 상세 조회 (GET /functions/:id) - 설정 페이지용
app.get(['/functions/:id', '/api/functions/:id'], cors(), async (req, res) => {
    const functionId = req.params.id;
    try {
        const command = new GetItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } }
        });
        const response = await db.send(command);

        if (!response.Item) {
            return res.status(404).json({ error: "Function not found" });
        }

        const item = response.Item;
        res.json({
            id: item.functionId.S,
            functionId: item.functionId.S,
            name: item.originalName?.S || "Unknown",
            runtime: item.runtime?.S || "python",
            description: item.description?.S || "",
            // S3 키 정보를 줘야 "코드 수정" 때 원본을 알 수 있음
            s3Key: item.s3Key?.S || "",
            uploadedAt: item.uploadedAt?.S || ""
        });
    } catch (error) {
        logger.error("Get Detail Error", error);
        res.status(500).json({ error: error.message });
    }
});

// 2. 함수 코드/정보 수정 (PUT /functions/:id)
// 파일이 있으면 S3 덮어쓰기 + DB 업데이트, 파일 없으면 DB만 업데이트
app.put(['/functions/:id', '/api/functions/:id'], authenticate, upload.single('file'), async (req, res) => {
    const functionId = req.params.id;
    try {
        // 1. 파일이 새로 올라왔으면 S3 Key 업데이트 필요
        let updateExpression = "set updated_at = :t";
        let expressionAttributeValues = {
            ":t": { S: new Date().toISOString() }
        };

        if (req.file) {
            // 새 파일이 있으면 S3 Key도 업데이트
            updateExpression += ", s3Key = :k, originalName = :n";
            expressionAttributeValues[":k"] = { S: req.file.key };
            expressionAttributeValues[":n"] = { S: req.file.originalname };
        }

        if (req.body.description) {
            updateExpression += ", description = :d";
            expressionAttributeValues[":d"] = { S: req.body.description };
        }

        const command = new UpdateItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } },
            UpdateExpression: updateExpression,
            ExpressionAttributeValues: expressionAttributeValues
        });

        await db.send(command);
        logger.info(`Function Updated`, { functionId });
        res.json({ success: true, functionId });

    } catch (error) {
        logger.error("Update Error", error);
        res.status(500).json({ error: error.message });
    }
});

// 3. 함수 삭제 (DELETE /functions/:id) - S3 파일까지 진짜 삭제
app.delete(['/functions/:id', '/api/functions/:id'], cors(), async (req, res) => {
    const functionId = req.params.id;
    try {
        // 1. 먼저 DB에서 S3 Key를 알아내야 함
        const getCmd = new GetItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } }
        });
        const item = await db.send(getCmd);

        // 2. S3에서 파일 삭제 (비용 절감)
        if (item.Item && item.Item.s3Key) {
            await s3.send(new DeleteObjectCommand({
                Bucket: process.env.BUCKET_NAME,
                Key: item.Item.s3Key.S
            }));
        }

        // 3. DynamoDB에서 메타데이터 삭제
        await db.send(new DeleteItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } }
        }));

        logger.info(`Function Deleted`, { functionId });
        res.json({ success: true, deletedId: functionId });

    } catch (error) {
        logger.error("Delete Error", error);
        res.status(500).json({ error: error.message });
    }
});

// Global Error Handler
app.use((err, req, res, next) => {
    logger.error("Global Error Handler", err);
    res.status(500).json({ error: "Internal Server Error" });
});

const server = app.listen(PORT, () => {
    logger.info(`NanoGrid Controller ${VERSION} Started`, { port: PORT });
});
server.setTimeout(300000); // Socket Timeout: 5 min

process.on('SIGTERM', () => {
    logger.info("SIGTERM received. Starting graceful shutdown...");
    server.close(() => {
        logger.info("HTTP Server Closed");
        // Disconnect Redis clients
        Promise.all([
            redis.quit().catch(err => logger.error("Error closing Redis", err)),
            redisSub.quit().catch(err => logger.error("Error closing RedisSub", err))
        ]).finally(() => {
            logger.info("Resource cleanup finished. Exiting.");
            process.exit(0);
        });
    });
});