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
// 1. Body Parser first
app.use(express.json({ limit: '10mb' }));

const PORT = 8080;
const VERSION = "v2.8";

// In-Memory Log Buffer (Ring Buffer)
const logBuffer = [];
const MAX_LOGS = 100;

function addLog(level, msg, context = {}) {
    const logEntry = {
        id: uuidv4(),
        level,
        timestamp: new Date(),
        msg,
        ...context
    };

    // 1. Stdout for external collectors
    if (level === 'ERROR') console.error(JSON.stringify(logEntry));
    else if (level === 'WARN') console.warn(JSON.stringify(logEntry));
    else console.log(JSON.stringify(logEntry));

    // 2. Buffer for API
    logBuffer.unshift(logEntry);
    if (logBuffer.length > MAX_LOGS) logBuffer.pop();
}

const logger = {
    info: (msg, context) => addLog('INFO', msg, context),
    warn: (msg, context) => addLog('WARN', msg, context),
    error: (msg, error = {}) => addLog('ERROR', msg, { error: error.message || error, stack: error.stack })
};

// Fail-Fast
const REQUIRED_ENV = ['AWS_REGION', 'BUCKET_NAME', 'TABLE_NAME', 'SQS_URL', 'REDIS_HOST', 'INFRA_API_KEY'];
const missingEnv = REQUIRED_ENV.filter(key => !process.env[key]);
if (missingEnv.length > 0) {
    logger.error(`FATAL: Missing environment variables`, { missing: missingEnv });
    process.exit(1);
}

// AWS Clients
const s3 = new S3Client({
    region: process.env.AWS_REGION,
    endpoint: process.env.S3_ENDPOINT
});
const sqs = new SQSClient({
    region: process.env.AWS_REGION,
    endpoint: process.env.SQS_ENDPOINT
});
const db = new DynamoDBClient({
    region: process.env.AWS_REGION,
    endpoint: process.env.DYNAMODB_ENDPOINT
});

// Redis Client
const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000),
    maxRetriesPerRequest: null,
    enableOfflineQueue: false
});

let isRedisConnected = false;
redis.on('error', (err) => { isRedisConnected = false; logger.error("Global Redis Connection Error", err); });
redis.on('connect', () => { isRedisConnected = true; logger.info("Global Redis Connected Successfully"); });

// Global Redis Subscriber
const redisSub = new Redis({
    host: process.env.REDIS_HOST,
    port: 6379,
    retryStrategy: times => Math.min(times * 50, 2000),
    enableOfflineQueue: false,
    enableReadyCheck: false
});
const responseEmitter = new EventEmitter();
responseEmitter.setMaxListeners(0);

redisSub.on('connect', () => {
    logger.info("Global Redis Subscriber Connected");
    redisSub.psubscribe('result:*', (err, count) => {
        if (err) logger.error("Failed to subscribe to result:*", err);
        else logger.info(`Subscribed to result channels. Count: ${count}`);
    });
});

redisSub.on('pmessage', (pattern, channel, message) => {
    if (pattern === 'result:*') {
        const parts = channel.split(':');
        const requestId = parts[1];
        if (requestId) {
            responseEmitter.emit(requestId, message);

            // 1. Persist execution log for "Recent Invocations" display
            // This ensures results appear in the frontend table even if the user isn't actively waiting
            try {
                // Safe JSON Parse
                let result;
                try {
                    result = JSON.parse(message);
                } catch (jsonErr) {
                    // Log but don't crash if message is malformed
                    console.error("[ERROR] JSON Parse Failed in Subscriber", jsonErr);
                    return;
                }

                if (!result || !result.functionId) return;

                // 2. Normalize fields (Handle CamelCase vs Snake_case differences between Worker/Controller)
                const duration = result.durationMs !== undefined ? result.durationMs : (result.duration_ms || 0);
                const memoryBytes = result.peakMemoryBytes !== undefined ? result.peakMemoryBytes : (result.peak_memory_bytes || 0);
                const memoryMb = memoryBytes ? Math.round(memoryBytes / 1024 / 1024) : 0;

                // 3. Add to In-Memory Log Buffer
                addLog(
                    result.status === 'SUCCESS' ? 'INFO' : 'ERROR',
                    `Function Executed: ${result.functionId}`,
                    {
                        functionId: result.functionId,
                        requestId: requestId,
                        duration: duration,
                        memory: memoryMb,
                        status: result.status,
                        exitCode: result.exitCode
                    }
                );
            } catch (e) {
                console.error("Failed to add log in subscriber", e);
            }
        }
    }
});

// app.use(express.json) Moved to top

// Auth Middleware
const authenticate = (req, res, next) => {
    const clientKey = req.headers['x-api-key'];
    if (!clientKey || clientKey !== process.env.INFRA_API_KEY) {
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
        const current = await redis.eval(RATE_LIMIT_SCRIPT, 1, key, 60);
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
        bucket: process.env.BUCKET_NAME,
        key: function (req, file, cb) {
            const functionId = uuidv4();
            req.functionId = functionId;
            cb(null, `functions/${functionId}/v1.zip`);
        }
    }),
    limits: { fileSize: 50 * 1024 * 1024 }
});

// 0. Health Check
app.get('/health', (req, res) => {
    const status = isRedisConnected ? 200 : 503;
    res.status(status).json({ status: isRedisConnected ? 'OK' : 'ERROR', version: VERSION });
});

app.get('/metrics', async (req, res) => {
    try {
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (err) {
        res.status(500).end(err);
    }
});

// 0.2 Model Catalog
app.get('/models', async (req, res) => {
    try {
        const aiNodeUrl = process.env.AI_NODE_URL || 'http://10.0.20.100:11434';
        const timeoutMs = parseInt(process.env.AI_NODE_TIMEOUT || "2000");
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        const response = await fetch(`${aiNodeUrl}/api/tags`, { signal: controller.signal });
        clearTimeout(timeout);
        if (!response.ok) throw new Error(`AI Node returned ${response.status}`);

        const data = await response.json();
        const models = data.models.map(m => ({
            id: m.name,
            name: m.name,
            size: m.size,
            details: m.details,
            status: 'available'
        }));

        res.json({ models });
    } catch (error) {
        logger.error("Model Catalog Error", { error: error.message });
        res.json({
            models: [{ id: 'llama3:8b', name: 'llama3:8b (Default)', status: 'fallback', details: {} }],
            warning: "Could not fetch dynamic model list from AI Node. showing default."
        });
    }
});

// [Security] Validation Middleware (Headers) - Imported from reference
const validateUploadRequest = (req, res, next) => {
    const ALLOWED_RUNTIMES = ["python", "cpp", "nodejs", "go"];
    const runtime = req.headers['x-runtime'] || "python";
    if (!ALLOWED_RUNTIMES.includes(runtime)) {
        return res.status(400).json({ error: `Invalid runtime. Allowed: ${ALLOWED_RUNTIMES.join(", ")}` });
    }

    const memoryMb = parseInt(req.headers['x-memory-mb'] || "128");
    const MAX_MEMORY = (runtime === 'python') ? 10240 : 1024;

    if (isNaN(memoryMb) || memoryMb < 128 || memoryMb > MAX_MEMORY) {
        return res.status(400).json({
            error: `Invalid memoryMb for ${runtime}. Must be between 128 and ${MAX_MEMORY} MB.`
        });
    }

    req.validatedRuntime = runtime;
    req.validatedMemoryMb = memoryMb;
    req.functionName = req.headers['x-function-name'] ? decodeURIComponent(req.headers['x-function-name']) : null;
    next();
};

app.post('/upload', authenticate, rateLimiter, validateUploadRequest, upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: "No file provided" });
        const functionId = req.functionId || uuidv4();
        await db.send(new PutItemCommand({
            TableName: process.env.TABLE_NAME,
            Item: {
                functionId: { S: functionId },
                name: { S: req.functionName || req.file.originalname },
                description: { S: req.body.description || "" },
                s3Key: { S: req.file.key },
                originalName: { S: req.file.originalname },
                runtime: { S: req.validatedRuntime },
                memoryMb: { N: req.validatedMemoryMb.toString() },
                uploadedAt: { S: new Date().toISOString() }
                // TODO: Store Environment Variables in DynamoDB
                // envVars: { S: req.body.envVars || "[]" }
            }
        }));
        logger.info(`Upload Success`, { functionId });
        res.json({ success: true, functionId });
    } catch (error) {
        if (req.file) {
            s3.send(new DeleteObjectCommand({ Bucket: process.env.BUCKET_NAME, Key: req.file.key }))
                .catch(err => logger.warn("Cleanup failed", err));
        }
        logger.error("Upload Error", error);
        res.status(500).json({ error: error.message });
    }
});

// 2. Run
app.post('/run', authenticate, rateLimiter, async (req, res) => {
    const { functionId, inputData, modelId } = req.body || {};
    const isAsync = req.headers['x-async'] === 'true';
    if (!functionId) return res.status(400).json({ error: "functionId is required" });

    const requestId = uuidv4();
    logger.info(`Run Request`, { requestId, functionId, modelId, mode: isAsync ? 'ASYNC' : 'SYNC' });

    try {
        const { Item } = await db.send(new GetItemCommand({
            TableName: process.env.TABLE_NAME, Key: { functionId: { S: functionId } }
        }));
        if (!Item) return res.status(404).json({ error: "Function not found" });

        db.send(new UpdateItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } },
            UpdateExpression: "ADD invocations :inc",
            ExpressionAttributeValues: { ":inc": { N: "1" } }
        })).catch(err => logger.error("Count Update Failed", err));

        const taskPayload = {
            requestId,
            functionId,
            runtime: Item.runtime ? Item.runtime.S : "python",
            memoryMb: Item.memoryMb ? parseInt(Item.memoryMb.N) : 128,
            s3Bucket: process.env.BUCKET_NAME,
            s3Key: Item.s3Key ? Item.s3Key.S : "",
            timeoutMs: 300000,
            input: inputData || {},
            modelId: modelId || "llama3:8b"
        };

        const sendMessageParams = {
            QueueUrl: process.env.SQS_URL,
            MessageBody: JSON.stringify(taskPayload)
        };
        if (process.env.SQS_URL.endsWith('.fifo')) {
            sendMessageParams.MessageGroupId = "default";
            sendMessageParams.MessageDeduplicationId = requestId;
        }

        await sqs.send(new SendMessageCommand(sendMessageParams));

        if (isAsync) return res.status(202).json({ status: "ACCEPTED", jobId: requestId });

        const result = await waitForResult(requestId);
        res.json(result);

    } catch (error) {
        logger.error("Run Error", error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/status/:jobId', authenticate, async (req, res) => {
    try {
        const result = await redis.get(`job:${req.params.jobId}`);
        if (!result) return res.json({ status: "pending", message: "Running or not found" });
        res.json(JSON.parse(result));
    } catch (error) { res.status(500).json({ error: error.message }); }
});

function waitForResult(requestId) {
    return new Promise((resolve) => {
        let completed = false;
        const onResult = (msg) => {
            if (completed) return;
            cleanup();
            try { resolve(JSON.parse(msg)); } catch (e) { resolve({ raw: msg }); }
        };
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
        responseEmitter.once(requestId, onResult);
    });
}

// GET /functions, /logs, /functions/:id
app.get(['/functions', '/api/functions'], cors(), authenticate, async (req, res) => {
    try {
        const response = await db.send(new ScanCommand({ TableName: process.env.TABLE_NAME }));
        const items = response.Items.map(item => ({
            functionId: item.functionId.S,
            name: item.name ? item.name.S : "Unknown",
            description: item.description ? item.description.S : "",
            runtime: item.runtime ? item.runtime.S : "python",
            memoryMb: item.memoryMb ? parseInt(item.memoryMb.N) : 128,
            invocations: item.invocations ? parseInt(item.invocations.N) : 0,
            uploadedAt: item.uploadedAt ? item.uploadedAt.S : new Date().toISOString()
        }));
        res.json(items);
    } catch (error) { res.status(500).json([]); }
});
app.get(['/logs', '/api/logs'], cors(), authenticate, (req, res) => { res.json(logBuffer); });
app.get(['/functions/:id', '/api/functions/:id'], cors(), authenticate, async (req, res) => {
    try {
        const response = await db.send(new GetItemCommand({ TableName: process.env.TABLE_NAME, Key: { functionId: { S: req.params.id } } }));
        if (!response.Item) return res.status(404).json({ error: "Not Found" });
        const item = response.Item;
        res.json({
            id: item.functionId.S,
            name: item.name?.S,
            description: item.description?.S || "",
            s3Key: item.s3Key?.S,
            uploadedAt: item.uploadedAt?.S
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// PUT /functions/:id (Update)
app.put(['/functions/:id', '/api/functions/:id'], authenticate, upload.single('file'), async (req, res) => {
    const functionId = req.params.id;
    try {
        // 1. If new file uploaded, update S3 Key
        let updateExpression = "set updated_at = :t";
        let expressionAttributeValues = {
            ":t": { S: new Date().toISOString() }
        };

        if (req.file) {
            // Clean up old S3 file before update to save cost
            try {
                const { Item: oldItem } = await db.send(new GetItemCommand({
                    TableName: process.env.TABLE_NAME, Key: { functionId: { S: functionId } }
                }));
                if (oldItem && oldItem.s3Key) {
                    s3.send(new DeleteObjectCommand({
                        Bucket: process.env.BUCKET_NAME,
                        Key: oldItem.s3Key.S
                    })).catch(err => logger.warn("Failed to delete old S3 file", err));
                }
            } catch (ignore) { }

            // Update S3 Key
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

// DELETE /functions/:id
app.delete(['/functions/:id', '/api/functions/:id'], cors(), authenticate, async (req, res) => {
    const functionId = req.params.id;
    try {
        // 1. Get S3 Key first
        const getCmd = new GetItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } }
        });
        const item = await db.send(getCmd);

        // 2. Delete S3 file
        if (item.Item && item.Item.s3Key) {
            await s3.send(new DeleteObjectCommand({
                Bucket: process.env.BUCKET_NAME,
                Key: item.Item.s3Key.S
            }));
        }

        // 3. Delete metadata
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

// Improved Global Error Handler
// Prevents 500 crashes from non-critical errors (e.g. Malformed JSON from curl)
app.use((err, req, res, next) => {
    logger.error("Global Error Handler", err);
    res.status(err.status || 500).json({ error: err.message || "Internal Server Error" });
});

const server = app.listen(PORT, () => {
    logger.info(`Infra Controller ${VERSION} Started`, { port: PORT });
});
server.setTimeout(300000);

// Graceful Shutdown - Merged from reference + safety
process.on('SIGTERM', () => {
    logger.info("SIGTERM received. Starting graceful shutdown...");
    server.close(() => {
        logger.info("HTTP Server Closed");
        Promise.all([
            redis.quit().catch(err => logger.error("Error closing Redis", err)),
            redisSub.quit().catch(err => logger.error("Error closing RedisSub", err))
        ]).finally(() => {
            logger.info("Resource cleanup finished. Exiting.");
            process.exit(0);
        });
    });
});
