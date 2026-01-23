require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const multerS3 = require('multer-s3');

const { S3Client, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const { DynamoDBClient, PutItemCommand, GetItemCommand, ScanCommand, DeleteItemCommand, UpdateItemCommand, QueryCommand } = require("@aws-sdk/client-dynamodb");
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
const RATE_LIMIT_MAX = parseInt(process.env.RATE_LIMIT || "100");

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

// Worker Registry for Heartbeat Push Pattern (NAT-free health check)
const workerRegistry = new Map();
const WORKER_TIMEOUT_MS = 30000; // 30 seconds

// Clean up dead workers every 5 seconds
setInterval(() => {
    const now = Date.now();
    for (const [workerId, data] of workerRegistry.entries()) {
        if (now - data.lastSeen > WORKER_TIMEOUT_MS) {
            logger.warn(`Worker ${workerId} marked as dead (no heartbeat for ${WORKER_TIMEOUT_MS}ms)`);
            workerRegistry.delete(workerId);
        }
    }
}, 5000);

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

            // Store job result for async polling (GET /status/:jobId)
            // TTL: 1 hour (3600 seconds)
            redis.set(`job:${requestId}`, message, 'EX', 3600).catch(err => {
                logger.error("Failed to store job result in Redis", err);
            });

            // Persist execution log
            try {
                // Safe JSON Parse
                let result;
                try {
                    result = JSON.parse(message);
                } catch (jsonErr) {
                    console.error("[ERROR] JSON Parse Failed in Subscriber", jsonErr);
                    return;
                }

                if (!result || !result.functionId) return;

                const duration = result.durationMs !== undefined ? result.durationMs : (result.duration_ms || 0);
                const memoryBytes = result.peakMemoryBytes !== undefined ? result.peakMemoryBytes : (result.peak_memory_bytes || 0);
                const memoryMb = memoryBytes ? Math.round(memoryBytes / 1024 / 1024) : 0;

                // Persist to DynamoDB Logs Table
                if (process.env.LOGS_TABLE_NAME) {

                    // Truncate message to avoid DynamoDB 400KB limit
                    // UTF-8 can take up to 4 bytes per char, but usually 1-3. 
                    // Safe limit: ~350k chars.
                    let safeMessage = result.stderr || result.stdout || "";
                    if (safeMessage.length > 350000) {
                        safeMessage = safeMessage.substring(0, 350000) + "\n...[TRUNCATED: Log too large for metadata store. Check S3 for full output]...";
                    }

                    // TTL: 30 days from now (Epoch time in seconds)
                    const TTH_SECONDS = 30 * 24 * 60 * 60;
                    const expiresAt = Math.floor(Date.now() / 1000) + TTH_SECONDS;

                    const logItem = {
                        functionId: { S: result.functionId },
                        timestamp: { S: new Date().toISOString() },
                        requestId: { S: requestId },
                        status: { S: result.status || (result.exitCode === 0 ? "SUCCESS" : "ERROR") },
                        duration: { N: duration.toString() },
                        memoryMb: { N: memoryMb.toString() },
                        message: { S: safeMessage },
                        expiresAt: { N: expiresAt.toString() }
                    };

                    db.send(new PutItemCommand({
                        TableName: process.env.LOGS_TABLE_NAME,
                        Item: logItem
                    })).catch(err => console.error("Failed to persist log to DynamoDB", err));

                    // Increment Invocations & Total Duration (for Avg Latency)
                    if (process.env.TABLE_NAME) {
                        db.send(new UpdateItemCommand({
                            TableName: process.env.TABLE_NAME,
                            Key: { functionId: { S: result.functionId } },
                            UpdateExpression: "ADD invocations :inc, totalDuration :dur",
                            ExpressionAttributeValues: {
                                ":inc": { N: "1" },
                                ":dur": { N: duration.toString() }
                            }
                        })).then(() => console.log(`[STATS] Updated stats for ${result.functionId}`))
                            .catch(err => console.error("[STATS] Failed to increment stats:", err));
                    } else {
                        console.error("[STATS] No TABLE_NAME defined");
                    }
                }

                // Add to In-Memory Buffer
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

                // Observe Prometheus Metrics
                try {
                    const durationSec = duration / 1000;
                    functionDuration.observe({ functionId: result.functionId, status: result.status }, durationSec);
                    functionInvocations.inc({ functionId: result.functionId, status: result.status });
                } catch (e) {
                    console.error("Failed to observe metrics", e);
                }

            } catch (e) {
                console.error("Failed to add log in subscriber", e);
            }
        }
    }
});


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
        res.set('X-RateLimit-Limit', RATE_LIMIT_MAX);
        res.set('X-RateLimit-Remaining', Math.max(0, RATE_LIMIT_MAX - current));
        if (current > RATE_LIMIT_MAX) return res.status(429).json({ error: "Too Many Requests" });
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

// Function Execution Metrics
const functionDuration = new client.Histogram({
    name: 'function_duration_seconds',
    help: 'Duration of Function Execution in seconds',
    labelNames: ['functionId', 'status'],
    buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30]
});
register.registerMetric(functionDuration);

const functionInvocations = new client.Counter({
    name: 'function_invocations_total',
    help: 'Total number of function invocations',
    labelNames: ['functionId', 'status']
});
register.registerMetric(functionInvocations);

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

// Health Check
app.get(['/health', '/api/health'], (req, res) => {
    const status = isRedisConnected ? 200 : 503;
    res.status(status).json({ status: isRedisConnected ? 'OK' : 'ERROR', version: VERSION });
});

// Prometheus Metrics Endpoint
app.get(['/metrics', '/api/metrics'], async (req, res) => {
    try {
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (err) {
        res.status(500).end(err.message);
    }
});

// Worker Heartbeat Receiver (NAT-free health check)
app.post('/api/worker/heartbeat', (req, res) => {
    try {
        const { workerId, status, pools, activeJobs, uptimeSeconds, timestamp } = req.body;
        if (!workerId) {
            return res.status(400).json({ error: 'workerId is required' });
        }

        workerRegistry.set(workerId, {
            workerId,
            status: status || 'healthy',
            pools: pools || {},
            activeJobs: activeJobs || 0,
            uptimeSeconds: uptimeSeconds || 0,
            lastSeen: Date.now(),
            receivedAt: new Date().toISOString()
        });

        logger.info(`Heartbeat received from ${workerId}`, { activeJobs, pools });
        res.json({ ok: true });
    } catch (error) {
        logger.error('Heartbeat receiver error', error);
        res.status(500).json({ error: error.message });
    }
});

// Worker Status (list all known workers)
app.get(['/api/workers', '/api/worker/status'], (req, res) => {
    const workers = [];
    const now = Date.now();

    for (const [workerId, data] of workerRegistry.entries()) {
        workers.push({
            ...data,
            healthStatus: (now - data.lastSeen) < WORKER_TIMEOUT_MS ? 'healthy' : 'unhealthy',
            lastSeenAgo: Math.round((now - data.lastSeen) / 1000) + 's'
        });
    }

    res.json({
        totalWorkers: workers.length,
        healthyWorkers: workers.filter(w => w.healthStatus === 'healthy').length,
        workers
    });
});



// Model Catalog
app.get(['/models', '/api/models'], async (req, res) => {
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

// System Status
app.get(['/system/status', '/api/system/status'], cors(), async (req, res) => {
    try {
        // 1. Try Aggregating from Worker Registry (Preferred for Uptime/Multi-worker)
        let totalPools = { python: 0, nodejs: 0, cpp: 0, go: 0 };
        let totalActiveJobs = 0;
        let maxUptime = 0;
        let workerCount = 0;

        const now = Date.now();
        for (const [workerId, data] of workerRegistry.entries()) {
            // Only count healthy workers
            if (now - data.lastSeen < WORKER_TIMEOUT_MS) {
                workerCount++;
                totalActiveJobs += (data.activeJobs || 0);
                maxUptime = Math.max(maxUptime, data.uptimeSeconds || 0);

                if (data.pools) {
                    totalPools.python += (data.pools.python || 0);
                    totalPools.nodejs += (data.pools.nodejs || 0);
                    totalPools.cpp += (data.pools.cpp || 0);
                    totalPools.go += (data.pools.go || 0);
                }
            }
        }

        if (workerCount > 0) {
            return res.json({
                status: "online",
                worker_count: workerCount,
                active_jobs: totalActiveJobs,
                pools: totalPools,
                uptime_seconds: maxUptime,
                timestamp: Date.now() / 1000
            });
        }

        // 2. Fallback to Redis System Status (Legacy/Single Worker Direct Push)
        const raw = await redis.get("system:status");
        if (!raw) {
            // Offline
            return res.json({
                pools: { python: 0, nodejs: 0, cpp: 0, go: 0 },
                active_jobs: 0,
                status: "offline"
            });
        }
        const status = JSON.parse(raw);
        status.status = "online";
        res.json(status);

    } catch (error) {
        logger.error("System Status Error", { error: error.message });
        res.status(500).json({ error: "Failed to fetch status" });
    }
});

// Validation Middleware (Headers)
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

app.post(['/upload', '/api/upload'], authenticate, rateLimiter, validateUploadRequest, upload.single('file'), async (req, res) => {
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
                uploadedAt: { S: new Date().toISOString() },
                envVars: { S: req.body.envVars || "{}" }
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

// Run
app.post(['/run', '/api/run'], authenticate, rateLimiter, async (req, res) => {
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

        // Parse Env Vars from DynamoDB
        let envVars = {};
        if (Item.envVars && Item.envVars.S) {
            try {
                envVars = JSON.parse(Item.envVars.S);
            } catch (e) {
                logger.warn("Failed to parse envVars", { functionId, error: e.message });
            }
        }

        const taskPayload = {
            requestId,
            functionId,
            runtime: Item.runtime ? Item.runtime.S : "python",
            memoryMb: Item.memoryMb ? parseInt(Item.memoryMb.N) : 128,
            s3Bucket: process.env.BUCKET_NAME,
            s3Key: Item.s3Key ? Item.s3Key.S : "",
            timeoutMs: 300000,
            input: inputData || {},
            modelId: modelId || "llama3:8b",
            envVars: envVars
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

app.get(['/status/:jobId', '/api/status/:jobId'], authenticate, async (req, res) => {
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
            avgDuration: (item.invocations && parseInt(item.invocations.N) > 0 && item.totalDuration)
                ? Math.round(parseInt(item.totalDuration.N) / parseInt(item.invocations.N))
                : 0,
            uploadedAt: item.uploadedAt ? item.uploadedAt.S : new Date().toISOString()
        }));
        res.json(items);
    } catch (error) { res.status(500).json([]); }
});
app.get(['/logs', '/api/logs'], cors(), authenticate, (req, res) => {
    res.json(logBuffer.map(l => ({
        ...l,
        msg: (l.msg || "").length > 200 ? (l.msg || "").substring(0, 200) + "..." : (l.msg || "")
    })));
});
app.get(['/functions/:id', '/api/functions/:id'], cors(), authenticate, async (req, res) => {
    try {
        const response = await db.send(new GetItemCommand({ TableName: process.env.TABLE_NAME, Key: { functionId: { S: req.params.id } } }));
        if (!response.Item) return res.status(404).json({ error: "Not Found" });
        const item = response.Item;
        res.json({
            id: item.functionId.S,
            name: item.name?.S,
            description: item.description?.S || "",
            runtime: item.runtime?.S || "python",
            memoryMb: item.memoryMb ? parseInt(item.memoryMb.N) : 128,
            s3Key: item.s3Key?.S,
            uploadedAt: item.uploadedAt?.S
        });
    } catch (e) { res.status(500).json({ error: e.message }); }
});



// GET /api/functions/:id/logs
app.get('/api/functions/:id/logs', cors(), authenticate, async (req, res) => {
    try {
        if (!process.env.LOGS_TABLE_NAME) {
            // Local Dev Fallback
            const logs = logBuffer.filter(l => l.functionId === req.params.id);
            return res.json(logs.map(l => ({
                id: l.requestId,
                requestId: l.requestId,
                timestamp: l.timestamp,
                status: l.status,
                duration: l.duration,
                memory: l.memory,
                message: (l.msg || "").length > 200 ? (l.msg || "").substring(0, 200) + "..." : (l.msg || "")
            })));
        }

        const limit = parseInt(req.query.limit) || 1000;
        const startTime = req.query.startTime;

        const params = {
            TableName: process.env.LOGS_TABLE_NAME,
            KeyConditionExpression: "functionId = :fid",
            ExpressionAttributeValues: {
                ":fid": { S: req.params.id }
            },
            ScanIndexForward: false, // Descending order (newest first)
            Limit: limit
        };

        // Efficient Filtering using Sort Key (timestamp)
        if (startTime) {
            params.KeyConditionExpression += " AND #ts >= :startTime";
            params.ExpressionAttributeNames = { "#ts": "timestamp" };
            params.ExpressionAttributeValues[":startTime"] = { S: startTime };
        }

        const command = new QueryCommand(params);

        const response = await db.send(command);
        const logs = response.Items.map(item => {
            const rawMsg = item.message?.S || "";
            return {
                id: item.requestId?.S, // Use requestId as ID
                requestId: item.requestId?.S,
                timestamp: item.timestamp?.S,
                status: item.status?.S,
                duration: item.duration?.N ? parseInt(item.duration.N) : 0,
                memory: item.memoryMb?.N ? parseInt(item.memoryMb.N) : 0,
                message: rawMsg.length > 200 ? rawMsg.substring(0, 200) + "..." : rawMsg
            };
        });

        res.json(logs);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/functions/:id/logs/:requestId
app.get('/api/functions/:id/logs/:requestId', cors(), authenticate, async (req, res) => {
    try {
        const { id, requestId } = req.params;

        if (!process.env.LOGS_TABLE_NAME) {
            // Fallback: In-memory
            const log = logBuffer.find(l => l.functionId === id && l.requestId === requestId);
            if (!log) return res.status(404).json({ error: "Log not found" });
            return res.json({
                message: log.msg || ""
            });
        }

        // DynamoDB Query

        const command = new QueryCommand({
            TableName: process.env.LOGS_TABLE_NAME,
            KeyConditionExpression: "functionId = :fid",
            FilterExpression: "requestId = :rid",
            ExpressionAttributeValues: {
                ":fid": { S: id },
                ":rid": { S: requestId }
            },
            Limit: 1
        });

        const response = await db.send(command);
        if (!response.Items || response.Items.length === 0) {
            return res.status(404).json({ error: "Log not found" });
        }

        const item = response.Items[0];
        res.json({
            message: item.message?.S || ""
        });

    } catch (e) { res.status(500).json({ error: e.message }); }
});
// PUT /functions/:id
app.put(['/functions/:id', '/api/functions/:id'], authenticate, upload.single('file'), async (req, res) => {
    const functionId = req.params.id;
    try {
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

        const mem = req.body.memoryMb || req.body.memory;
        if (mem) {
            updateExpression += ", memoryMb = :m";
            expressionAttributeValues[":m"] = { N: mem.toString() };
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
        const getCmd = new GetItemCommand({
            TableName: process.env.TABLE_NAME,
            Key: { functionId: { S: functionId } }
        });
        const item = await db.send(getCmd);

        // Check if function exists
        if (!item.Item) {
            logger.warn(`Function not found for deletion`, { functionId });
            return res.status(404).json({ error: "Function not found", functionId });
        }

        // Try to delete S3 object (non-blocking - continue even if S3 fails)
        if (item.Item.s3Key && item.Item.s3Key.S) {
            try {
                await s3.send(new DeleteObjectCommand({
                    Bucket: process.env.BUCKET_NAME,
                    Key: item.Item.s3Key.S
                }));
                logger.info(`S3 object deleted`, { functionId, s3Key: item.Item.s3Key.S });
            } catch (s3Error) {
                // Include all details in the message for UI visibility
                const errInfo = `bucket=${process.env.BUCKET_NAME}, key=${item.Item.s3Key.S}, error=${s3Error.name}: ${s3Error.message}`;
                logger.warn(`S3 deletion failed: ${errInfo}`, { functionId });
            }
        }

        // Delete from DynamoDB
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
    res.status(err.status || 500).json({ error: err.message || "Internal Server Error" });
});

const server = app.listen(PORT, () => {
    logger.info(`Infra Controller ${VERSION} Started`, { port: PORT });
});
server.setTimeout(300000);

// Graceful Shutdown
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
