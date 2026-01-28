const http = require('http');

const API_KEY = process.env.INFRA_API_KEY || 'test-api-key';
const HOST = 'localhost';
const PORT = 8080;
const CONCURRENCY = 50;
const DURATION_SEC = 15;

function makeRequest(path, method = 'GET', body = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: HOST,
            port: PORT,
            path: path,
            method: method,
            headers: {
                'x-api-key': API_KEY,
                'Content-Type': 'application/json'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => resolve({ status: res.statusCode, body: data }));
        });

        req.on('error', (e) => reject(e));
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

async function run() {
    console.log(`\x1b[36m⚡ Starting Load Test: ${CONCURRENCY} Virtual Users\x1b[0m`);

    // 1. Get Function
    console.log("🔍 Finding target function...");
    let functionId = null;
    let functionName = 'Unknown';

    try {
        const res = await makeRequest('/api/functions');
        const funcs = JSON.parse(res.body);
        if (funcs.length > 0) {
            functionId = funcs[0].functionId;
            functionName = funcs[0].name;
            console.log(`✅ Target found: \x1b[33m${functionName}\x1b[0m (${functionId})`);
        } else {
            console.log("❌ No functions found. Please deploy a function first.");
            process.exit(1);
        }
    } catch (e) {
        console.log("❌ Failed to fetch functions: " + e.message);
        process.exit(1);
    }

    // 2. Hammer Time
    console.log(`🚀 Sending traffic... (Duration: ${DURATION_SEC}s)`);

    const startTime = Date.now();
    let requestsSent = 0;
    let successCount = 0;
    let failCount = 0;

    const displayInterval = setInterval(() => {
        const elapsed = (Date.now() - startTime) / 1000;
        const rps = (requestsSent / elapsed).toFixed(1);
        console.log(`[${elapsed.toFixed(1)}s] Requests: ${requestsSent} | Success: \x1b[32m${successCount}\x1b[0m | Failed: \x1b[31m${failCount}\x1b[0m | RPS: ${rps}`);
    }, 1000);

    const attackLoop = async () => {
        while ((Date.now() - startTime) < DURATION_SEC * 1000) {
            try {
                requestsSent++;
                const r = await makeRequest('/api/run', 'POST', {
                    functionId: functionId,
                    inputData: { test: true }
                });
                if (r.status === 200 || r.status === 202) successCount++;
                else failCount++;
            } catch (e) {
                failCount++;
            }
            // Small random drift to simulate users
            await new Promise(r => setTimeout(r, Math.random() * 50));
        }
    };

    const workers = [];
    for (let i = 0; i < CONCURRENCY; i++) {
        workers.push(attackLoop());
    }

    await Promise.all(workers);
    clearInterval(displayInterval);

    console.log("\n=================================");
    console.log("\x1b[32m✅ Load Test Completed\x1b[0m");
    console.log(`Total Requests: ${requestsSent}`);
    console.log(`Success Rate: ${((successCount / requestsSent) * 100).toFixed(1)}%`);
    console.log("=================================");
}

run();
