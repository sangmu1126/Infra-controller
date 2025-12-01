# 🚀 NanoGrid Controller (Part B) Detailed Role Definition

This document describes the detailed functions and responsibilities of the **Controller server (Node.js/Express)** in the NanoGrid project's Control Plane (Part B). The Controller handles user requests, distributes them to AWS managed services, and relays the responses from Workers back to users.

## 🏗️ Controller Architecture Roles

The Controller focuses on reliably routing requests regardless of the Worker (Data Plane) state.

### 1.1 Resource Manager Role (Resource Management)

**Goal:** Safely store user code and record metadata required for execution.

**Implementation:** Implemented in the `POST /upload` endpoint.

### 1.2 Dispatcher Role (Task Distribution & Waiting)

**Goal:** Direct tasks to Workers via SQS and act as a mediator to receive results via Redis.

**Implementation:** Implemented in the `POST /run` endpoint.

## 🔌 Endpoint Function Details

### 1️⃣ Code Upload (`POST /upload`)

Handles user-uploaded code files.

| Function          | Technology                | Description                                                                            |
| ----------------- | ------------------------- | -------------------------------------------------------------------------------------- |
| S3 Uploader       | multer-s3                 | Stores user files in S3 bucket (path: `functions/[func-id]/v1.zip`).                   |
| Metadata Recorder | DynamoDB (PutItemCommand) | Records necessary execution info (`functionId`, `s3Key`, `runtime`) after file upload. |

### 2️⃣ Function Execution Request (`POST /run`)

Directs Workers to execute functions and waits for results.

| Function        | Technology                | Description                                                                                                                            |
| --------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Metadata Reader | DynamoDB (GetItemCommand) | Retrieves S3 path and settings for the `functionId` from the database.                                                                 |
| SQS Producer    | SQS (SendMessageCommand)  | Sends task messages to **nanogrid-task-queue** for Workers to process.                                                                 |
| Redis Consumer  | Redis Pub/Sub             | Blocks until the Worker publishes results to `result:{req_id}` channel. This simulates synchronous response from asynchronous Workers. |

## 🛡️ Controller Reliability & Features

### 3.1 Async-to-Sync Bridge

While tasks are asynchronously sent to Workers via SQS, Redis Pub/Sub ensures that clients receive a **synchronous (Sync)** response. This design enhances the user experience (UX) during API calls.

### 3.2 Timeout Handling

* **Wait Time:** `/run` waits up to 25 seconds for Worker responses.
* **Response:** If the Worker does not respond within 25 seconds, returns HTTP 200 with `{ "status": "TIMEOUT" }`, ensuring the system remains stable and communicates the status safely to clients.

### 3.3 Error Handling

* **404 Not Found:** Returned immediately if the `functionId` does not exist in DynamoDB.
* **500 Internal Server Error:** Prevents critical failures during communication with S3, DynamoDB, or SQS.
