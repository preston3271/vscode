"""Lesson 2: the smallest useful Lambda function.

Every Lambda function is just a regular function that AWS calls with two
arguments:

* ``event``   - a dict describing *what happened* (an HTTP request, a file
                upload, a queue message...). Its shape depends on the source.
* ``context`` - an object describing *the invocation itself* (request id,
                remaining time, memory size, function name...).

Whatever you return is sent back to the caller (for synchronous invokes).
"""

import json
import logging
import os

# Code at module level runs ONCE per execution environment (the "init"
# phase / cold start) and is then reused by every warm invocation.
logger = logging.getLogger()
GREETING = os.environ.get("GREETING", "Hello")


def lambda_handler(event, context):
    # API Gateway (HTTP API, payload v2) puts ?name=... here. When you invoke
    # the function directly, you can pass {"name": "..."} instead.
    query = event.get("queryStringParameters") or {}
    name = query.get("name") or event.get("name") or "world"

    logger.info("Saying hello", extra={"visitor": name})

    body = {
        "message": f"{GREETING}, {name}!",
        "requestId": context.aws_request_id,
        "functionName": context.function_name,
        "memoryLimitMB": int(context.memory_limit_in_mb),
        "remainingTimeMs": context.get_remaining_time_in_millis(),
    }

    # This shape (statusCode/headers/body) is what API Gateway expects back.
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
