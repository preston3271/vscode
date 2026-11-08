# AWS Lambda from Zero: A Hands-On Beginner's Guide

This repository teaches you AWS Lambda using a small, real project: a
**notes service**. It has an HTTP API, a file-import pipeline and a background
worker, and each one is a Lambda function. Every lesson points to working code that has
tests and deploys to AWS with one command.

```
                         ┌──────────────────┐
  curl / browser ──────► │  API Gateway     │──► HelloFunction            (Lesson 2)
                         │  (HTTP API)      │──► NotesApiFunction ──┐     (Lesson 3)
                         └──────────────────┘          │ CRUD       │ enqueue
                                                       ▼            ▼
  aws s3 cp x.csv ──► S3 bucket ──► CsvImporter ──► DynamoDB ◄── WordCount ◄── SQS queue
                     (imports/)     (Lesson 4)      NotesTable   (Lesson 5)     │ 3 failures
                                                                               ▼
                                                                         Dead-letter queue
```

| Path | What it is |
|---|---|
| `src/hello/app.py` | Lesson 2: the smallest useful function |
| `src/notes_api/app.py` | Lesson 3: a REST API with validation, error handling and DynamoDB |
| `src/csv_importer/app.py` | Lesson 4: an S3 upload trigger, with idempotency |
| `src/word_count/app.py` | Lesson 5: an SQS worker with partial batch failures and a DLQ |
| `template.yaml` | Infrastructure as code (AWS SAM) for everything above |
| `tests/` | Offline unit tests (pytest + moto). No AWS account needed |
| `events/` | Sample events for `sam local invoke` |

---

## Table of contents

0. [Lesson 0: The mental model](#lesson-0-the-mental-model)
1. [Lesson 1: Your first function in the AWS Console](#lesson-1-your-first-function-in-the-aws-console)
2. [Lesson 2: Tools, Hello World and your first deploy](#lesson-2-tools-hello-world-and-your-first-deploy)
3. [Lesson 3: A real REST API with DynamoDB](#lesson-3-a-real-rest-api-with-dynamodb)
4. [Lesson 4: Reacting to S3 uploads](#lesson-4-reacting-to-s3-uploads)
5. [Lesson 5: Background work with SQS](#lesson-5-background-work-with-sqs)
6. [Lesson 6: Testing](#lesson-6-testing)
7. [Lesson 7: Logs, metrics, tracing and debugging](#lesson-7-logs-metrics-tracing-and-debugging)
8. [Lesson 8: Permissions, the part everyone gets stuck on](#lesson-8-permissions-the-part-everyone-gets-stuck-on)
9. [Best practices checklist](#best-practices-checklist)
10. [Common errors and fixes](#common-errors-and-fixes)
11. [Cost and cleanup](#cost-and-cleanup)
12. [Where to go next](#where-to-go-next)

---

## Lesson 0: The mental model

**Lambda runs your function when something happens, and you pay only while it runs.**
There are no servers to patch, nothing runs while idle, and it scales from 0 to
thousands of concurrent copies on its own.

### The four words you must know

| Term | Meaning |
|---|---|
| **Handler** | The function AWS calls, e.g. `app.lambda_handler` means "function `lambda_handler` in file `app.py`". |
| **Event** | A dict (JSON) describing what happened. Its shape depends on the source (API Gateway, S3, SQS, etc.). |
| **Context** | Information about the invocation: `aws_request_id`, `get_remaining_time_in_millis()`, and so on. |
| **Trigger / event source** | The thing that invokes your function: an HTTP request, a file upload, a queue message, a schedule. |

```python
def lambda_handler(event, context):
    # event:   what happened
    # context: about this invocation
    return {"anything": "JSON-serializable"}
```

### The execution environment lifecycle (cold starts, explained)

```
 first request                        later requests (seconds/minutes apart)
 ─────────────────────────────────    ─────────────────────────────────────
 [ INIT: download code, start       ] [ INVOKE handler ] [ INVOKE handler ] ...
 [ runtime, run module-level code   ]
 [ INVOKE handler                   ]   ← "warm": init is skipped
   ↑ this extra time = "cold start"
```

* Code **outside** the handler (imports, `boto3` clients, config) runs **once
  per environment** and is reused. Put expensive setup there.
* Code **inside** the handler runs **every invocation**.
* One environment handles **one request at a time**. When 50 requests arrive at once,
  Lambda creates up to 50 environments.
* Environments are frozen between requests and are eventually discarded. **Never
  store state you need in memory or in `/tmp`.** Use DynamoDB, S3 and similar services.

### How functions get invoked

| Style | Who uses it | Who handles errors/retries |
|---|---|---|
| **Synchronous** | API Gateway, a direct `aws lambda invoke`, SDK calls | The caller sees the error |
| **Asynchronous** | S3, SNS, EventBridge | Lambda retries twice, then sends the event to an on-failure destination or DLQ |
| **Polling (event source mapping)** | SQS, Kinesis, DynamoDB Streams | Lambda polls for you and retries according to the queue/stream rules |

### Limits worth memorizing

* Timeout: **max 15 minutes**. Lambda is not for long-running jobs.
* Memory: **128 MB to 10 GB**. **CPU scales with memory**, so more memory often costs *less* because the function finishes sooner.
* `/tmp` storage: 512 MB by default, configurable up to 10 GB.
* Payload: **6 MB** for synchronous request/response. Async invocations are smaller.
* Deployment package: 50 MB zipped / 250 MB unzipped, or 10 GB as a container image.

Quotas change over time. Check the official [Lambda quotas page](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html).

### When *not* to use Lambda

Avoid Lambda for jobs longer than 15 minutes, for steady and very high traffic (a container on ECS or
Fargate can be cheaper), for things that need persistent connections or local state, and for GPU work.

---

## Lesson 1: Your first function in the AWS Console

Start by clicking through the console once so you can see the pieces.

1. Sign in to the AWS Console. Pick a region (e.g. `us-east-1`) and stick with it.
2. Go to **Lambda → Create function → Author from scratch**.
   * Name: `my-first-function`
   * Runtime: **Python 3.13**
   * Architecture: **arm64**
   * Permissions: *Create a new role with basic Lambda permissions*. This role
     lets the function write logs to CloudWatch.
3. Replace the code with:

   ```python
   def lambda_handler(event, context):
       name = event.get("name", "world")
       return {"message": f"Hello, {name}!"}
   ```

4. Click **Deploy**. Your edits do nothing until you deploy.
5. Click **Test**, create a test event with `{"name": "Preston"}`, and run it. You'll see:
   * the **response**: `{"message": "Hello, Preston!"}`
   * **Duration**, **Billed duration** and **Max memory used**
   * a link to the **CloudWatch logs**. Anything you `print()` or log shows up there.
6. Try this: add `import time; time.sleep(5)` and run it again. It fails with
   `Task timed out after 3.00 seconds`. Go to **Configuration → General
   configuration** and raise the timeout. You have now met your first Lambda error.
7. Delete the function when you're done (**Actions → Delete**).

The console is great for exploring. **Real projects define everything in code**,
which is what the rest of this guide does.

---

## Lesson 2: Tools, Hello World and your first deploy

### One-time setup

| Tool | Why | Install |
|---|---|---|
| AWS account | Where things run | <https://aws.amazon.com/free> |
| AWS CLI v2 | Talk to AWS from your terminal | <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html> |
| AWS SAM CLI | Build, test locally and deploy Lambda apps | <https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html> |
| Python 3.13 | Same version as the Lambda runtime | <https://www.python.org/downloads/> |
| Docker *(optional)* | Needed only for `sam local ...` | <https://docs.docker.com/get-docker/> |

**Credentials:** don't use your root account. Create a user in **IAM Identity
Center**, then run:

```bash
aws configure sso          # follow the prompts; name the profile e.g. "dev"
export AWS_PROFILE=dev
aws sts get-caller-identity   # confirms who you are
```

> 💡 Set a **billing alarm** (Billing → Budgets → create a $5 budget). Everything here
> fits in the free tier, but a budget alarm is cheap insurance.

### Read the code

Open [`src/hello/app.py`](src/hello/app.py). It reads `?name=` from the HTTP
request, reads `GREETING` from an environment variable, and returns
an **API Gateway–shaped response**:

```python
return {
    "statusCode": 200,
    "headers": {"Content-Type": "application/json"},
    "body": json.dumps(body),   # body must be a STRING
}
```

Now open [`template.yaml`](template.yaml) and find `HelloFunction`:

```yaml
HelloFunction:
  Type: AWS::Serverless::Function
  Properties:
    CodeUri: src/hello/            # folder that gets zipped and uploaded
    Handler: app.lambda_handler    # file.function
    Environment:
      Variables:
        GREETING: Hello
    Events:
      HelloApi:                    # this block creates the HTTP endpoint
        Type: HttpApi
        Properties: { Path: /hello, Method: GET }
```

`Globals` at the top sets the runtime, memory, timeout, X-Ray tracing and JSON
logging for every function.

**SAM** (Serverless Application Model) is a shorthand for CloudFormation. Those ~15
lines expand into a Lambda function, an IAM role, an API Gateway HTTP API, a route,
an integration and a permission that lets API Gateway invoke the function.

### Run it locally (requires Docker)

```bash
sam build
sam local invoke HelloFunction --event events/hello.json
sam local start-api            # then open http://127.0.0.1:3000/hello?name=you
```

### Deploy it

```bash
sam build
sam deploy --guided
```

Answer the prompts:

* **Stack name**: `lambda-guide`. *Use lowercase*, because it becomes part of an S3 bucket name.
* **Region**: your region
* **Confirm changes before deploy**: `y`. This shows you a "changeset" of what will be created.
* **Allow SAM CLI IAM role creation**: `y`
* **"...may not have authorization defined, Is this okay?"**: `y` (we'll talk about auth in [Where to go next](#where-to-go-next))
* **Save arguments to samconfig.toml**: `y`. Afterwards, plain `sam deploy` is enough.

When it finishes, it prints **Outputs**, including `ApiUrl`:

```bash
API=https://abc123.execute-api.us-east-1.amazonaws.com   # use your ApiUrl
curl "$API/hello?name=Preston"
```

The **edit → `sam build` → `sam deploy`** loop is your daily workflow.
For faster iteration while developing, `sam sync --watch` pushes code changes in seconds.

---

## Lesson 3: A real REST API with DynamoDB

Code: [`src/notes_api/app.py`](src/notes_api/app.py)

```bash
curl -s -X POST "$API/notes" -H 'content-type: application/json' \
     -d '{"title":"First note","content":"hello lambda world","tags":["Learning"]}'
# → 201 {"id":"6b0e...","title":"First note",...}

curl -s "$API/notes?limit=10"
curl -s "$API/notes/<id>"
curl -s -X PUT "$API/notes/<id>" -H 'content-type: application/json' -d '{"content":"edited"}'
curl -s -X DELETE "$API/notes/<id>" -i     # 204
curl -s -X POST "$API/notes" -d '{"title":""}'   # 400 with a helpful message
```

### Concepts introduced, and where to find them in the code

**1. One function, many routes.** API Gateway HTTP API (payload v2) sends a
`routeKey` like `"GET /notes/{id}"`, and a dict maps it to a Python function:

```python
ROUTES = {
    "POST /notes": create_note,
    "GET /notes/{id}": get_note,
    ...
}
```

The alternative is one Lambda per route. Both are valid. One function per *resource*
is simpler to start with.

**2. Reuse clients across invocations.** Creating a `boto3` client takes
tens of milliseconds, so it's done once per execution environment and cached:

```python
_table = None
def get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table
```

**3. Configuration comes from environment variables.** The table name is never
hard-coded. The template injects it with `TABLE_NAME: !Ref NotesTable`, so the
same code works in dev, staging and prod.

**4. Validate every input.** `validate_note()` rejects missing or oversized fields,
wrong types and unknown fields with a clear `400` message. **Never trust the
event body.**

**5. Centralized error handling.** Routes `raise HttpError(404, "Note not
found")`. One `try/except` in `lambda_handler` turns that into a response.
Unexpected exceptions are logged with a full stack trace (`logger.exception`), but
the client only gets `500 Internal server error` plus a `requestId` it can quote
to you. Use that ID to find the log line.

**6. DynamoDB conditional writes.** These avoid race conditions and give you real 404s:

```python
put_item(..., ConditionExpression="attribute_not_exists(id)")   # never overwrite
update_item(..., ConditionExpression="attribute_exists(id)")    # never create on update
# → raises ConditionalCheckFailedException, which we map to 404
```

**7. `Decimal` gotcha.** DynamoDB returns numbers as `Decimal`, which `json.dumps`
rejects. `_json_default()` converts them.

**8. Pagination.** DynamoDB returns at most `Limit` items plus a
`LastEvaluatedKey`. The API base64-encodes that key into an opaque `nextToken`.

**9. Least-privilege permissions.** In the template, the function gets only
`DynamoDBCrudPolicy` on *this table* and `SQSSendMessagePolicy` on *this queue*.

> ⚠️ `GET /notes` uses `Scan`, which reads the whole table. That is fine for learning,
> but in production you would design keys or indexes so you can `Query` instead.

---

## Lesson 4: Reacting to S3 uploads

Code: [`src/csv_importer/app.py`](src/csv_importer/app.py)

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name lambda-guide \
  --query "Stacks[0].Outputs[?OutputKey=='ImportBucketName'].OutputValue" --output text)

aws s3 cp events/sample.csv "s3://$BUCKET/imports/sample.csv"
sam logs -n CsvImporterFunction --stack-name lambda-guide --tail
curl -s "$API/notes"     # the CSV rows are now notes
```

### Concepts introduced

**1. Event-driven and asynchronous.** S3 invokes the function and moves on. Nobody waits
for the return value. If the function raises, Lambda **retries up to 2 more times**.

**2. Events can be batched.** Always loop over `event["Records"]`.

**3. Keys arrive URL-encoded.** `my file.csv` arrives as `my+file.csv`, so always
call `urllib.parse.unquote_plus(key)`.

**4. Idempotency is the most important serverless habit.** AWS delivers events
*at least once*, so you **will** occasionally process the same event twice. The
importer builds each note's ID from `(bucket, key, version, line number)` with
`uuid5`, so a duplicate delivery overwrites the same items instead of creating
duplicates. `test_reprocessing_the_same_event_is_idempotent` checks this.

**5. Filters and the recursion trap.** The trigger only fires for
`imports/*.csv`. If a function ever **writes to the same bucket that triggers
it**, it can trigger itself forever and run up your bill. Use a
different bucket or prefix for output. Lambda has recursive-loop detection, but don't depend on it.

**6. Protect your memory.** The importer refuses files over 5 MB before reading
them. For huge files, stream them or use a different tool, such as Step Functions Distributed Map.

**7. A CloudFormation gotcha.** The bucket has a fixed name (`!Sub
"${AWS::StackName}-imports-${AWS::AccountId}"`) so the IAM policy can reference it
without a *circular dependency* (bucket → notifies function → function's policy →
bucket). This is a common problem with S3 triggers in SAM.

---

## Lesson 5: Background work with SQS

Code: [`src/word_count/app.py`](src/word_count/app.py)

When you create or edit a note, the API doesn't make you wait for analysis. It drops
`{"noteId": "..."}` on an SQS queue and returns right away. Lambda **polls
the queue for you** and invokes `WordCountFunction` with batches of up to 10 messages.

```bash
curl -s -X POST "$API/notes" -H 'content-type: application/json' \
     -d '{"title":"Count me","content":"one two three four"}'
sleep 10
curl -s "$API/notes/<id>"      # now has "wordCount": 4
```

### Concepts introduced

**1. Decouple slow work with a queue.** The API stays fast, and the worker can be
retried, throttled or scaled on its own.

**2. Partial batch failures.** By default, if one message in a batch of 10 fails,
**all 10** go back on the queue. With `FunctionResponseTypes:
[ReportBatchItemFailures]` you return only the failed ones:

```python
return {"batchItemFailures": [{"itemIdentifier": "<messageId>"}]}
```

**3. Permanent vs. transient errors.**

| Error | Example | What we do |
|---|---|---|
| Permanent | Malformed JSON, the note was deleted | Log it and **drop** it. Retrying can never succeed. |
| Transient | Throttling, network blip | Report it as a failure so SQS **retries** it |

**4. Dead-letter queue (DLQ).** After `maxReceiveCount: 3` failed attempts, SQS
moves the message to `WordCountDLQ`. Nothing is lost, and you can inspect and redrive it later.
Set a CloudWatch alarm on the DLQ's `ApproximateNumberOfMessagesVisible`.

**5. Visibility timeout must exceed function timeout.** While Lambda processes a
message it's hidden from other consumers. If the function's timeout is longer than the queue's
visibility timeout, the message reappears mid-processing and runs twice. AWS
recommends a visibility timeout of at least **6× the function timeout**, which is why the template sets 60 s against 10 s.

---

## Lesson 6: Testing

Lambda handlers are plain functions, so testing is just calling them with a
dict.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -v
```

The tests use [**moto**](https://github.com/getmoto/moto), which fakes DynamoDB,
S3 and SQS in memory. They run in about 2 seconds, need no AWS account and cost nothing.

Things the tests cover:

* the full CRUD lifecycle and pagination (`test_notes_api.py`)
* validation errors, table-driven with `pytest.mark.parametrize`
* that internal errors **don't leak** to clients
* the S3 importer's URL decoding, rejected rows and **idempotency**
* the SQS worker's partial-batch-failure behavior

How the tests work:

* `conftest.py` sets **fake AWS credentials**, so a test can never touch a real account.
* A fake `context` (`SimpleNamespace`) stands in for the real one.
* Each test resets the module's cached clients so it gets moto's fakes.

**The testing pyramid for Lambda:**

1. **Unit tests** (here): fast, run on every save.
2. **Local invoke** (`sam local invoke -e events/...`): checks packaging and the runtime.
   `sam local generate-event s3 put` produces realistic sample events.
3. **Integration tests against a real dev stack**: deploy with
   `sam deploy --stack-name lambda-guide-dev`, then run `curl` or pytest against `ApiUrl`.
   This is the only way to catch IAM and configuration mistakes.

---

## Lesson 7: Logs, metrics, tracing and debugging

**Logs.** Everything printed or logged goes to CloudWatch Logs, in log group
`/aws/lambda/<function-name>`. The template sets `LogFormat: JSON`, so this

```python
logger.info("Imported file", extra={"key": key, "imported": 2})
```

becomes a searchable JSON line that includes the `requestId`. Tail logs from your terminal:

```bash
sam logs -n NotesApiFunction --stack-name lambda-guide --tail
```

Query them with **CloudWatch Logs Insights**:

```
fields @timestamp, level, message, route, status
| filter level = "ERROR"
| sort @timestamp desc
```

**Metrics.** These are free and automatic: `Invocations`, `Errors`, `Duration`, `Throttles`,
`ConcurrentExecutions`. Alarm on `Errors` and on the DLQ depth.

**Tracing.** `Tracing: Active` turns on AWS X-Ray, which shows a timeline of each
request (Lambda → DynamoDB → SQS) so you can see where time goes.

**The `REPORT` line.** Every invocation ends with:

```
REPORT RequestId: ... Duration: 12.3 ms Billed Duration: 13 ms Memory Size: 256 MB
Max Memory Used: 78 MB Init Duration: 310.2 ms
```

`Init Duration` appears only on cold starts. If `Max Memory Used` is close to
`Memory Size`, raise the memory.

**Power-up:** [Powertools for AWS Lambda (Python)](https://docs.powertools.aws.dev/lambda/python/latest/)
provides structured logging, metrics, tracing, idempotency, batch processing, an event
parser and an API router. It's the standard toolkit once you know the basics, and the
code in this repo is a hand-written version of what it automates.

---

## Lesson 8: Permissions, the part everyone gets stuck on

Two kinds of permission are involved:

```
   WHO CAN CALL MY FUNCTION?                 WHAT CAN MY FUNCTION DO?
   (resource-based policy)                   (execution role)
   API Gateway ──invoke──►  [ Lambda ]  ──►  DynamoDB / S3 / SQS / logs
   S3          ──invoke──►
```

* **Execution role**: the IAM role your code runs as. A `boto3` call succeeds only if
  this role allows it. SAM `Policies:` entries such as `DynamoDBCrudPolicy` add to it.
* **Resource-based policy**: lets other services invoke your function. SAM adds these
  automatically when you declare `Events:`.

`AccessDeniedException ... is not authorized to perform: dynamodb:PutItem on
resource ...` almost always means the **execution role** is missing a permission.
Add the right policy to that function in the template.

**Rules:** grant the minimum (one table, not `*`), use one role per function (SAM does this
for you), and never put access keys in code. The role provides temporary credentials automatically.

**Secrets** such as API keys and DB passwords don't belong in plain environment variables.
Store them in **AWS Secrets Manager** or **SSM Parameter Store**, grant the role read
access, and fetch them once during init so they're cached.

---

## Best practices checklist

- [ ] Infrastructure as code (SAM, CDK, Terraform). Avoid console-only resources.
- [ ] Expensive setup (clients, config, secrets) happens **outside** the handler.
- [ ] Configuration comes from environment variables. Nothing is hard-coded.
- [ ] Every input is validated. Clients get clean 4xx/5xx errors, with no stack traces.
- [ ] Handlers are **idempotent**, because events arrive at least once.
- [ ] Least-privilege IAM, with one role per function.
- [ ] Timeouts are set deliberately. For SQS, visibility timeout is at least 6× the function timeout.
- [ ] Async and queue sources have a **DLQ or on-failure destination**, plus an alarm.
- [ ] Structured JSON logs, and alarms on `Errors`, `Throttles` and DLQ depth.
- [ ] CloudWatch log retention is set. The default is *never expire*. Add an
      `AWS::Logs::LogGroup` with `RetentionInDays`.
- [ ] Memory is tuned. Try [AWS Lambda Power Tuning](https://github.com/alexcasalboni/aws-lambda-power-tuning).
- [ ] Deployment packages are small, with only the dependencies you need, so cold starts are faster.
- [ ] Separate stacks or accounts for dev and prod, deployed from CI/CD.

---

## Common errors and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| `Task timed out after 3.00 seconds` | Default timeout too low, or a hung network call | Raise `Timeout`. Set SDK timeouts. Check VPC networking. |
| `Runtime.ImportModuleError: No module named 'requests'` | Dependency not packaged | Add `requirements.txt` in the function's `CodeUri` folder and run `sam build` |
| `Runtime.HandlerNotFound` | `Handler:` doesn't match `file.function` | e.g. `app.lambda_handler` for `def lambda_handler` in `app.py` |
| `AccessDeniedException ... not authorized to perform` | Execution role missing permission | Add a SAM policy template or IAM statement |
| API returns `{"message":"Internal Server Error"}` | Function raised, or returned the wrong shape | Return `statusCode` + string `body`. Check the logs. |
| `Object of type Decimal is not JSON serializable` | DynamoDB numbers | Use a `default=` converter like `_json_default` |
| Same SQS message processed repeatedly | Visibility timeout < function timeout, or an unhandled exception | Raise the visibility timeout. Use partial batch responses. |
| Bill spike from an S3 trigger | Function writes to the bucket that triggers it | Write output to a different bucket or prefix |
| `Circular dependency between resources` (S3 + SAM) | Policy references `!Ref Bucket` | Use a fixed bucket name, as in `template.yaml` |

---

## Cost and cleanup

Lambda's free tier includes **1 million requests and 400,000 GB-seconds per
month**. After that you pay per request and per GB-second of duration. DynamoDB on-demand, SQS,
S3 and API Gateway HTTP APIs are also pay-per-use. Learning with this project
should cost **$0 to a few cents**. Check current prices on the
[Lambda pricing page](https://aws.amazon.com/lambda/pricing/).

**Tear everything down when you're done:**

```bash
aws s3 rm "s3://$BUCKET" --recursive   # buckets must be empty before deletion
sam delete --stack-name lambda-guide
```

---

## Where to go next

1. **Auth:** add a JWT authorizer (Amazon Cognito) to the HTTP API so only signed-in users can call `/notes`.
2. **Powertools:** rewrite `notes_api` using Powertools' `APIGatewayHttpResolver`, `Logger` and `BatchProcessor`.
3. **Scheduling:** add an `Events: { Type: ScheduleV2 }` function that emails a daily digest.
4. **Orchestration:** use Step Functions for multi-step workflows instead of chaining Lambdas.
5. **CI/CD:** run `pytest` and then `sam deploy` from GitHub Actions using OIDC (no stored keys).
6. **Other languages:** the same concepts apply to Node.js, Java, Go (`provided.al2023`), .NET and Ruby.
7. **Docs:** the [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
   and [Serverless Land](https://serverlessland.com/) patterns.

### Exercises

1. Add a `GET /notes?tag=work` filter. Then design a GSI so it uses `Query` instead of `Scan`.
2. Make `POST /notes` idempotent using an `Idempotency-Key` header.
3. Make the importer write a summary report to `s3://<bucket>/reports/`. Think about the recursion trap first.
4. Add a CloudWatch alarm on the DLQ to the template.
5. Write a test that proves an `update` without `content` does **not** enqueue a word-count job.
