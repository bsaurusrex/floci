# DynamoDB

**Protocol:** JSON 1.1 (`X-Amz-Target: DynamoDB_20120810.*`)
**Endpoint:** `POST http://localhost:4566/`

## Supported Actions

| Action | Description |
|---|---|
| `CreateTable` | Create a table with indexes |
| `DeleteTable` | Delete a table |
| `DescribeTable` | Get table metadata |
| `ListTables` | List all tables |
| `UpdateTable` | Update throughput, indexes, streams |
| `PutItem` | Write an item |
| `GetItem` | Read an item by primary key |
| `DeleteItem` | Delete an item |
| `UpdateItem` | Partially update an item |
| `Query` | Query by partition key with optional filter |
| `Scan` | Full table scan with optional filter |
| `BatchWriteItem` | Write/delete up to 25 items across tables |
| `BatchGetItem` | Read up to 100 items across tables |
| `TransactWriteItems` | ACID write transaction |
| `TransactGetItems` | ACID read transaction |
| `DescribeTimeToLive` | Get TTL configuration |
| `UpdateTimeToLive` | Enable/disable TTL on a table |
| `TagResource` | Tag a table |
| `UntagResource` | Remove tags |
| `ListTagsOfResource` | List tags |
| `DescribeContinuousBackups` | Get PITR backup configuration |
| `UpdateContinuousBackups` | Enable/disable PITR |
| `DescribeKinesisStreamingDestination` | List Kinesis streaming destinations |
| `EnableKinesisStreamingDestination` | Enable Kinesis streaming for a table |
| `DisableKinesisStreamingDestination` | Disable Kinesis streaming for a table |
| `ExportTableToPointInTime` | Export table data to S3 as gzip NDJSON |
| `DescribeExport` | Get export status and metadata |
| `ListExports` | List exports, optionally filtered by table ARN |

## Streams {#streams}

DynamoDB Streams are supported via a separate target (`DynamoDBStreams_20120810`):

| Action | Description |
|---|---|
| `ListStreams` | List all streams |
| `DescribeStream` | Get stream and shard info |
| `GetShardIterator` | Get a shard iterator |
| `GetRecords` | Read stream records from a shard |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_SERVICES_DYNAMODB_ENABLED` | `true` | Enable or disable the service |
| `FLOCI_STORAGE_SERVICES_DYNAMODB_MODE` | *(global default)* | Storage mode override for DynamoDB (`memory`, `persistent`, `hybrid`, `wal`) |
| `FLOCI_STORAGE_SERVICES_DYNAMODB_FLUSH_INTERVAL_MS` | `5000` | Flush interval for `hybrid`/`wal` storage modes (milliseconds) |

## Examples

```bash
export AWS_ENDPOINT_URL=http://localhost:4566

# Create a table
aws dynamodb create-table \
  --table-name Users \
  --attribute-definitions \
    AttributeName=userId,AttributeType=S \
  --key-schema \
    AttributeName=userId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url $AWS_ENDPOINT_URL

# Put an item
aws dynamodb put-item \
  --table-name Users \
  --item '{"userId":{"S":"u1"},"name":{"S":"Alice"},"age":{"N":"30"}}' \
  --endpoint-url $AWS_ENDPOINT_URL

# Get an item
aws dynamodb get-item \
  --table-name Users \
  --key '{"userId":{"S":"u1"}}' \
  --endpoint-url $AWS_ENDPOINT_URL

# Query (partition key)
aws dynamodb query \
  --table-name Users \
  --key-condition-expression "userId = :id" \
  --expression-attribute-values '{":id":{"S":"u1"}}' \
  --endpoint-url $AWS_ENDPOINT_URL

# Scan with filter
aws dynamodb scan \
  --table-name Users \
  --filter-expression "age > :min" \
  --expression-attribute-values '{":min":{"N":"25"}}' \
  --endpoint-url $AWS_ENDPOINT_URL

# Enable TTL
aws dynamodb update-time-to-live \
  --table-name Users \
  --time-to-live-specification Enabled=true,AttributeName=expiresAt \
  --endpoint-url $AWS_ENDPOINT_URL

# Enable Streams
aws dynamodb update-table \
  --table-name Users \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --endpoint-url $AWS_ENDPOINT_URL
```

## Global Secondary Indexes

```bash
aws dynamodb create-table \
  --table-name Orders \
  --attribute-definitions \
    AttributeName=orderId,AttributeType=S \
    AttributeName=customerId,AttributeType=S \
  --key-schema AttributeName=orderId,KeyType=HASH \
  --global-secondary-indexes '[{
    "IndexName": "CustomerIndex",
    "KeySchema": [{"AttributeName":"customerId","KeyType":"HASH"}],
    "Projection": {"ProjectionType":"ALL"}
  }]' \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url $AWS_ENDPOINT_URL
```

## Export to S3

Export table data to an S3 bucket as gzip-compressed NDJSON (DynamoDB JSON format):

```bash
# Create a bucket to receive the export
aws s3 mb s3://my-exports --endpoint-url $AWS_ENDPOINT_URL

# Start an export
EXPORT_ARN=$(aws dynamodb export-table-to-point-in-time \
  --table-arn arn:aws:dynamodb:us-east-1:000000000000:table/Users \
  --s3-bucket my-exports \
  --s3-prefix exports \
  --export-format DYNAMODB_JSON \
  --query ExportDescription.ExportArn --output text \
  --endpoint-url $AWS_ENDPOINT_URL)

# Poll until COMPLETED
aws dynamodb describe-export \
  --export-arn $EXPORT_ARN \
  --query ExportDescription.ExportStatus \
  --endpoint-url $AWS_ENDPOINT_URL

# List exports for a table
aws dynamodb list-exports \
  --table-arn arn:aws:dynamodb:us-east-1:000000000000:table/Users \
  --endpoint-url $AWS_ENDPOINT_URL
```

The export writes to `s3://<bucket>/<prefix>/AWSDynamoDB/<exportId>/data/` as one or more `.json.gz` files, along with `manifest-summary.json` and `manifest-files.json` — the same layout as real AWS DynamoDB exports.
```
## Container-backed data plane (opt-in)

By default Floci serves DynamoDB entirely in-process. Setting the backend to `container` delegates
item storage, expression evaluation, PartiQL and transactions to an `amazon/dynamodb-local`
container, so those semantics come from AWS's own downloadable engine:

```bash
FLOCI_SERVICES_DYNAMODB_BACKEND=container
```

The container is started on first use and stopped with the emulator. It needs the Docker socket,
the same as Lambda, RDS and the other Docker-backed services.

**What stays in Floci.** The control plane is unchanged, so every parameter Floci already accepts
keeps round-tripping: table metadata and ARNs, `DescribeTable`, tags, PITR/continuous backups,
`ExportTableToPointInTime`, Kinesis streaming destinations, and stream fan-out to Lambda event
source mappings and EventBridge Pipes. DynamoDB local models none of those.

**How accounts and regions stay separated.** DynamoDB local keeps a separate store per
(access key id, region) unless `-sharedDb` is passed, which Floci deliberately omits. Floci sends
the caller's resolved account id as the access key, so the container's partitioning matches
Floci's own account and region scoping without renaming tables.

**How streams keep working.** Writes no longer pass through Floci, so before/after images cannot be
captured inline — and they cannot be rebuilt from the forwarded calls either, because
`ReturnValues=ALL_OLD` does not exist on `BatchWriteItem` or `TransactWriteItems`. The mirrored
table therefore always carries `NEW_AND_OLD_IMAGES`, and Floci replays the container's stream into
its own, applying the caller's configured `StreamViewType` on the way out. Consumers see no
difference.

### Known divergences in this mode

These come from downloadable DynamoDB itself and apply only when `backend=container`:

| Behaviour | Native backend | Container backend |
|---|---|---|
| Table-name case sensitivity | `Authors` and `authors` are distinct, as in AWS | Names are case-insensitive; the second create fails |
| `TagResource` / `ListTagsOfResource` | Supported | Served by Floci; never reaches the container |
| Point-in-time recovery | Supported | Served by Floci; DynamoDB local has no PITR |
| `billingModeSummary` | Populated | Served by Floci |
| Item collection metrics | Populated | Container returns nulls |
| `TransactionConflictException` | Raised | Never raised by DynamoDB local |

### Not yet mirrored

Tables created through CloudFormation (`AWS::DynamoDB::Table`) go through
`DynamoDbService.createTable` rather than the JSON handler, so they are not mirrored into the
container and their data plane will not work in this mode. The same applies to the legacy
`states:::dynamodb:*` Step Functions task path and IoT rule actions, which call the service
directly. Use the native backend for those.

The container runs with `-inMemory`, so it starts empty. With a persistent or hybrid storage mode
Floci reloads its table definitions across a restart while the container does not, leaving the
data plane returning `ResourceNotFoundException` for tables Floci still lists. Recreate the tables,
or run this mode with `FLOCI_STORAGE_SERVICES_DYNAMODB_MODE=memory` so both sides forget together.
