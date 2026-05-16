import json
import base64
import boto3
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
import jwt  # PyJWT

# ── Secrets Manager ───────────────────────────────────────────
def get_secrets(secret_name):
    client = boto3.client("secretsmanager", region_name="ap-south-1")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# ── Generate Snowflake JWT token ──────────────────────────────
def generate_jwt(account, user, private_key_str):
    # Load private key
    private_key = serialization.load_pem_private_key(
        private_key_str.encode("utf-8"),
        password=None,
        backend=default_backend()
    )

    # Get public key fingerprint
    public_key = private_key.public_key()
    public_key_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    import hashlib
    sha256_hash = hashlib.sha256(public_key_der).digest()
    public_key_fp = "SHA256:" + base64.b64encode(sha256_hash).decode("utf-8")

    # Build JWT payload
    account_identifier = account.upper()
    user_upper = user.upper()

    qualified_name = f"{account_identifier}.{user_upper}"

    now = datetime.now(timezone.utc)
    payload = {
        "iss": f"{qualified_name}.{public_key_fp}",
        "sub": qualified_name,
        "iat": now,
        "exp": now + timedelta(minutes=59),
    }

    token = jwt.encode(payload, private_key, algorithm="RS256")
    return token

# ── Snowflake REST API call ───────────────────────────────────
def execute_snowflake_query(account, token, warehouse, database, schema, role, sql, bindings=None):
    url = f"https://{account}.snowflakecomputing.com/api/v2/statements"

    body = {
        "statement": sql,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "role": role,
    }

    if bindings:
        body["bindings"] = bindings

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))

# ── Write single record to Snowflake ─────────────────────────
def write_to_snowflake(secrets, token, record):
    sql = """
        INSERT INTO CRYPTO_DB.RAW.CRYPTO_PRICES_RAW
            (RAW_DATA, COIN_ID, INGESTED_AT, SOURCE)
        SELECT
            PARSE_JSON(:1),
            :2,
            :3::TIMESTAMP_NTZ,
            'coingecko'
    """
    bindings = {
        "1": {"type": "TEXT", "value": json.dumps(record)},
        "2": {"type": "TEXT", "value": record["id"]},
        "3": {"type": "TEXT", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")},
    }

    execute_snowflake_query(
        account=secrets["SNOWFLAKE_ACCOUNT"],
        token=token,
        warehouse=secrets["SNOWFLAKE_WAREHOUSE"],
        database=secrets["SNOWFLAKE_DATABASE"],
        schema=secrets["SNOWFLAKE_SCHEMA"],
        role=secrets["SNOWFLAKE_ROLE"],
        sql=sql,
        bindings=bindings,
    )
    print(f"Inserted: {record['id']} @ ${record['current_price']}")

# ── Lambda handler ────────────────────────────────────────────
def lambda_handler(event, context):
    print(f"[{datetime.now(timezone.utc).isoformat()}] Consumer triggered")

    # 1. Load secrets
    import os
    secret_name = os.environ["SECRET_NAME"]
    secrets = get_secrets(secret_name)

    # 2. Generate JWT
    token = generate_jwt(
        account=secrets["SNOWFLAKE_ACCOUNT"],
        user=secrets["SNOWFLAKE_USER"],
        private_key_str=secrets["SNOWFLAKE_PRIVATE_KEY"],
    )
    print("JWT generated ✅")

    # 3. Process Kafka messages
    # MSK trigger sends records in batches
    records = event.get("records", {})
    total = 0

    for topic_partition, messages in records.items():
        for message in messages:
            # Decode base64 encoded Kafka message value
            raw_value = base64.b64decode(message["value"]).decode("utf-8")
            record = json.loads(raw_value)

            print(f"Processing: {record.get('id')} @ ${record.get('current_price')}")
            write_to_snowflake(secrets, token, record)
            total += 1

    print(f"Total inserted: {total} records")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "records_processed": total,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    }