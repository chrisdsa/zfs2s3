# List all files in a bucket
aws s3 ls s3://bucket-name --recursive

# Get detailed information including ETag (checksum)
aws s3api list-objects-v2 --bucket bucket-name

# Get checksum of a specific file
aws s3api get-object \
--bucket bucket \
--key "object" \
--endpoint-url http://localhost:3900 \
/dev/stdout | sha256sum

# List all files
aws s3 ls s3://bucket/ --recursive --endpoint-url http://localhost:3900
