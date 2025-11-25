use futures::stream::StreamExt;
use object_store::WriteMultipart;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path as ObjectPath};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct S3Client {
    store: Box<dyn ObjectStore>,
}

impl S3Client {
    pub fn new(
        url: &str,
        region: &str,
        bucket: &str,
        key_id: &str,
        secret_key: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let store = AmazonS3Builder::new()
            .with_endpoint(url)
            .with_allow_http(true)
            .with_region(region)
            .with_bucket_name(bucket)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret_key)
            .build()?;

        Ok(S3Client {
            store: Box::new(store),
        })
    }

    // Stream any AsyncRead (e.g., ChildStdout) without buffering entire output
    pub async fn upload_stream<R: AsyncRead + Unpin>(
        &self,
        mut stream: R,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // S3 multipart has an object size of max 5TB, with each part between 5MB and 5GB.
        // The max number of parts is 10,000.
        // Since we do not know the total size in advance, we will use a part size of 500MB to
        // cover the max use case.
        const UPLOAD_BUFFER_SIZE: usize = 500 * 1024 * 1024; // 500MB
        const MAX_CONCURRENT_UPLOADS: usize = 1; // Number of concurrent uploads

        let upload = self.store.put_multipart(&ObjectPath::from(key)).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, UPLOAD_BUFFER_SIZE);

        let mut buf = vec![0u8; UPLOAD_BUFFER_SIZE];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            writer.wait_for_capacity(MAX_CONCURRENT_UPLOADS).await?;
            writer.write(&buf[..n]);
        }

        writer.finish().await?;
        Ok(())
    }

    pub async fn list_objects(
        &self,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut object_keys = Vec::new();
        let mut stream = self.store.list(None);

        while let Some(meta) = stream.next().await.transpose()? {
            object_keys.push(meta.location.to_string());
        }
        Ok(object_keys)
    }

    pub async fn delete_object(
        &self,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.store.delete(&ObjectPath::from(key)).await?;
        Ok(())
    }
}
