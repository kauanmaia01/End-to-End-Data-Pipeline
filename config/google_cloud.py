from google.cloud import storage
from io import BytesIO
import pandas as pd


class GoogleCloudStorage:

    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def create_bucket(self):
        try:
            bucket = self.client.create_bucket(self.bucket_name)
            print(f"Bucket criado: {self.bucket_name}")
            return bucket
        except Exception as e:
            print("Erro ao criar bucket:", e)
            raise


    def upload_parquet(self, object_name, df: pd.DataFrame):
        blob = self.bucket.blob(object_name)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        blob.upload_from_file(buffer, content_type="application/octet-stream")


    def download_parquet(self, object_name) -> pd.DataFrame:
        blob = self.bucket.blob(object_name)
        data = blob.download_as_bytes()
        return pd.read_parquet(BytesIO(data))


    def delete_object(self, object_name):
        blob = self.bucket.blob(object_name)
        blob.delete()


# gcs = GoogleCloudStorage("kauan-investment-bucket-2026-xyz")

# gcs.create_bucket()

# Upload
# gcs.upload_parquet("dados.parquet", df)

# Download
# df_parquet = gcs.download_parquet("dados.parquet")