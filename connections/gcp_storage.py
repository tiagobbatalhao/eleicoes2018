import os
import gcsfs
import gzip

GCP_PROJECT = os.getenv('GCP_PROJECT')
GCP_STORAGE_BUCKET = os.getenv('GCP_STORAGE_BUCKET')

class GCP_storage:

    def __init__(self):
        self.file_system = gcsfs.GCSFileSystem(project=GCP_PROJECT)
        self.bucket = GCP_STORAGE_BUCKET

    def save_file(self, local_path, remote_path):
        """
        Save a file from local_path to GCP_Storage
        """
        self.file_system.put(
            lpath = local_path,
            rpath = f'gs://{self.bucket}/{remote_path}',
        )

    def save_object(self, file_object, remote_path, **kwargs):
        """
        Save a file from an existing object to GCP_Storage
        """
        blocksize = kwargs.get('blocksize') or 5 * 2 ** 20
        rpath = f'gs://{self.bucket}/{remote_path}'
        with self.file_system.open(rpath, 'wb', **kwargs) as f1:
            while True:
                d = file_object.read(blocksize)
                if not d:
                    break
                f1.write(d)

    def read_gzip_file(self, remote_path):
        """
        Read and decompress a gzip file
        """
        rpath = f'gs://{self.bucket}/{remote_path}'
        with self.file_system.open(rpath, 'rb') as f1:
            with gzip.open(f1) as f2:
                while True:
                    line = f2.readline().decode('latin1').replace('\n', '')
                    if not line:
                        break
                    yield line
