import os
import wget
from multiprocessing import Pool

# PROJECT_ID = "ml-performance-analysis"
# dataset_id = 'nyctaxi'
bucket_name = "nyc-taxi-dataset"


def download_file(ctx):
    url, filename = ctx
    if not os.path.exists(filename):
        print(f"\nDownloading {filename}...")
        wget.download(url, filename)
        os.system(f"gsutil -m cp yellow_tripdata_*.parquet gs://{bucket_name}/trip-data/")
        os.remove(filename)

if __name__ == "__main__":

    pool = Pool(processes=4)

    for year in range(2010, 2014):
        for month in range(1, 13):
            month = str(month).zfill(2)
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
            filename = f"yellow_tripdata_{year}-{month}.parquet"
            ctx = (url, filename)
            pool.apply_async(download_file, args=(ctx,))

    pool.close()
    pool.join()

