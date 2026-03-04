import urllib.request
import zipfile
import io
import os

url = "https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip"
print(f"Downloading {url}...")
response = urllib.request.urlopen(url)
print("Download complete. Extracting...")
with zipfile.ZipFile(io.BytesIO(response.read())) as z:
    for file_info in z.infolist():
        if "financial" in file_info.filename and ("financial.sqlite" in file_info.filename or ".csv" in file_info.filename):
            z.extract(file_info, path=".")
            print(f"Extracted {file_info.filename}")
print("Done!")
