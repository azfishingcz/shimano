import os
from ftplib import FTP
import io
from pydrive2.auth import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive

FTP_HOST = os.environ["FTP_HOST"]
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]
FTP_FILE = "ArtExPPLNFBaltic.txt"

FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
TARGET_NAME = "ArtExPPLNFBaltic.txt"  # jak se bude jmenovat na Drive

def download_from_ftp() -> bytes:
    ftp = FTP(FTP_HOST, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {FTP_FILE}", buf.write)
    ftp.quit()
    return buf.getvalue()

def gdrive_client():
    sa_json = os.environ["GDRIVE_SA_JSON"]
    json_path = "sa.json"
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(sa_json)
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scopes)
    drive = GoogleDrive(creds)
    return drive

def upload_replace_public(drive, content: bytes, name: str, folder_id: str):
    existing = drive.ListFile({
        "q": f"'{folder_id}' in parents and name='{name}' and trashed=false"
    }).GetList()
    for f in existing:
        f.Delete()

    f = drive.CreateFile({"title": name, "parents": [{"id": folder_id}]})
    f.SetContentString(content.decode("utf-8", errors="ignore"))
    f.Upload()

    f.InsertPermission({"type": "anyone", "role": "reader"})
    f.FetchMetadata(fields="id,webContentLink,webViewLink")
    print("FILE_ID=", f["id"])
    print("DOWNLOAD_LINK=", f["webContentLink"])
    print("VIEW_LINK=", f["webViewLink"])

def main():
    data = download_from_ftp()
    drive = gdrive_client()
    upload_replace_public(drive, data, TARGET_NAME, FOLDER_ID)

if __name__ == "__main__":
    main()
