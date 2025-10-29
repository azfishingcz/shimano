import os, io, posixpath
from typing import Optional, List
from ftplib import FTP, FTP_TLS, error_perm
from pydrive2.auth import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive

FTP_HOST = os.environ["FTP_HOST"]
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]

TARGET_BASENAME = "ArtExPPLNFBaltic.txt"   # hledaný název (case-insensitive)
FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
TARGET_NAME = "ArtExPPLNFBaltic.txt"       # jak se bude jmenovat na Drive
MAX_DEPTH = 6

def connect_ftp():
    try:
        ftp = FTP(FTP_HOST, timeout=60); ftp.login(FTP_USER, FTP_PASS); ftp.set_pasv(True)
        print("INFO: Connected via plain FTP."); return ftp
    except Exception as e:
        print(f"WARN: Plain FTP failed ({e}). Trying FTPS...")
        ftps = FTP_TLS(FTP_HOST, timeout=60); ftps.login(FTP_USER, FTP_PASS); ftps.prot_p(); ftps.set_pasv(True)
        print("INFO: Connected via FTPS."); return ftps

def listdir_safe(ftp, path: str) -> List[str]:
    try: return ftp.nlst(path)
    except error_perm as e:
        if "550" in str(e): return []
        raise

def is_dir(ftp, path: str) -> bool:
    pwd = ftp.pwd()
    try:
        ftp.cwd(path); ftp.cwd(pwd); return True
    except error_perm:
        return False

def find_file(ftp, start: str, target_basename: str, depth: int = 0) -> Optional[str]:
    if depth > MAX_DEPTH: return None
    start = "/" if start in ("", None) else start
    entries = listdir_safe(ftp, start)
    normalized = [e if e.startswith("/") else posixpath.join(start, e) for e in entries]

    # 1) soubory v aktuální složce
    for p in normalized:
        if posixpath.basename(p).lower() == target_basename.lower():
            print(f"INFO: Found file at {p}")
            return p

    # 2) rekurze do podsložek
    for p in normalized:
        try:
            if is_dir(ftp, p):
                found = find_file(ftp, p, target_basename, depth + 1)
                if found: return found
        except Exception as e:
            print(f"WARN: Skipping {p}: {e}")
            continue
    return None

def download_file(ftp, remote_path: str) -> bytes:
    buf = io.BytesIO()
    dirpath = posixpath.dirname(remote_path) or "/"
    name = posixpath.basename(remote_path)
    if dirpath not in ("", "/"): ftp.cwd(dirpath)
    ftp.retrbinary(f"RETR {name}", buf.write)
    return buf.getvalue()

def gdrive_client():
    sa_json = os.environ["GDRIVE_SA_JSON"]; json_path = "sa.json"
    with open(json_path, "w", encoding="utf-8") as f: f.write(sa_json)
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scopes)
    return GoogleDrive(creds)

def upload_replace_public(drive, content: bytes, name: str, folder_id: str):
    existing = drive.ListFile({"q": f"'{folder_id}' in parents and name='{name}' and trashed=false"}).GetList()
    for f in existing: f.Delete()
    f = drive.CreateFile({"title": name, "parents": [{"id": folder_id}]})
    f.SetContentString(content.decode("utf-8", errors="ignore")); f.Upload()
    f.InsertPermission({"type": "anyone", "role": "reader"})
    f.FetchMetadata(fields="id,webContentLink,webViewLink")
    print("FILE_ID=", f["id"])
    print("DOWNLOAD_LINK=", f["webContentLink"])
    print("VIEW_LINK=", f["webViewLink"])

def main():
    ftp = connect_ftp()
    print("INFO: Searching for:", TARGET_BASENAME)
    found_path = find_file(ftp, "/", TARGET_BASENAME, depth=0)
    if not found_path:
        try:
            root = listdir_safe(ftp, "/"); print("DEBUG: Root entries:", root)
        except Exception as e:
            print("DEBUG: Root listing failed:", e)
        raise FileNotFoundError(f"Soubor {TARGET_BASENAME} nebyl nalezen (MAX_DEPTH={MAX_DEPTH}).")
    data = download_file(ftp, found_path)
    try: ftp.quit()
    except Exception: pass
    drive = gdrive_client()
    upload_replace_public(drive, data, TARGET_NAME, FOLDER_ID)

if __name__ == "__main__":
    main()
