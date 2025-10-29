import os, io, posixpath, sys
from typing import Optional, List, Tuple
from ftplib import FTP, FTP_TLS, error_perm, all_errors
from pydrive2.auth import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive

FTP_HOST = os.environ["FTP_HOST"]
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]

TARGET_BASENAME = "ArtExPPLNFBaltic.txt"   # co hledáme (case-insensitive)
FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
TARGET_NAME = "ArtExPPLNFBaltic.txt"
MAX_DEPTH = int(os.environ.get("MAX_DEPTH", "6"))

def log(msg: str):
    print(msg, flush=True)

def try_connect(mode: str) -> Tuple[object, str]:
    """
    mode: 'FTP_PASV' | 'FTP_ACTIVE' | 'FTPS_PASV' | 'FTPS_ACTIVE'
    Vrací (ftp_obj, popis_rezimu)
    """
    try:
        if mode == "FTP_PASV":
            ftp = FTP(FTP_HOST, timeout=60)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(True)
            return ftp, "Connected via FTP (PASV)"
        if mode == "FTP_ACTIVE":
            ftp = FTP(FTP_HOST, timeout=60)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(False)
            return ftp, "Connected via FTP (ACTIVE)"
        if mode == "FTPS_PASV":
            ftp = FTP_TLS(FTP_HOST, timeout=60)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.prot_p()
            ftp.set_pasv(True)
            return ftp, "Connected via FTPS (PASV)"
        if mode == "FTPS_ACTIVE":
            ftp = FTP_TLS(FTP_HOST, timeout=60)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.prot_p()
            ftp.set_pasv(False)
            return ftp, "Connected via FTPS (ACTIVE)"
        raise RuntimeError("Unknown mode")
    except all_errors as e:
        raise RuntimeError(f"CONNECT_FAIL[{mode}]: {e}")

def listdir_safe(ftp, path: str) -> List[str]:
    try:
        return ftp.nlst(path)
    except error_perm as e:
        if "550" in str(e):
            return []
        raise

def is_dir(ftp, path: str) -> bool:
    pwd = ftp.pwd()
    try:
        ftp.cwd(path)
        ftp.cwd(pwd)
        return True
    except error_perm:
        return False

def find_file(ftp, start: str, target_basename: str, depth: int = 0) -> Optional[str]:
    if depth > MAX_DEPTH:
        return None
    start = "/" if start in ("", None) else start

    try:
        entries = listdir_safe(ftp, start)
    except Exception as e:
        log(f"DIR_LIST_ERROR at '{start}': {e}")
        return None

    normalized = [e if e.startswith("/") else posixpath.join(start, e) for e in entries]
    # soubory v aktuální složce
    for p in normalized:
        if posixpath.basename(p).lower() == target_basename.lower():
            log(f"FOUND: {p}")
            return p
    # rekurze do podsložek
    for p in normalized:
        try:
            if is_dir(ftp, p):
                found = find_file(ftp, p, target_basename, depth + 1)
                if found:
                    return found
        except Exception as e:
            log(f"WARN: skip '{p}': {e}")
            continue
    return None

def download_file(ftp, remote_path: str) -> bytes:
    buf = io.BytesIO()
    dirpath = posixpath.dirname(remote_path) or "/"
    name = posixpath.basename(remote_path)
    if dirpath not in ("", "/"):
        ftp.cwd(dirpath)
        log(f"CWD: {dirpath}")
    log(f"RETR: {name}")
    ftp.retrbinary(f"RETR {name}", buf.write)
    return buf.getvalue()

def gdrive_client():
    try:
        sa_json = os.environ["GDRIVE_SA_JSON"]
    except KeyError:
        log("ERROR: chybí secret GDRIVE_SA_JSON"); sys.exit(20)
    json_path = "sa.json"
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(sa_json)
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scopes)
    return GoogleDrive(creds)

def upload_replace_public(drive, content: bytes, name: str, folder_id: str):
    try:
        existing = drive.ListFile({"q": f"'{folder_id}' in parents and name='{name}' and trashed=false"}).GetList()
    except Exception as e:
        log(f"DRIVE_LIST_ERROR: {e}"); sys.exit(30)
    for f in existing:
        try:
            f.Delete()
        except Exception as e:
            log(f"DRIVE_DELETE_WARN: {e}")
    f = drive.CreateFile({"title": name, "parents": [{"id": folder_id}]})
    f.SetContentString(content.decode("utf-8", errors="ignore"))
    log("DRIVE_UPLOAD: uploading…")
    f.Upload()
    f.InsertPermission({"type": "anyone", "role": "reader"})
    f.FetchMetadata(fields="id,webContentLink,webViewLink")
    log(f"FILE_ID= {f['id']}")
    log(f"DOWNLOAD_LINK= {f['webContentLink']}")
    log(f"VIEW_LINK= {f['webViewLink']}")

def main():
    log(f"START: host={FTP_HOST}, user={FTP_USER}, target={TARGET_BASENAME}, depth={MAX_DEPTH}")
    modes = ["FTP_PASV", "FTP_ACTIVE", "FTPS_PASV", "FTPS_ACTIVE"]
    last_err = None
    ftp = None
    chosen = None

    # 1) Připojení
    for m in modes:
        try:
            ftp, chosen = try_connect(m)
            log(f"OK: {chosen}")
            break
        except RuntimeError as e:
            log(str(e))
            last_err = e
            continue
    if not ftp:
        log("ERROR: Nepodařilo se připojit žádným režimem.")
        sys.exit(10)

    # 2) Výpis rootu (pro debug)
    try:
        pwd = ftp.pwd()
        root_entries = listdir_safe(ftp, "/")
        log(f"PWD: {pwd}")
        log(f"ROOT_ENTRIES({len(root_entries)}): {root_entries[:50]}")  # omezíme délku
    except Exception as e:
        log(f"ROOT_LIST_ERROR: {e}")

    # 3) Hledání souboru
    log(f"SEARCH: {TARGET_BASENAME}")
    found = find_file(ftp, "/", TARGET_BASENAME, 0)
    if not found:
        log("ERROR: Soubor nebyl nalezen (možné příčiny: jiný název, jiná složka, přístupová práva).")
        try:
            log(f"TIP: Zkus ručně ve Windows Exploreru prohlédnout strukturu a napiš mi přesnou cestu.")
        except Exception:
            pass
        try:
            ftp.quit()
        except Exception:
            pass
        sys.exit(11)

    # 4) Stažení
    try:
        data = download_file(ftp, found)
        log(f"OK: staženo {len(data)} B")
    except all_errors as e:
        log(f"ERROR: RETR selhalo: {e}")
        sys.exit(12)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    # 5) Upload na Drive
    try:
        drive = gdrive_client()
        upload_replace_public(drive, data, TARGET_NAME, FOLDER_ID)
        log("DONE: Upload hotov.")
    except Exception as e:
        log(f"ERROR: DRIVE selhal: {e}")
        sys.exit(31)

if __name__ == "__main__":
    main()
