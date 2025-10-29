import os
import io
import posixpath
from typing import Optional, List

# === Google Drive (upload) ===
from pydrive2.auth import ServiceAccountCredentials
from pydrive2.drive import GoogleDrive

# === FTP / FTPS ===
from ftplib import FTP, FTP_TLS, error_perm

FTP_HOST = os.environ["FTP_HOST"]
FTP_USER = os.environ["FTP_USER"]
FTP_PASS = os.environ["FTP_PASS"]
TARGET_BASENAME = "ArtExPPLNFBaltic.txt"  # co hledáme (case-insensitive)

FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
TARGET_NAME = "ArtExPPLNFBaltic.txt"     # jak se bude jmenovat na Drive

MAX_DEPTH = 3  # jak hluboko procházet strom

def connect_ftp():
    """
    Zkusí obyčejné FTP, při chybě zkusí FTPS (explicit TLS).
    Vrací přihlášenou instanci.
    """
    try:
        ftp = FTP(FTP_HOST, timeout=60)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.set_pasv(True)
        print("INFO: Connected via plain FTP.")
        return ftp
    except Exception as e:
        print(f"WARN: Plain FTP failed ({e}). Trying FTPS...")
        ftps = FTP_TLS(FTP_HOST, timeout=60)
        ftps.login(FTP_USER, FTP_PASS)
        ftps.prot_p()    # data connection protected
        ftps.set_pasv(True)
        print("INFO: Connected via FTPS.")
        return ftps

def listdir_safe(ftp, path: str) -> List[str]:
    """
    Vrátí seznam položek v adresáři path (jen jména).
    Používá NLST kvůli přenositelnosti.
    """
    try:
        return ftp.nlst(path)
    except error_perm as e:
        # Některé servery vrací 550 i pro prázdné složky – ošetřeno
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
    """
    DFS prohledání FTP stromu od 'start' do hloubky MAX_DEPTH,
    vrací plnou cestu k souboru (POSIX, např. '/folder/file.txt').
    """
    if depth > MAX_DEPTH:
        return None

    # normalizace: při rootu některé servery chtějí "" místo "/"
    start = "/" if start in ("", None) else start

    entries = listdir_safe(ftp, start)
    # NLST může vracet buď plná jména, nebo jen názvy – sjednotíme na plné cesty
    normalized = []
    for e in entries:
        # pokud už je to plná cesta, bereme jak je, jinak připojíme ke 'start'
        if e.startswith("/"):
            normalized.append(e)
        else:
            normalized.append(posixpath.join(start, e))

    # 1) Hledej soubory v aktuálním adresáři
    for p in normalized:
        base = posixpath.basename(p)
        if base.lower() == target_basename.lower():
            print(f"INFO: Found file at {p}")
            return p

    # 2) Recurse do podsložek
    for p in normalized:
        try:
            if is_dir(ftp, p):
                found = find_file(ftp, p, target_basename, depth + 1)
                if found:
                    return found
        except Exception as e:
            print(f"WARN: Skipping {p}: {e}")
            continue

    return None

def download_file(ftp, remote_path: str) -> bytes:
    buf = io.BytesIO()
    # Změníme CWD na adresář souboru a RETR jen basename (lepší kompatibilita)
    dirpath = posixpath.dirname(remote_path) or "/"
    name = posixpath.basename(remote_path)
    if dirpath not in ("", "/"):
        ftp.cwd(dirpath)
    ftp.retrbinary(f"RETR {name}", buf.write)
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
    # smaž předchozí stejnojmenné soubory
    existing = drive.ListFile({
        "q": f"'{folder_id}' in parents and name='{name}' and trashed=false"
    }).GetList()
    for f in existing:
        f.Delete()

    f = drive.CreateFile({"title": name, "parents": [{"id": folder_id}]})
    # pokus o UTF-8 – pokud by soubor byl Win-1250, můžeš přepnout decode
    f.SetContentString(content.decode("utf-8", errors="ignore"))
    f.Upload()
    f.InsertPermission({"type": "anyone", "role": "reader"})
    f.FetchMetadata(fields="id,webContentLink,webViewLink")
    print("FILE_ID=", f["id"])
    print("DOWNLOAD_LINK=", f["webContentLink"])
    print("VIEW_LINK=", f["webViewLink"])

def main():
    ftp = connect_ftp()
    print("INFO: Listing from root, searching for:", TARGET_BASENAME)
    found_path = find_file(ftp, "/", TARGET_BASENAME, depth=0)
    if not found_path:
        # vypiš root listing do logu pro diagnostiku
        try:
            root = listdir_safe(ftp, "/")
            print("DEBUG: Root entries:", root)
        except Exception as e:
            print("DEBUG: Root listing failed:", e)
        raise FileNotFoundError(f"Soubor {TARGET_BASENAME} nebyl na FTP nalezen (do hloubky {MAX_DEPTH}).")

    data = download_file(ftp, found_path)
    try:
        ftp.quit()
    except Exception:
        pass

    drive = gdrive_client()
    upload_replace_public(drive, data, TARGET_NAME, FOLDER_ID)

if __name__ == "__main__":
    main()


