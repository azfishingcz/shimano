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

    # 1) Hledej soubory v aktuálním ad

