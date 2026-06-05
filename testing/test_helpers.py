"""
SmartLab IoT Testing Framework — Utility & Helper Functions
============================================================
Fungsi-fungsi umum yang dipakai oleh semua modul test:
- Isolasi database SQLite (create/cleanup test DB)
- Isolasi Firebase (sandbox node)
- Pengukuran waktu (timing decorator)
- Ping host (ICMP)
- HTTP request simulator ke /api/scan
- Checksum produksi DB
"""

import os
import sys
import sqlite3
import hashlib
import time
import datetime
import subprocess
import platform
import json
import requests
import re

# Tambahkan project root ke path agar bisa import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from testing.config import (
    TEST_DB_PATH, PROD_DB_PATH, TEST_DB_NAME, PROD_DB_NAME,
    FIREBASE_CREDENTIALS_PATH, FIREBASE_DB_URL, FIREBASE_SANDBOX_PREFIX,
    SERVER_BASE_URL, DB_TIME_FORMAT, TEST_USERS, TEST_SCHEDULES,
    PING_COUNT, PING_TIMEOUT_SEC, PROJECT_ROOT, RESULTS_DIR
)


# ================================================================
# DATABASE ISOLATION (SQLite)
# ================================================================

def create_test_db():
    """
    Membuat test database dengan menyalin SKEMA dari database produksi.
    Data produksi TIDAK disalin — hanya struktur tabel.
    Test users & schedules dummy dimasukkan untuk kebutuhan pengujian.
    """
    # Hapus file test lama jika ada
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()

    # Buat tabel dengan skema identik ke server_main.py init_db()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
        fingerprint_id INTEGER PRIMARY KEY,
        nama TEXT,
        id_asisten_kampus TEXT,
        hak_akses TEXT,
        is_synced INTEGER DEFAULT 1,
        sync_action TEXT DEFAULT NULL
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firebase_key TEXT,
        fingerprint_id INTEGER, nama TEXT, id_asisten_kampus TEXT,
        waktu_masuk DATETIME, waktu_keluar DATETIME,
        status TEXT, lokasi_lab TEXT, kelas TEXT,
        is_synced INTEGER DEFAULT 0
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS active_sessions (
        fingerprint_id INTEGER PRIMARY KEY,
        nama TEXT, id_asisten_kampus TEXT,
        waktu_masuk DATETIME,
        jam_selesai_kelas DATETIME,
        lokasi_lab TEXT, kelas TEXT,
        log_db_id INTEGER
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS schedules (
        id_jadwal TEXT PRIMARY KEY,
        nama_kelas TEXT,
        hari TEXT,
        jam_mulai TEXT,
        jam_selesai TEXT,
        lokasi_lab TEXT,
        is_online INTEGER,
        is_synced INTEGER DEFAULT 1,
        sync_action TEXT DEFAULT NULL
    )''')

    # Insert test users
    for user in TEST_USERS:
        cursor.execute(
            "INSERT OR REPLACE INTO users (fingerprint_id, nama, id_asisten_kampus, hak_akses) VALUES (?, ?, ?, ?)",
            (user["fingerprint_id"], user["nama"], user["id_asisten_kampus"], user["hak_akses"])
        )

    # Insert test schedules
    for sched in TEST_SCHEDULES:
        cursor.execute(
            "INSERT OR REPLACE INTO schedules (id_jadwal, nama_kelas, hari, jam_mulai, jam_selesai, lokasi_lab, is_online) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sched["id_jadwal"], sched["nama_kelas"], sched["hari"],
             sched["jam_mulai"], sched["jam_selesai"], sched["lokasi_lab"], sched["is_online"])
        )

    conn.commit()
    conn.close()
    print(f"[TEST-DB] Database uji '{TEST_DB_NAME}' berhasil dibuat dengan {len(TEST_USERS)} user dummy & {len(TEST_SCHEDULES)} jadwal dummy.")
    return TEST_DB_PATH


def cleanup_test_db():
    """Menghapus file test database setelah pengujian selesai."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
        print(f"[TEST-DB] Database uji '{TEST_DB_NAME}' berhasil dihapus.")
    else:
        print(f"[TEST-DB] Database uji '{TEST_DB_NAME}' tidak ditemukan (sudah bersih).")


def get_test_db_conn():
    """Return SQLite connection ke test database."""
    if not os.path.exists(TEST_DB_PATH):
        create_test_db()
    conn = sqlite3.connect(TEST_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def get_prod_db_checksum():
    """
    Menghitung SHA-256 checksum dari database produksi.
    Digunakan untuk memverifikasi bahwa produksi TIDAK berubah setelah pengujian.
    """
    if not os.path.exists(PROD_DB_PATH):
        return None
    sha256 = hashlib.sha256()
    with open(PROD_DB_PATH, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# ================================================================
# FIREBASE ISOLATION (Sandbox)
# ================================================================

_firebase_app_initialized = False

def init_firebase_for_testing():
    """
    Inisialisasi Firebase Admin SDK untuk testing.
    Menggunakan app name terpisah agar tidak bentrok dengan server produksi.
    """
    global _firebase_app_initialized
    if _firebase_app_initialized:
        return

    try:
        import firebase_admin
        from firebase_admin import credentials

        # Cek apakah sudah ada app default
        try:
            firebase_admin.get_app('testing')
            _firebase_app_initialized = True
            return
        except ValueError:
            pass

        if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
            print(f"[FIREBASE] WARNING: File credentials tidak ditemukan: {FIREBASE_CREDENTIALS_PATH}")
            print("[FIREBASE] Pengujian Firebase akan dilewati.")
            return

        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DB_URL}, name='testing')
        _firebase_app_initialized = True
        print("[FIREBASE] Firebase testing app berhasil diinisialisasi.")
    except Exception as e:
        print(f"[FIREBASE ERROR] Gagal inisialisasi: {e}")


def firebase_sandbox_ref(path=""):
    """
    Return Firebase database reference ke sandbox node.
    Contoh: firebase_sandbox_ref("absensi_log") → ref ke /test_sandbox/absensi_log
    """
    import firebase_admin
    from firebase_admin import db as fb_db

    init_firebase_for_testing()

    full_path = f"{FIREBASE_SANDBOX_PREFIX}/{path}" if path else FIREBASE_SANDBOX_PREFIX
    try:
        app = firebase_admin.get_app('testing')
        return fb_db.reference(full_path, app=app)
    except Exception as e:
        print(f"[FIREBASE ERROR] Gagal mendapatkan reference: {e}")
        return None


def cleanup_firebase_sandbox():
    """Menghapus seluruh node /test_sandbox/ dari Firebase."""
    try:
        ref = firebase_sandbox_ref()
        if ref:
            ref.delete()
            print(f"[FIREBASE] Node /{FIREBASE_SANDBOX_PREFIX}/ berhasil dihapus.")
    except Exception as e:
        print(f"[FIREBASE ERROR] Gagal cleanup sandbox: {e}")


# ================================================================
# TIMING & MEASUREMENT
# ================================================================

def measure_time(func):
    """
    Decorator untuk mengukur execution time suatu fungsi.
    Mengembalikan tuple (result, elapsed_ms).
    """
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - start) * 1000
        return result, elapsed_ms
    return wrapper


def timestamp_now():
    """Return timestamp ISO dengan microsecond precision."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def timestamp_now_ms():
    """Return current time sebagai epoch milliseconds (int)."""
    return int(time.time() * 1000)


# ================================================================
# NETWORK UTILITIES (Ping)
# ================================================================

def ping_host(ip, count=None, timeout=None):
    """
    Ping host dan return dictionary metrik.
    Bekerja di Windows.
    
    Returns:
        dict: {
            "ip": str,
            "avg_ms": float, "min_ms": float, "max_ms": float,
            "packet_loss_pct": float,
            "jitter_ms": float,
            "raw_times": list[float],
            "reachable": bool
        }
    """
    if count is None:
        count = PING_COUNT
    if timeout is None:
        timeout = PING_TIMEOUT_SEC

    result = {
        "ip": ip,
        "avg_ms": 0.0, "min_ms": 0.0, "max_ms": 0.0,
        "packet_loss_pct": 100.0,
        "jitter_ms": 0.0,
        "raw_times": [],
        "reachable": False
    }

    try:
        # Windows: ping -n <count> -w <timeout_ms>
        cmd = ["ping", "-n", str(count), "-w", str(timeout * 1000), ip]
        output = subprocess.run(cmd, capture_output=True, text=True, timeout=count * timeout + 10)
        stdout = output.stdout

        # Parse packet loss
        loss_match = re.search(r'\((\d+)% loss\)', stdout)
        if loss_match:
            result["packet_loss_pct"] = float(loss_match.group(1))

        # Parse individual round-trip times (Windows format: "Reply from ... time=Xms")
        time_matches = re.findall(r'time[=<](\d+)ms', stdout)
        if time_matches:
            raw_times = [float(t) for t in time_matches]
            result["raw_times"] = raw_times
            result["min_ms"] = min(raw_times)
            result["max_ms"] = max(raw_times)
            result["avg_ms"] = sum(raw_times) / len(raw_times)
            result["reachable"] = True

            # Jitter = standar deviasi dari raw_times
            if len(raw_times) > 1:
                mean = result["avg_ms"]
                variance = sum((t - mean) ** 2 for t in raw_times) / len(raw_times)
                result["jitter_ms"] = round(variance ** 0.5, 2)

        # Juga coba parse summary line Windows: "Minimum = Xms, Maximum = Xms, Average = Xms"
        summary_match = re.search(
            r'Minimum\s*=\s*(\d+)ms.*Maximum\s*=\s*(\d+)ms.*Average\s*=\s*(\d+)ms',
            stdout
        )
        if summary_match and not time_matches:
            result["min_ms"] = float(summary_match.group(1))
            result["max_ms"] = float(summary_match.group(2))
            result["avg_ms"] = float(summary_match.group(3))
            result["reachable"] = True

    except subprocess.TimeoutExpired:
        print(f"[PING] Timeout saat ping ke {ip}")
    except Exception as e:
        print(f"[PING ERROR] {ip}: {e}")

    return result


# ================================================================
# HTTP REQUEST SIMULATOR
# ================================================================

def simulate_scan_request(fp_id, lab, server_url=None):
    """
    Simulasi HTTP GET ke /api/scan (endpoint yang dipanggil ESP32).
    
    Args:
        fp_id: Fingerprint ID
        lab: Nama lab (e.g., "LAB_AP")
        server_url: URL server (default dari config)
    
    Returns:
        dict: {
            "fp_id": int,
            "lab": str,
            "status_code": int,
            "response_text": str,
            "latency_ms": float,
            "timestamp_request": str,
            "timestamp_response": str,
            "success": bool
        }
    """
    if server_url is None:
        server_url = SERVER_BASE_URL

    url = f"{server_url}/api/scan?id={fp_id}&lab={lab}"

    result = {
        "fp_id": fp_id,
        "lab": lab,
        "status_code": 0,
        "response_text": "",
        "latency_ms": 0.0,
        "timestamp_request": "",
        "timestamp_response": "",
        "success": False
    }

    try:
        result["timestamp_request"] = timestamp_now()
        start = time.perf_counter()
        resp = requests.get(url, timeout=10)
        elapsed = (time.perf_counter() - start) * 1000

        result["timestamp_response"] = timestamp_now()
        result["status_code"] = resp.status_code
        result["response_text"] = resp.text.strip()
        result["latency_ms"] = round(elapsed, 3)
        result["success"] = resp.status_code == 200
    except requests.exceptions.ConnectionError:
        result["response_text"] = "CONNECTION_ERROR"
        result["timestamp_response"] = timestamp_now()
    except requests.exceptions.Timeout:
        result["response_text"] = "TIMEOUT"
        result["timestamp_response"] = timestamp_now()
    except Exception as e:
        result["response_text"] = f"ERROR: {str(e)}"
        result["timestamp_response"] = timestamp_now()

    return result


# ================================================================
# PAYROLL CALCULATION (Sesuai Apps Script & server_main.py)
# ================================================================

def hitung_durasi_menit(waktu_masuk_str, waktu_keluar_str):
    """
    Menghitung durasi kerja dalam MENIT (genap ke bawah).
    Logika identik dengan server_main.py hitung_durasi_menit().
    """
    if not waktu_masuk_str or not waktu_keluar_str:
        return 0
    try:
        masuk = datetime.datetime.strptime(waktu_masuk_str, DB_TIME_FORMAT)
        keluar = datetime.datetime.strptime(waktu_keluar_str, DB_TIME_FORMAT)
        if keluar < masuk:
            return 0
        durasi_total = keluar - masuk
        total_menit_genap = durasi_total.total_seconds() // 60
        return int(total_menit_genap)
    except Exception:
        return 0


def hitung_gaji(total_menit, tarif_per_menit=281.25):
    """
    Menghitung gaji berdasarkan total menit kerja.
    Logika identik dengan rumus Google Sheets:
    (HOUR(I)*60 + MINUTE(I)) * 281.25
    """
    return total_menit * tarif_per_menit


# ================================================================
# GENERAL UTILITIES
# ================================================================

def ensure_results_dir():
    """Pastikan folder results/ ada."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return RESULTS_DIR


def print_header(title):
    """Print header cantik untuk output console."""
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_result(label, value, status=""):
    """Print hasil pengujian individual."""
    status_icon = ""
    if status.upper() == "PASS":
        status_icon = "[PASS]"
    elif status.upper() == "FAIL":
        status_icon = "[FAIL]"
    elif status.upper() == "WARNING":
        status_icon = "[WARN]"
    print(f"  {status_icon} {label}: {value}")
