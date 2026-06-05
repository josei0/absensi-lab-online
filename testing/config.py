"""
SmartLab IoT Testing Framework — Konfigurasi Terpusat
=====================================================
Semua konstanta, path, dan pengaturan pengujian didefinisikan di sini.
PENTING: Tidak ada operasi WRITE ke database/Firebase produksi.
"""

import os

# --- Path Proyek ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTING_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(TESTING_DIR, "results")

# --- Database Isolation ---
TEST_DB_NAME = "test_database_lab.sqlite"         # File SQLite khusus pengujian
TEST_DB_PATH = os.path.join(PROJECT_ROOT, TEST_DB_NAME)
PROD_DB_NAME = "database_lab.sqlite"               # File SQLite produksi (READ-ONLY reference)
PROD_DB_PATH = os.path.join(PROJECT_ROOT, PROD_DB_NAME)

# --- Firebase Isolation ---
FIREBASE_CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "firebase_credentials.json")
FIREBASE_DB_URL = "https://absensi-lab-ap-default-rtdb.asia-southeast1.firebasedatabase.app/"
FIREBASE_SANDBOX_PREFIX = "test_sandbox"
# Semua write akan masuk ke:
#   /test_sandbox/absensi_log/
#   /test_sandbox/asisten_master/
#   /test_sandbox/device_control/

# --- Network Static IPs (Dari Router TP-Link TL-WR840N) ---
SERVER_IP = "192.168.137.1"
SERVER_PORT = 5001  # Port berbeda dari produksi (5000) agar tidak bentrok
SERVER_BASE_URL = f"http://{SERVER_IP}:{SERVER_PORT}"

ESP32_DEVICES = {
    "LAB_AP":     {"ip": "192.168.137.101", "mac": "DE:AD:BE:EF:FE:01"},
    "LAB_TEKDIG": {"ip": "192.168.137.102", "mac": "DE:AD:BE:EF:FE:02"},
    "LAB_MIKRO":  {"ip": "192.168.137.103", "mac": "DE:AD:BE:EF:FE:03"},
}

# Daftar nama lab (urutan konsisten untuk laporan)
LAB_NAMES = ["LAB_AP", "LAB_TEKDIG", "LAB_MIKRO"]

# --- Test Users (Dummy, fingerprint_id tinggi agar tidak bentrok dengan produksi) ---
TEST_USERS = [
    {
        "fingerprint_id": 901,
        "nama": "Test User A",
        "id_asisten_kampus": "TEST001",
        "hak_akses": "LAB_AP,LAB_TEKDIG,LAB_MIKRO",  # Akses ke semua lab
    },
    {
        "fingerprint_id": 902,
        "nama": "Test User B",
        "id_asisten_kampus": "TEST002",
        "hak_akses": "LAB_AP,LAB_TEKDIG",  # Akses terbatas
    },
    {
        "fingerprint_id": 903,
        "nama": "Test User C",
        "id_asisten_kampus": "TEST003",
        "hak_akses": "LAB_MIKRO",  # Akses hanya LAB_MIKRO
    },
]

# --- Test Schedules (Jadwal dummy untuk pengujian) ---
TEST_SCHEDULES = [
    {
        "id_jadwal": "TEST_J001",
        "nama_kelas": "Test Kelas Pagi",
        "hari": "Senin",
        "jam_mulai": "08:00",
        "jam_selesai": "10:00",
        "lokasi_lab": "LAB_AP",
        "is_online": 0,
    },
    {
        "id_jadwal": "TEST_J002",
        "nama_kelas": "Test Kelas Siang",
        "hari": "Senin",
        "jam_mulai": "10:00",
        "jam_selesai": "12:00",
        "lokasi_lab": "LAB_TEKDIG",
        "is_online": 0,
    },
    {
        "id_jadwal": "TEST_J003",
        "nama_kelas": "Test Kelas Sore",
        "hari": "Senin",
        "jam_mulai": "13:00",
        "jam_selesai": "15:00",
        "lokasi_lab": "LAB_MIKRO",
        "is_online": 0,
    },
]

# --- Payroll (Kategori 8) ---
TARIF_PER_MENIT = 281.25           # Rp per menit (sesuai Apps Script)
ANGGARAN_MAKS = 1080000            # Rp batas maks per asisten per bulan
ESTIMASI_MANUAL_PER_BARIS_DETIK = 20.0  # Estimasi waktu admin rekap manual per baris log

# --- Database Time Format (konsisten dengan server_main.py) ---
DB_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# --- Ping Configuration ---
PING_COUNT = 10        # Jumlah paket ping per device
PING_TIMEOUT_SEC = 2   # Timeout per paket ping (detik)

# --- Excel Styling ---
EXCEL_HEADER_COLOR = "CC0000"       # Warna header merah (RGB hex)
EXCEL_HEADER_FONT_COLOR = "FFFFFF"  # Warna font header putih
EXCEL_PASS_COLOR = "C6EFCE"        # Warna hijau muda untuk PASS
EXCEL_WARNING_COLOR = "FFEB9C"     # Warna kuning muda untuk WARNING
EXCEL_FAIL_COLOR = "FFC7CE"        # Warna merah muda untuk FAIL
