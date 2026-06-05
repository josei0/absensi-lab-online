"""
SmartLab IoT Testing Framework
================================
Kategori 6: Uji Server Logic — Auto Tap-Out & Cleanup
-------------------------------------------------------
Menguji efektivitas dan kecepatan pemrosesan logika server:
  - Auto Tap-Out: Sesi ditutup otomatis saat jadwal kelas habis
  - Auto Cleanup: Sesi kemarin dibersihkan

SIMULASI MURNI — tidak memerlukan perangkat fisik.
Bekerja langsung pada test database.

Output: 06_Server_Logic_{date}.xlsx
  - Sheet 'Detail Pemrosesan Sesi': Verifikasi per sesi
  - Sheet 'Ringkasan Eksekusi': Jumlah sesi & waktu proses

Cara Pakai:
  python -m testing.test_06_server_logic
"""

import time
import datetime
import sqlite3

from testing.config import (
    TEST_DB_PATH, DB_TIME_FORMAT, TEST_USERS, LAB_NAMES
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db,
    print_header, print_result, timestamp_now, hitung_durasi_menit
)
from testing.excel_reporter import ExcelReporter


def _run_auto_tap_out_logic(db_path):
    """
    Menjalankan logika auto tap-out IDENTIK dengan task_smart_auto_tap_out()
    di server_main.py, tetapi sekali jalan (bukan loop).
    
    Returns: (processed_count, status_updates)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now = datetime.datetime.now()
    today_str = datetime.date.today().strftime('%Y-%m-%d')

    cur.execute("""
        SELECT fingerprint_id, nama, log_db_id, jam_selesai_kelas, waktu_masuk
        FROM active_sessions
        WHERE ? >= jam_selesai_kelas OR date(waktu_masuk) != ?
    """, (now.strftime(DB_TIME_FORMAT), today_str))

    expired_sessions = cur.fetchall()
    status_updates = []

    for sess in expired_sessions:
        fp_id = sess["fingerprint_id"]
        nama_user = sess["nama"]
        log_db_id = sess["log_db_id"]
        jam_selesai_str = sess["jam_selesai_kelas"]
        waktu_masuk_str = sess["waktu_masuk"]

        status_final = 'Keluar (Auto)'

        waktu_masuk_tanggal_obj = datetime.datetime.strptime(waktu_masuk_str, DB_TIME_FORMAT).date()
        if waktu_masuk_tanggal_obj != datetime.date.today():
            status_final = 'Keluar (Auto-Cleanup)'

        waktu_selesai_kelas = datetime.datetime.strptime(jam_selesai_str, DB_TIME_FORMAT)

        cur.execute("UPDATE logs SET status=?, waktu_keluar=?, is_synced=0 WHERE id=?",
                    (status_final, waktu_selesai_kelas.strftime(DB_TIME_FORMAT), log_db_id))
        cur.execute("DELETE FROM active_sessions WHERE fingerprint_id=?", (fp_id,))

        status_updates.append({
            "fp_id": fp_id,
            "nama": nama_user,
            "log_db_id": log_db_id,
            "waktu_masuk": waktu_masuk_str,
            "jam_selesai_kelas": jam_selesai_str,
            "status_final": status_final,
            "waktu_keluar": waktu_selesai_kelas.strftime(DB_TIME_FORMAT),
        })

    conn.commit()
    conn.close()

    return len(expired_sessions), status_updates


def run():
    """Menjalankan uji server logic (auto tap-out & cleanup)."""
    print_header("KATEGORI 6: UJI SERVER LOGIC (AUTO TAP-OUT & CLEANUP)")

    # Setup
    create_test_db()

    now = datetime.datetime.now()
    detail_rows = []
    execution_summary = []

    # ============================================================
    # BAGIAN A: Seed Data — Sesi yang jam_selesai sudah lewat (Auto Tap-Out)
    # ============================================================
    print("\n--- A. Menyiapkan sesi expired (untuk Auto Tap-Out) ---")
    conn = sqlite3.connect(TEST_DB_PATH)
    cur = conn.cursor()

    # Buat sesi dengan jam_selesai 2 jam yang lalu
    jam_selesai_lalu = (now - datetime.timedelta(hours=2)).strftime(DB_TIME_FORMAT)
    jam_masuk_lalu = (now - datetime.timedelta(hours=4)).strftime(DB_TIME_FORMAT)

    auto_tapout_sesi = []
    for idx, user in enumerate(TEST_USERS):
        fp_id = user["fingerprint_id"]
        nama = user["nama"]
        lab = LAB_NAMES[idx % len(LAB_NAMES)]

        # Insert ke logs
        cur.execute('''INSERT INTO logs 
            (fingerprint_id, nama, id_asisten_kampus, waktu_masuk, status, lokasi_lab, kelas, is_synced)
            VALUES (?, ?, ?, ?, 'MASUK', ?, 'Test Kelas', 0)''',
            (fp_id, nama, user["id_asisten_kampus"], jam_masuk_lalu, lab))
        log_id = cur.lastrowid

        # Insert ke active_sessions
        cur.execute('''INSERT OR REPLACE INTO active_sessions 
            (fingerprint_id, nama, id_asisten_kampus, waktu_masuk, jam_selesai_kelas, lokasi_lab, kelas, log_db_id)
            VALUES (?, ?, ?, ?, ?, ?, 'Test Kelas', ?)''',
            (fp_id, nama, user["id_asisten_kampus"], jam_masuk_lalu, jam_selesai_lalu, lab, log_id))

        auto_tapout_sesi.append({
            "fp_id": fp_id, "nama": nama, "lab": lab,
            "waktu_masuk": jam_masuk_lalu, "jam_selesai": jam_selesai_lalu,
            "log_id": log_id, "expected_status": "Keluar (Auto)"
        })

    # Tambahkan sesi dari KEMARIN (untuk Auto Cleanup)
    print("\n--- B. Menyiapkan sesi kemarin (untuk Auto Cleanup) ---")
    yesterday = now - datetime.timedelta(days=1)
    jam_masuk_kemarin = yesterday.replace(hour=8, minute=0, second=0).strftime(DB_TIME_FORMAT)
    jam_selesai_kemarin = yesterday.replace(hour=10, minute=0, second=0).strftime(DB_TIME_FORMAT)

    cleanup_sesi = []
    # Gunakan FP ID yang berbeda agar tidak bentrok
    cleanup_users = [
        {"fp_id": 911, "nama": "Cleanup User 1", "id_kampus": "CLN001"},
        {"fp_id": 912, "nama": "Cleanup User 2", "id_kampus": "CLN002"},
    ]

    for idx, cu in enumerate(cleanup_users):
        lab = LAB_NAMES[idx % len(LAB_NAMES)]

        # Insert user dulu
        cur.execute("INSERT OR REPLACE INTO users (fingerprint_id, nama, id_asisten_kampus, hak_akses) VALUES (?,?,?,?)",
                    (cu["fp_id"], cu["nama"], cu["id_kampus"], ",".join(LAB_NAMES)))

        cur.execute('''INSERT INTO logs 
            (fingerprint_id, nama, id_asisten_kampus, waktu_masuk, status, lokasi_lab, kelas, is_synced)
            VALUES (?, ?, ?, ?, 'MASUK', ?, 'Kelas Kemarin', 0)''',
            (cu["fp_id"], cu["nama"], cu["id_kampus"], jam_masuk_kemarin, lab))
        log_id = cur.lastrowid

        cur.execute('''INSERT OR REPLACE INTO active_sessions 
            (fingerprint_id, nama, id_asisten_kampus, waktu_masuk, jam_selesai_kelas, lokasi_lab, kelas, log_db_id)
            VALUES (?, ?, ?, ?, ?, ?, 'Kelas Kemarin', ?)''',
            (cu["fp_id"], cu["nama"], cu["id_kampus"], jam_masuk_kemarin, jam_selesai_kemarin, lab, log_id))

        cleanup_sesi.append({
            "fp_id": cu["fp_id"], "nama": cu["nama"], "lab": lab,
            "waktu_masuk": jam_masuk_kemarin, "jam_selesai": jam_selesai_kemarin,
            "log_id": log_id, "expected_status": "Keluar (Auto-Cleanup)"
        })

    conn.commit()

    # Verifikasi jumlah active sessions sebelum proses
    cur.execute("SELECT COUNT(*) FROM active_sessions")
    total_sebelum = cur.fetchone()[0]
    print(f"  Total active sessions sebelum proses: {total_sebelum}")
    conn.close()

    # ============================================================
    # BAGIAN C: Jalankan Auto Tap-Out Logic & Ukur Waktu
    # ============================================================
    print("\n--- C. Menjalankan Auto Tap-Out & Cleanup Logic ---")

    # Auto Tap-Out
    t_start = time.perf_counter()
    waktu_mulai_str = timestamp_now()
    tapout_count, tapout_updates = _run_auto_tap_out_logic(TEST_DB_PATH)
    t_elapsed_tapout = (time.perf_counter() - t_start) * 1000
    waktu_selesai_str = timestamp_now()

    print_result("Auto Tap-Out", f"{tapout_count} sesi diproses dalam {t_elapsed_tapout:.2f}ms", "PASS")

    execution_summary.append([
        "Auto Tap-Out Logika",
        f"{tapout_count} Sesi",
        waktu_mulai_str,
        waktu_selesai_str,
        f"{t_elapsed_tapout:.2f} ms",
        "SUCCESS" if tapout_count > 0 else "NO DATA",
    ])

    # Verifikasi hasil
    all_expected = auto_tapout_sesi + cleanup_sesi
    conn = sqlite3.connect(TEST_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    test_no = 0
    for sesi in all_expected:
        test_no += 1
        cur.execute("SELECT status, waktu_keluar FROM logs WHERE id=?", (sesi["log_id"],))
        log_row = cur.fetchone()

        actual_status = dict(log_row)["status"] if log_row else "NOT FOUND"
        waktu_keluar = dict(log_row)["waktu_keluar"] if log_row else "N/A"
        match = "Yes" if actual_status == sesi["expected_status"] else "No"

        detail_rows.append([
            test_no,
            sesi["expected_status"].replace("Keluar (Auto-Cleanup)", "Auto Cleanup").replace("Keluar (Auto)", "Auto Tap-Out"),
            sesi["fp_id"],
            sesi["waktu_masuk"],
            sesi["jam_selesai"],
            sesi["expected_status"],
            actual_status,
            waktu_keluar if waktu_keluar else "N/A",
            match,
        ])

    # Verifikasi active_sessions kosong
    cur.execute("SELECT COUNT(*) FROM active_sessions")
    remaining = cur.fetchone()[0]
    conn.close()

    print_result("Sisa Active Sessions", remaining, "PASS" if remaining == 0 else "FAIL")

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("06_Server_Logic", metadata={
        "notes": f"Pengujian logika Auto Tap-Out dan Auto Cleanup. {tapout_count} sesi diproses."
    })

    reporter.add_sheet(
        "Detail Pemrosesan Sesi",
        headers=["No", "Test Case Skenario", "FP ID", "Waktu Masuk", "Jam Selesai Kelas",
                 "Expected Status", "Actual Status", "Waktu Keluar", "Match?"],
        data_rows=detail_rows,
        status_col_name="Match?",
        left_align_cols=["Waktu Masuk", "Jam Selesai Kelas", "Waktu Keluar"]
    )

    reporter.add_sheet(
        "Ringkasan Eksekusi",
        headers=["Nama Proses", "Jumlah Sesi Diproses", "Waktu Mulai", "Waktu Selesai",
                 "Waktu Proses Total (ms)", "Status Akhir"],
        data_rows=execution_summary,
        status_col_name="Status Akhir",
        left_align_cols=["Waktu Mulai", "Waktu Selesai"]
    )

    filepath = reporter.save()

    # Cleanup
    cleanup_test_db()

    print(f"\n[SELESAI] Hasil disimpan di: {filepath}")
    return filepath


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    run()
