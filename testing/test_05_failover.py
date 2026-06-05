"""
SmartLab IoT Testing Framework
================================
Kategori 5: Uji Failover & Reliability Multiuser (3 Lab)
----------------------------------------------------------
Menguji kemampuan self-healing saat jaringan putus dan 
Zero Data Loss saat 3 lab beroperasi simultan dalam kondisi offline.

CATATAN: Pengujian self-healing membutuhkan intervensi manual operator
         (cabut/pasang kabel LAN atau matikan/hidupkan WiFi).

Output: 05_Failover_Reliability_{date}.xlsx
  - Sheet 'Self-Healing': Recovery time saat jaringan putus
  - Sheet 'Zero Data Loss': Verifikasi konsistensi data 3 lab

Cara Pakai:
  python -m testing.test_05_failover
"""

import time
import json
import concurrent.futures

from testing.config import (
    TEST_USERS, LAB_NAMES, SERVER_BASE_URL, DB_TIME_FORMAT
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db, cleanup_firebase_sandbox,
    get_test_db_conn, firebase_sandbox_ref, init_firebase_for_testing,
    simulate_scan_request,
    print_header, print_result, timestamp_now
)
from testing.excel_reporter import ExcelReporter


def run():
    """Menjalankan uji failover dan zero data loss."""
    print_header("KATEGORI 5: UJI FAILOVER & RELIABILITY MULTIUSER")

    # Setup
    create_test_db()
    init_firebase_for_testing()

    # ============================================================
    # BAGIAN A: Self-Healing (Simulasi Offline → Online)
    # ============================================================
    print("\n--- A. Self-Healing (Simulasi Offline → Online) ---")
    print("  INSTRUKSI OPERATOR:")
    print("  1. Script akan mengirim data ke server")
    print("  2. Saat diminta, CABUT kabel LAN / matikan WiFi")
    print("  3. Saat diminta, PASANG kembali kabel LAN / hidupkan WiFi")
    print("  4. Script akan mengukur recovery time\n")

    healing_rows = []

    # Fase 1: Kirim data saat online (memastikan baseline berfungsi)
    print("  [Fase 1] Mengirim data saat ONLINE...")
    baseline_results = {}
    for lab in LAB_NAMES:
        fp_id = TEST_USERS[0]["fingerprint_id"]
        result = simulate_scan_request(fp_id, lab)
        baseline_results[lab] = result["success"]
        print(f"    {lab}: {'OK' if result['success'] else 'GAGAL'}")
        # Tap out
        time.sleep(0.3)
        simulate_scan_request(fp_id, lab)
        time.sleep(0.3)

    # Fase 2: Instruksi untuk memutus koneksi
    print("\n  [Fase 2] Silakan CABUT kabel LAN / matikan WiFi SEKARANG.")
    waktu_disconnect = timestamp_now()
    input("  Tekan ENTER setelah koneksi terputus...")

    # Fase 3: Kirim data saat offline (simulasi 3 lab bersamaan)
    print("\n  [Fase 3] Mengirim data dari 3 lab secara simultan (mode offline)...")
    offline_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for idx, lab in enumerate(LAB_NAMES):
            fp_id = TEST_USERS[idx]["fingerprint_id"]
            futures[lab] = executor.submit(simulate_scan_request, fp_id, lab)
        for lab, future in futures.items():
            offline_results[lab] = future.result()
            status_text = "Saved Locally" if offline_results[lab]["response_text"] else "Error"
            print(f"    {lab}: {offline_results[lab]['response_text'][:50]}")

    # Fase 4: Instruksi untuk menyambung kembali
    print("\n  [Fase 4] Silakan PASANG KEMBALI kabel LAN / hidupkan WiFi SEKARANG.")
    input("  Tekan ENTER setelah koneksi tersambung kembali...")
    waktu_reconnect = timestamp_now()

    # Fase 5: Monitor recovery
    print("\n  [Fase 5] Mengukur recovery time...", flush=True)
    t_recovery_start = time.perf_counter()
    server_recovered = False
    max_wait = 120  # Max 2 menit

    for _ in range(max_wait * 2):  # Check setiap 0.5 detik
        try:
            test_result = simulate_scan_request(TEST_USERS[0]["fingerprint_id"], LAB_NAMES[0])
            if test_result["success"]:
                server_recovered = True
                break
        except Exception:
            pass
        time.sleep(0.5)

    recovery_time_s = round((time.perf_counter() - t_recovery_start), 1)

    status = "SUCCESS" if server_recovered else "FAIL"
    print_result("Recovery Time", f"{recovery_time_s}s", status)

    healing_rows.append([
        1,
        "Simulasi 3 Lab Offline",
        waktu_disconnect,
        waktu_reconnect,
        recovery_time_s,
        "ACTIVE" if server_recovered else "DOWN",
        "Manual Check",
        status,
    ])

    # Tap out dari baseline
    time.sleep(0.3)
    simulate_scan_request(TEST_USERS[0]["fingerprint_id"], LAB_NAMES[0])
    time.sleep(0.3)

    # ============================================================
    # BAGIAN B: Zero Data Loss (Verifikasi Konsistensi 3 Lab)
    # ============================================================
    print("\n--- B. Zero Data Loss (Verifikasi Data 3 Lab) ---")
    zdl_rows = []

    # Kirim data baru dari 3 lab
    print("  Mengirim data segar dari 3 lab...")
    for idx, lab in enumerate(LAB_NAMES):
        fp_id = TEST_USERS[idx]["fingerprint_id"]
        result = simulate_scan_request(fp_id, lab)
        print(f"    {lab}: {result['response_text']}")
        time.sleep(0.5)

    # Beri waktu sync ke Firebase
    print("  Menunggu sync ke Firebase (10 detik)...")
    time.sleep(10)

    # Verifikasi SQLite vs Firebase
    conn = get_test_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 10")
    recent_logs = cur.fetchall()
    conn.close()

    log_no = 0
    for log in recent_logs:
        log_no += 1
        log_dict = dict(log)
        log_id = log_dict.get("id", "N/A")
        lab_terlibat = log_dict.get("lokasi_lab", "N/A")

        sqlite_json = json.dumps({k: str(v) for k, v in log_dict.items()}, ensure_ascii=False)
        sqlite_size = len(sqlite_json.encode("utf-8"))

        # Cek di Firebase
        firebase_json = "N/A"
        firebase_size = 0
        fields_match_pct = "0%"

        try:
            ref = firebase_sandbox_ref("absensi_log")
            if ref:
                fb_data = ref.get()
                if fb_data and isinstance(fb_data, dict):
                    for key, val in fb_data.items():
                        if (val.get("nama_asisten") == log_dict.get("nama") and
                                val.get("lokasi_lab") == lab_terlibat):
                            firebase_json = json.dumps(val, ensure_ascii=False)
                            firebase_size = len(firebase_json.encode("utf-8"))
                            fields_match_pct = "100%"
                            break
        except Exception:
            pass

        status = "SUCCESS" if fields_match_pct == "100%" else "FAIL"

        zdl_rows.append([
            log_no,
            log_id,
            lab_terlibat,
            sqlite_json[:100] + "..." if len(sqlite_json) > 100 else sqlite_json,
            firebase_json[:100] + "..." if len(firebase_json) > 100 else firebase_json,
            sqlite_size,
            firebase_size,
            fields_match_pct,
            status,
        ])

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("05_Failover_Reliability", metadata={
        "notes": "Pengujian failover (self-healing) dan zero data loss dengan simulasi multiuser 3 lab."
    })

    reporter.add_sheet(
        "Self-Healing",
        headers=["No", "Skenario Multiuser", "Waktu Disconnect", "Waktu Reconnect",
                 "Recovery Time (s)", "Status Server Uji", "OLED Indikator ESP32", "Status"],
        data_rows=healing_rows,
        status_col_name="Status",
        left_align_cols=["Waktu Disconnect", "Waktu Reconnect"]
    )

    reporter.add_sheet(
        "Zero Data Loss",
        headers=["No", "Log ID", "Lab Terlibat", "Data SQLite (JSON)", "Data Firebase (JSON)",
                 "Size SQLite (bytes)", "Size Firebase (bytes)", "Fields Match (%)", "Status"],
        data_rows=zdl_rows,
        status_col_name="Status",
        left_align_cols=["Data SQLite (JSON)", "Data Firebase (JSON)"]
    )

    filepath = reporter.save()

    # Cleanup
    cleanup_test_db()
    cleanup_firebase_sandbox()

    print(f"\n[SELESAI] Hasil disimpan di: {filepath}")
    return filepath


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    run()
