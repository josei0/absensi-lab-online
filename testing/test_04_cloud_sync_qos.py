"""
SmartLab IoT Testing Framework
================================
Kategori 4: Uji Cloud Sync & Jaringan QoS
-------------------------------------------
Mengukur kecepatan replikasi data SQLite → Firebase RTDB
serta metrik jaringan (ping, jitter, packet loss) ke Firebase server.

Output: 04_Cloud_Sync_QoS_{date}.xlsx
  - Sheet 'Sync Latency & QoS': Waktu sync dan metrik jaringan

Cara Pakai:
  python -m testing.test_04_cloud_sync_qos
"""

import time
import json
import sqlite3

from testing.config import (
    TEST_DB_PATH, DB_TIME_FORMAT, FIREBASE_DB_URL,
    LAB_NAMES, TEST_USERS, SERVER_BASE_URL
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db, cleanup_firebase_sandbox,
    get_test_db_conn, firebase_sandbox_ref, init_firebase_for_testing,
    ping_host, simulate_scan_request,
    print_header, print_result, timestamp_now, timestamp_now_ms
)
from testing.excel_reporter import ExcelReporter


def _measure_firebase_sync(lab_name, fp_id, max_wait_sec=15):
    """
    Mengukur waktu dari write SQLite (via /api/scan) sampai data muncul di Firebase sandbox.
    
    Returns: dict dengan metrik sync.
    """
    # 1. Catat waktu awal & kirim scan request
    t_start = time.perf_counter()
    result = simulate_scan_request(fp_id, lab_name)
    t_sqlite_write = (time.perf_counter() - t_start) * 1000  # ms

    # 2. Poll Firebase sandbox sampai data muncul
    t_sync_start = time.perf_counter()
    firebase_synced = False
    sync_latency = 0

    ref = firebase_sandbox_ref("absensi_log")
    if ref:
        for _ in range(max_wait_sec * 4):  # Polling setiap 250ms
            time.sleep(0.25)
            try:
                data = ref.get()
                if data and isinstance(data, dict):
                    for key, val in data.items():
                        if val.get("lokasi_lab") == lab_name:
                            firebase_synced = True
                            break
            except Exception:
                pass
            if firebase_synced:
                break

        sync_latency = (time.perf_counter() - t_sync_start) * 1000  # ms

    total_e2e = t_sqlite_write + sync_latency

    return {
        "sqlite_write_ms": round(t_sqlite_write, 2),
        "sync_to_firebase_ms": round(sync_latency, 2),
        "total_e2e_ms": round(total_e2e, 2),
        "firebase_synced": firebase_synced,
        "response_text": result["response_text"],
        "data_size_bytes": len(result["response_text"].encode("utf-8")),
    }


def run():
    """Menjalankan uji cloud sync dan QoS."""
    print_header("KATEGORI 4: UJI CLOUD SYNC & JARINGAN QoS")

    # Setup
    create_test_db()
    init_firebase_for_testing()

    # ============================================================
    # BAGIAN A: Ping ke Firebase Server
    # ============================================================
    print("\n--- A. Ping ke Firebase Server ---")

    # Extract hostname dari Firebase URL
    import urllib.parse
    parsed = urllib.parse.urlparse(FIREBASE_DB_URL)
    firebase_host = parsed.hostname  # e.g., "absensi-lab-ap-default-rtdb.asia-southeast1.firebasedatabase.app"

    ping_result = ping_host(firebase_host, count=30)

    print_result("Firebase Host", firebase_host, "")
    print_result("Ping Avg", f"{ping_result['avg_ms']:.1f}ms", "PASS" if ping_result["reachable"] else "FAIL")
    print_result("Jitter", f"{ping_result['jitter_ms']:.1f}ms", "")
    print_result("Packet Loss", f"{ping_result['packet_loss_pct']:.1f}%", "")

    # ============================================================
    # BAGIAN B: Sync Latency (SQLite → Firebase)
    # ============================================================
    print("\n--- B. Sync Latency (10 iterasi) ---")
    sync_rows = []

    fp_id = TEST_USERS[0]["fingerprint_id"]
    lab = LAB_NAMES[0]  # Gunakan LAB_AP

    for i in range(1, 11):
        print(f"  [{i}/10] Sync test...", end=" ", flush=True)

        sync_result = _measure_firebase_sync(lab, fp_id)

        status = "EXCELLENT"
        if not sync_result["firebase_synced"]:
            status = "FAIL"
        elif sync_result["total_e2e_ms"] > 5000:
            status = "WARNING"

        sync_rows.append([
            i,
            sync_result["data_size_bytes"],
            sync_result["sqlite_write_ms"],
            sync_result["sync_to_firebase_ms"],
            sync_result["total_e2e_ms"],
            ping_result["jitter_ms"],
            f"{ping_result['packet_loss_pct']:.1f}%",
            status,
        ])

        print(f"E2E={sync_result['total_e2e_ms']:.0f}ms [{status}]")

        # Tap out agar bisa tap in lagi
        time.sleep(0.3)
        simulate_scan_request(fp_id, lab)
        time.sleep(0.3)

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("04_Cloud_Sync_QoS", metadata={
        "notes": f"Pengujian sync SQLite → Firebase dan QoS jaringan. Firebase: {firebase_host}"
    })

    reporter.add_sheet(
        "Sync Latency & QoS",
        headers=["No", "Data Size (bytes)", "Write to SQLite (ms)", "Sync to Firebase (ms)",
                 "Total E2E Latency (ms)", "Jitter Jaringan (ms)", "Packet Loss (%)", "Status"],
        data_rows=sync_rows,
        status_col_name="Status"
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
