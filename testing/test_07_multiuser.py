"""
SmartLab IoT Testing Framework
================================
Kategori 7: Uji Multiuser — Antrian & Validasi Akses Ganda
-------------------------------------------------------------
Menguji:
  - Antrian concurrent: 3 lab tap bersamaan, verifikasi urutan masuk DB
  - Validasi akses ganda: User yang sudah aktif di satu lab
    DITOLAK saat tap di lab lain

Output: 07_Multiuser_{date}.xlsx
  - Sheet 'Antrian Data': Urutan masuk saat concurrent
  - Sheet 'Validasi Akses Multi-Lab': Pencegahan double login

Cara Pakai:
  python -m testing.test_07_multiuser
"""

import time
import concurrent.futures
import threading

from testing.config import (
    SERVER_BASE_URL, TEST_USERS, LAB_NAMES
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db, cleanup_firebase_sandbox,
    get_test_db_conn, simulate_scan_request,
    print_header, print_result, timestamp_now
)
from testing.excel_reporter import ExcelReporter


def run():
    """Menjalankan uji multiuser (antrian dan validasi akses)."""
    print_header("KATEGORI 7: UJI MULTIUSER (ANTRIAN & VALIDASI AKSES)")

    # Setup
    create_test_db()

    # ============================================================
    # BAGIAN A: Antrian Data (Concurrent 3 Lab)
    # ============================================================
    print("\n--- A. Antrian Data (Concurrent 3 Lab, 5 iterasi) ---")
    queue_rows = []
    overall_test_no = 0

    for iteration in range(1, 6):
        print(f"\n  Iterasi {iteration}/5:")

        # Kirim 3 request bersamaan dari 3 lab berbeda
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for idx, lab in enumerate(LAB_NAMES):
                fp_id = TEST_USERS[idx]["fingerprint_id"]
                thread_name = f"Thread-{idx+1}"
                futures[(thread_name, lab, fp_id)] = executor.submit(
                    simulate_scan_request, fp_id, lab
                )

            # Kumpulkan hasil dan urutkan berdasarkan timestamp response
            results = []
            for (thread_name, lab, fp_id), future in futures.items():
                res = future.result()
                results.append({
                    "thread": thread_name,
                    "fp_id": fp_id,
                    "lab": lab,
                    "result": res,
                })

        # Sort berdasarkan timestamp response untuk menentukan urutan masuk
        results.sort(key=lambda x: x["result"]["timestamp_response"])

        for order, r in enumerate(results, start=1):
            overall_test_no += 1
            res = r["result"]
            selisih_ms = res["latency_ms"]

            status = "PASS" if res["success"] else "FAIL"

            queue_rows.append([
                overall_test_no,
                r["thread"],
                r["fp_id"],
                r["lab"],
                res["timestamp_request"],
                res["timestamp_response"],
                order,
                round(selisih_ms, 2),
                status,
            ])

            print(f"    {r['thread']} ({r['lab']}): Urutan {order}, Latency {selisih_ms:.1f}ms [{status}]")

        # Tap out semua sebelum iterasi berikutnya
        time.sleep(0.3)
        for idx, lab in enumerate(LAB_NAMES):
            simulate_scan_request(TEST_USERS[idx]["fingerprint_id"], lab)
            time.sleep(0.1)
        time.sleep(0.3)

    # ============================================================
    # BAGIAN B: Validasi Akses Multi-Lab (Double Login Prevention)
    # ============================================================
    print("\n--- B. Validasi Akses Multi-Lab (Double Login Prevention) ---")
    validation_rows = []

    # User A (FP=901) punya akses ke semua lab
    test_user = TEST_USERS[0]
    fp_id = test_user["fingerprint_id"]

    # Skenario: Tap MASUK di LAB_AP, lalu coba tap di LAB_TEKDIG (harus GAGAL)
    test_cases = [
        {"tap1_lab": "LAB_AP",     "tap2_lab": "LAB_TEKDIG"},
        {"tap1_lab": "LAB_TEKDIG", "tap2_lab": "LAB_MIKRO"},
        {"tap1_lab": "LAB_MIKRO",  "tap2_lab": "LAB_AP"},
    ]

    for case_no, case in enumerate(test_cases, start=1):
        lab1 = case["tap1_lab"]
        lab2 = case["tap2_lab"]

        print(f"\n  Skenario {case_no}: Tap MASUK di {lab1}, coba tap di {lab2}")

        # Tap 1: MASUK di lab1
        result1 = simulate_scan_request(fp_id, lab1)
        hasil_tap1 = result1["response_text"]
        print(f"    Tap 1 ({lab1}): {hasil_tap1}")

        time.sleep(0.3)

        # Tap 2: Coba MASUK di lab2 (harus ditolak karena aktif di lab1)
        result2 = simulate_scan_request(fp_id, lab2)
        hasil_tap2 = result2["response_text"]
        print(f"    Tap 2 ({lab2}): {hasil_tap2}")

        # Verifikasi: Tap 2 harus GAGAL
        expected_tap2 = "DITOLAK"
        tap2_ditolak = "GAGAL" in hasil_tap2 or "Aktif" in hasil_tap2
        match = "Yes" if tap2_ditolak else "No"

        validation_rows.append([
            case_no,
            fp_id,
            lab1,
            hasil_tap1,
            lab2,
            hasil_tap2 if tap2_ditolak else f"ERROR: {hasil_tap2}",
            match,
            hasil_tap2,
        ])

        print_result(f"Double Login {lab1}→{lab2}", match, "PASS" if match == "Yes" else "FAIL")

        # Tap out dari lab1 untuk reset
        time.sleep(0.3)
        simulate_scan_request(fp_id, lab1)
        time.sleep(0.3)

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("07_Multiuser", metadata={
        "notes": "Pengujian antrian concurrent 3 lab dan validasi pencegahan double login."
    })

    reporter.add_sheet(
        "Antrian Data",
        headers=["No", "Thread ID", "Fingerprint ID", "Lab Terkait",
                 "Timestamp Kirim", "Timestamp Diterima Server", "Urutan Masuk DB",
                 "Selisih (ms)", "Status"],
        data_rows=queue_rows,
        status_col_name="Status",
        left_align_cols=["Timestamp Kirim", "Timestamp Diterima Server"]
    )

    reporter.add_sheet(
        "Validasi Akses Multi-Lab",
        headers=["No", "Asisten ID", "Tap 1 (Lab)", "Hasil Tap 1",
                 "Tap 2 (Lab Lain)", "Hasil Tap 2 (Expected: DITOLAK)",
                 "Match?", "Response Server"],
        data_rows=validation_rows,
        status_col_name="Match?",
        left_align_cols=["Hasil Tap 1", "Hasil Tap 2 (Expected: DITOLAK)", "Response Server"]
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
