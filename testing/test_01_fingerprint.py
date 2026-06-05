"""
SmartLab IoT Testing Framework
================================
Kategori 1: Uji Akurasi & Posisi Enroll Jari (Fingerprint)
-----------------------------------------------------------
Menguji pengaruh posisi jari (lurus, miring, sebagian) terhadap akurasi
pembacaan fingerprint sensor pada ESP32.

MEMBUTUHKAN: ESP32 fisik terhubung + Server test berjalan di port 5001.

Output: 01_Fingerprint_Accuracy_{date}.xlsx
  - Sheet 'Akurasi Fingerprint': Ringkasan per skenario
  - Sheet 'Detail Percobaan': Log detail setiap percobaan tap

Cara Pakai:
  python -m testing.test_01_fingerprint
"""

import time
import datetime

from testing.config import (
    SERVER_BASE_URL, TEST_USERS, LAB_NAMES, DB_TIME_FORMAT
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db, cleanup_firebase_sandbox,
    simulate_scan_request, print_header, print_result, timestamp_now
)
from testing.excel_reporter import ExcelReporter


# ================================================================
# SKENARIO PENGUJIAN FINGERPRINT
# ================================================================

SCENARIOS = [
    {"skenario": "Terdaftar",      "posisi": "Lurus Penuh",   "expected": "Accept", "jumlah": 20},
    {"skenario": "Terdaftar",      "posisi": "Miring",        "expected": "Accept", "jumlah": 20},
    {"skenario": "Terdaftar",      "posisi": "Sebagian (Tip)","expected": "Accept", "jumlah": 20},
    {"skenario": "Tidak Terdaftar","posisi": "Lurus Penuh",   "expected": "Reject", "jumlah": 10},
]


def run(interactive=True):
    """
    Menjalankan uji akurasi fingerprint.
    
    Args:
        interactive: Jika True, menunggu input operator untuk setiap tap.
                     Jika False, menggunakan simulasi HTTP (tanpa alat fisik).
    """
    print_header("KATEGORI 1: UJI AKURASI FINGERPRINT")

    # Setup
    create_test_db()
    detail_rows = []  # Semua percobaan individual
    summary_rows = []  # Ringkasan per skenario

    test_fp_id = TEST_USERS[0]["fingerprint_id"]  # 901 (terdaftar)
    unknown_fp_id = 999  # ID tidak terdaftar

    scenario_no = 0

    for scenario in SCENARIOS:
        scenario_no += 1
        skenario = scenario["skenario"]
        posisi = scenario["posisi"]
        expected = scenario["expected"]
        jumlah = scenario["jumlah"]

        fp_id = test_fp_id if skenario == "Terdaftar" else unknown_fp_id
        lab = LAB_NAMES[0]  # Gunakan LAB_AP untuk pengujian

        berhasil = 0
        gagal = 0

        print(f"\n--- Skenario {scenario_no}: {skenario} | Posisi: {posisi} ---")

        if interactive:
            print(f"  Instruksi: Lakukan {jumlah}x tap jari dengan posisi '{posisi}'")
            print(f"  FP ID yang digunakan: {fp_id}")
            print(f"  Tekan ENTER setelah setiap tap, atau ketik 'skip' untuk lewati skenario.")

        for i in range(1, jumlah + 1):
            if interactive:
                user_input = input(f"  [{i}/{jumlah}] Tekan ENTER setelah tap (atau 'skip'): ").strip()
                if user_input.lower() == 'skip':
                    print(f"  Skenario '{posisi}' dilewati.")
                    break

            # Kirim request ke server test
            result = simulate_scan_request(fp_id, lab)
            response_text = result["response_text"]
            latency = result["latency_ms"]

            # Tentukan actual status dari response
            if "MASUK" in response_text or "KELUAR" in response_text:
                actual_status = "Accepted"
            elif "GAGAL" in response_text:
                actual_status = "Rejected"
            else:
                actual_status = "Unknown"

            # Match check
            match = "Yes"
            if expected == "Accept" and actual_status != "Accepted":
                match = "No"
            elif expected == "Reject" and actual_status != "Rejected":
                match = "No"

            if match == "Yes":
                berhasil += 1
            else:
                gagal += 1

            # Simpan detail
            detail_rows.append([
                len(detail_rows) + 1,
                result["timestamp_request"],
                fp_id,
                skenario,
                posisi,
                actual_status,
                expected,
                match,
                round(latency, 2),
            ])

            if not interactive:
                # Jeda kecil antar request untuk simulasi
                time.sleep(0.1)

        # Hitung akurasi skenario
        total_attempts = berhasil + gagal
        akurasi = (berhasil / total_attempts * 100) if total_attempts > 0 else 0

        # Status skenario
        if akurasi >= 90:
            status = "PASS"
        elif akurasi >= 70:
            status = "WARNING"
        else:
            status = "FAIL"

        summary_rows.append([
            scenario_no, skenario, posisi, total_attempts,
            berhasil, gagal, f"{akurasi:.1f}%", status
        ])

        print_result(f"{skenario} ({posisi})", f"Akurasi {akurasi:.1f}%", status)

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("01_Fingerprint_Accuracy", metadata={
        "notes": "Pengujian akurasi fingerprint dengan variasi posisi jari."
    })

    # Sheet 1: Akurasi Fingerprint
    reporter.add_sheet(
        "Akurasi Fingerprint",
        headers=["No", "Skenario Jari", "Posisi Jari saat Tap", "Jumlah Percobaan",
                 "Berhasil", "Ditolak", "Akurasi (%)", "Status"],
        data_rows=summary_rows,
        status_col_name="Status"
    )

    # Sheet 2: Detail Percobaan
    reporter.add_sheet(
        "Detail Percobaan",
        headers=["No", "Timestamp", "Fingerprint ID", "Skenario Jari", "Posisi Jari",
                 "Actual Status", "Expected", "Match?", "Waktu Respon Server (ms)"],
        data_rows=detail_rows,
        status_col_name="Match?",
        left_align_cols=["Timestamp", "Actual Status"]
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
    import sys
    # Default: interactive mode (membutuhkan operator)
    # Gunakan --simulate untuk mode simulasi tanpa alat fisik
    interactive_mode = "--simulate" not in sys.argv
    if not interactive_mode:
        print("[MODE] Simulasi (tanpa alat fisik)")
    run(interactive=interactive_mode)
