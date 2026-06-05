"""
SmartLab IoT Testing Framework
================================
Kategori 3: Uji Latensi Sistem (Single & Simultan 3 Lab)
---------------------------------------------------------
Mengukur performa kecepatan respon server Flask:
  - Single lab tap (per lab)
  - Simultan 3 lab tap bersamaan (concurrent threads)

Output: 03_System_Latency_{date}.xlsx
  - Sheet 'Latensi Single': Respon per-request
  - Sheet 'Latensi Simultan': Hasil concurrent 3 lab
  - Sheet 'Statistik': Ringkasan statistik (mean, median, p95, p99)

Cara Pakai:
  python -m testing.test_03_latensi_sistem
"""

import time
import statistics
import concurrent.futures

from testing.config import (
    SERVER_BASE_URL, TEST_USERS, LAB_NAMES
)
from testing.test_helpers import (
    create_test_db, cleanup_test_db, cleanup_firebase_sandbox,
    simulate_scan_request, print_header, print_result, timestamp_now
)
from testing.excel_reporter import ExcelReporter


def _percentile(data, p):
    """Hitung percentile ke-p dari data list."""
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def run():
    """Menjalankan uji latensi sistem."""
    print_header("KATEGORI 3: UJI LATENSI SISTEM")

    # Setup
    create_test_db()

    test_user = TEST_USERS[0]  # User A (akses semua lab)
    fp_id = test_user["fingerprint_id"]

    # ============================================================
    # BAGIAN A: Latensi Single (Per Lab)
    # ============================================================
    print("\n--- A. Latensi Single (per lab, 10x tiap lab) ---")
    single_rows = []
    all_single_latencies = []
    test_no = 0

    for lab in LAB_NAMES:
        print(f"\n  Lab: {lab}")
        for i in range(10):
            test_no += 1
            result = simulate_scan_request(fp_id, lab)
            latency = result["latency_ms"]
            all_single_latencies.append(latency)

            single_rows.append([
                test_no,
                lab,
                fp_id,
                result["timestamp_request"],
                result["timestamp_response"],
                round(latency, 3),
                result["status_code"],
                result["response_text"][:80],  # Truncate long responses
            ])

            # Tap out setelah tap in (alternating MASUK/KELUAR)
            time.sleep(0.05)

        avg_lab = statistics.mean([r[5] for r in single_rows if r[1] == lab])
        print_result(lab, f"Avg={avg_lab:.1f}ms ({10} requests)", "PASS")

    # ============================================================
    # BAGIAN B: Latensi Simultan (3 Lab Bersamaan)
    # ============================================================
    print("\n--- B. Latensi Simultan (3 lab bersamaan, 10 iterasi) ---")
    simultan_rows = []
    all_simultan_latencies = []

    for iteration in range(1, 11):
        # Kirim 3 request bersamaan menggunakan ThreadPool
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for lab in LAB_NAMES:
                futures[lab] = executor.submit(simulate_scan_request, fp_id, lab)

            results = {}
            for lab, future in futures.items():
                results[lab] = future.result()

        latencies = {lab: results[lab]["latency_ms"] for lab in LAB_NAMES}
        lat_values = list(latencies.values())
        avg_lat = statistics.mean(lat_values)
        max_lat = max(lat_values)
        std_dev = statistics.stdev(lat_values) if len(lat_values) > 1 else 0

        all_simultan_latencies.extend(lat_values)

        simultan_rows.append([
            iteration,
            "Simultan 3 Lab",
            ", ".join(LAB_NAMES),
            round(latencies["LAB_AP"], 2),
            round(latencies["LAB_TEKDIG"], 2),
            round(latencies["LAB_MIKRO"], 2),
            round(avg_lat, 2),
            round(max_lat, 2),
            round(std_dev, 2),
        ])

        # Tap out semua sebelum iterasi berikutnya
        time.sleep(0.1)

    avg_simultan = statistics.mean(all_simultan_latencies) if all_simultan_latencies else 0
    print_result("Simultan 3 Lab", f"Avg={avg_simultan:.1f}ms ({len(simultan_rows)} iterasi)", "PASS")

    # ============================================================
    # BAGIAN C: Tabel Statistik
    # ============================================================
    print("\n--- C. Statistik ---")

    def calc_stats(data, label):
        if not data:
            return [label, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"]
        return [
            label,
            round(statistics.mean(data), 2),
            round(statistics.median(data), 2),
            round(statistics.stdev(data), 2) if len(data) > 1 else 0,
            round(min(data), 2),
            round(max(data), 2),
            round(_percentile(data, 95), 2),
            round(_percentile(data, 99), 2),
        ]

    stats_rows = [
        calc_stats(all_single_latencies, "Single Tap"),
        calc_stats(all_simultan_latencies, "Simultan 3 Lab"),
    ]

    for row in stats_rows:
        print_result(row[0], f"Mean={row[1]}ms, P95={row[6]}ms, P99={row[7]}ms", "PASS")

    # --- GENERATE EXCEL ---
    reporter = ExcelReporter("03_System_Latency", metadata={
        "notes": f"Latensi single tap (10x per lab) dan simultan 3 lab (10 iterasi). Total {test_no + len(simultan_rows)*3} requests."
    })

    # Sheet 1: Latensi Single
    reporter.add_sheet(
        "Latensi Single",
        headers=["No", "Lab", "FP ID", "Waktu Request", "Waktu Response",
                 "Latency (ms)", "HTTP Status", "Response Body"],
        data_rows=single_rows,
        left_align_cols=["Waktu Request", "Waktu Response", "Response Body"]
    )

    # Sheet 2: Latensi Simultan
    reporter.add_sheet(
        "Latensi Simultan",
        headers=["No", "Skenario", "Lab Terlibat",
                 "Latency LAB_AP (ms)", "Latency LAB_TEKDIG (ms)", "Latency LAB_MIKRO (ms)",
                 "Avg (ms)", "Max (ms)", "Std Dev (ms)"],
        data_rows=simultan_rows
    )

    # Sheet 3: Statistik
    reporter.add_sheet(
        "Statistik",
        headers=["Metrik", "Mean (ms)", "Median (ms)", "Std Dev (ms)",
                 "Min (ms)", "Max (ms)", "P95 (ms)", "P99 (ms)"],
        data_rows=stats_rows
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
