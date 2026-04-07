/* ============================================================
   SmartLab SESL — Shared JavaScript Utilities
   ============================================================ */

// --- Sidebar Toggle ---
function toggleSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    sidebar.classList.toggle('open');
    overlay.classList.toggle('show');
}

function closeSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    if (sidebar) sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('show');
}

// --- Subnav Toggle ---
function toggleSubnav(id) {
    const subnav = document.getElementById(id);
    const arrow = document.querySelector(`[data-target="${id}"] .nav-arrow`);
    if (subnav) subnav.classList.toggle('open');
    if (arrow) arrow.classList.toggle('rotated');
}

// --- Modal ---
function openModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.style.display = 'flex';
        setTimeout(() => modal.classList.add('show'), 10);
    }
}

function closeModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.remove('show');
        setTimeout(() => { modal.style.display = 'none'; }, 200);
    }
}

// Close modal on backdrop click
document.addEventListener('click', function(e) {
    if (e.target.classList.contains('modal-backdrop')) {
        const modal = e.target.closest('.modal');
        if (modal) closeModal(modal.id);
    }
});

// --- Tab Switching ---
function openTab(evt, tabName) {
    const tabcontents = document.querySelectorAll('.tabcontent');
    tabcontents.forEach(tc => tc.classList.remove('active-tab'));

    const tablinks = document.querySelectorAll('.tab-btn');
    tablinks.forEach(tl => tl.classList.remove('active'));

    const tab = document.getElementById(tabName);
    if (tab) tab.classList.add('active-tab');
    if (evt && evt.currentTarget) evt.currentTarget.classList.add('active');
}

// --- Toast Notifications ---
function showToast(message, type = 'success') {
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    const icon = type === 'success' ? '✓' : type === 'error' ? '✕' : '⚠';
    toast.innerHTML = `<span style="font-size:1.1rem;">${icon}</span> ${message}`;
    
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(60px)';
        toast.style.transition = 'all 300ms ease';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// --- Badge Helper (for sync status) ---
function badgeHelper(is_synced, sync_action) {
    if (is_synced) return '<span class="badge badge-synced">✓ Synced</span>';
    if (sync_action === 'INSERT') return '<span class="badge badge-insert">+ Pending Add</span>';
    if (sync_action === 'UPDATE') return '<span class="badge badge-pending">● Pending Edit</span>';
    if (sync_action === 'DELETE') return '<span class="badge badge-delete">✕ Pending Del</span>';
    return '<span class="badge badge-pending">● Pending</span>';
}

// --- Confirm Dialog (styled alternative to default confirm) ---
function confirmAction(message, onConfirm) {
    if (confirm(message)) {
        onConfirm();
    }
}

// --- Close sidebar on overlay click ---
document.addEventListener('DOMContentLoaded', function () {
    const overlay = document.querySelector('.sidebar-overlay');
    if (overlay) {
        overlay.addEventListener('click', closeSidebar);
    }
});
