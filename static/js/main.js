/* ============================================================
   SmartLab SESL — Shared JavaScript Utilities
   ============================================================ */

// --- Sidebar Toggle (Desktop: collapse/expand, Mobile: overlay) ---
function toggleSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    const isMobile = window.innerWidth <= 768;

    if (isMobile) {
        sidebar.classList.toggle('open');
        overlay.classList.toggle('show');
    } else {
        sidebar.classList.toggle('collapsed');
        localStorage.setItem('sidebar-collapsed', sidebar.classList.contains('collapsed'));
        closePopupRekap();
        hideTooltip();
        // Resize Plotly charts after CSS transition
        setTimeout(() => {
            document.querySelectorAll('.js-plotly-plot').forEach(el => {
                try { Plotly.Plots.resize(el); } catch(e) {}
            });
        }, 450);
    }
}

// --- Header Click (expand sidebar when collapsed, navigate to home when expanded) ---
function handleHeaderClick(event) {
    const sidebar = document.querySelector('.sidebar');
    if (sidebar && sidebar.classList.contains('collapsed')) {
        event.preventDefault();
        event.stopPropagation();
        toggleSidebar();
    } else {
        // Expanded: navigate to home
        window.location.href = '/home';
    }
}

function closeSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    if (sidebar) sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('show');
}

// --- Subnav Toggle (expanded mode) ---
function toggleSubnav(id) {
    const subnav = document.getElementById(id);
    const arrow = document.querySelector(`[data-target="${id}"] .nav-arrow`);
    if (subnav) subnav.classList.toggle('open');
    if (arrow) arrow.classList.toggle('rotated');
}

// --- Rekap Gaji Click Handler ---
function handleRekapClick(event) {
    const sidebar = document.querySelector('.sidebar');
    const isCollapsed = sidebar && sidebar.classList.contains('collapsed');

    if (isCollapsed) {
        event.preventDefault();
        event.stopPropagation();
        togglePopupRekap(event);
    } else {
        toggleSubnav('subnav-rekap');
    }
}

// --- Popup Rekap (collapsed mode floating dropdown) ---
function togglePopupRekap(event) {
    const popup = document.getElementById('popup-rekap');
    const trigger = document.getElementById('rekap-trigger');
    if (!popup || !trigger) return;

    if (popup.style.display === 'block') {
        closePopupRekap();
        return;
    }

    const rect = trigger.getBoundingClientRect();
    const sidebarCollapsedWidth = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--sidebar-collapsed')) || 64;
    popup.style.left = (sidebarCollapsedWidth + 8) + 'px';
    popup.style.top = rect.top + 'px';
    popup.style.display = 'block';
    trigger.classList.add('popup-active');
}

function closePopupRekap() {
    const popup = document.getElementById('popup-rekap');
    const trigger = document.getElementById('rekap-trigger');
    if (popup) popup.style.display = 'none';
    if (trigger) trigger.classList.remove('popup-active');
}

document.addEventListener('click', function(e) {
    const popup = document.getElementById('popup-rekap');
    const trigger = document.getElementById('rekap-trigger');
    if (popup && popup.style.display === 'block') {
        if (!popup.contains(e.target) && e.target !== trigger && !trigger.contains(e.target)) {
            closePopupRekap();
        }
    }
});

// --- JS Tooltip System (for collapsed sidebar) ---
let tooltipTimeout = null;

function showTooltipFor(navItem) {
    const sidebar = document.querySelector('.sidebar');
    if (!sidebar || !sidebar.classList.contains('collapsed')) return;
    if (navItem.classList.contains('popup-active')) return;

    const tooltip = document.getElementById('sidebar-tooltip');
    if (!tooltip) return;

    const text = navItem.getAttribute('data-tooltip');
    if (!text) return;

    const rect = navItem.getBoundingClientRect();
    const sidebarCollapsedWidth = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--sidebar-collapsed')) || 64;

    tooltip.textContent = text;
    tooltip.style.left = (sidebarCollapsedWidth + 8) + 'px';
    tooltip.style.top = (rect.top + rect.height / 2) + 'px';
    tooltip.style.transform = 'translateY(-50%)';
    tooltip.classList.add('visible');
}

function hideTooltip() {
    const tooltip = document.getElementById('sidebar-tooltip');
    if (tooltip) tooltip.classList.remove('visible');
}

function initTooltips() {
    const navItems = document.querySelectorAll('.sidebar .nav-item[data-tooltip]');
    navItems.forEach(item => {
        item.addEventListener('mouseenter', function() {
            clearTimeout(tooltipTimeout);
            showTooltipFor(this);
        });
        item.addEventListener('mouseleave', function() {
            tooltipTimeout = setTimeout(hideTooltip, 100);
        });
    });
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

// --- Badge Helper ---
function badgeHelper(is_synced, sync_action) {
    if (is_synced) return '<span class="badge badge-synced">✓ Synced</span>';
    if (sync_action === 'INSERT') return '<span class="badge badge-insert">+ Pending Add</span>';
    if (sync_action === 'UPDATE') return '<span class="badge badge-pending">● Pending Edit</span>';
    if (sync_action === 'DELETE') return '<span class="badge badge-delete">✕ Pending Del</span>';
    return '<span class="badge badge-pending">● Pending</span>';
}

// --- Confirm Dialog ---
function confirmAction(message, onConfirm) {
    if (confirm(message)) onConfirm();
}

// --- Init ---
document.addEventListener('DOMContentLoaded', function () {
    const overlay = document.querySelector('.sidebar-overlay');
    if (overlay) overlay.addEventListener('click', closeSidebar);

    // Restore sidebar state
    const sidebar = document.querySelector('.sidebar');
    if (sidebar && window.innerWidth > 768) {
        if (localStorage.getItem('sidebar-collapsed') === 'true') {
            sidebar.classList.add('collapsed');
        }
    }

    // Remove instant-load class → re-enable CSS transitions
    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            document.documentElement.classList.remove('sidebar-is-collapsed');
        });
    });

    // Init tooltip system
    initTooltips();
});
