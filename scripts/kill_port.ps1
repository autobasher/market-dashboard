# Kill whatever process is holding port 4003 (Streamlit default).
# Created ~Feb 6, 2026 during initial Streamlit setup.
# REUSABLE: use when a zombie Streamlit process blocks the port.

$conns = Get-NetTCPConnection -LocalPort 4003 -ErrorAction SilentlyContinue
foreach ($c in $conns) {
    Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
}
