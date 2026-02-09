# Check what process is listening on port 4003 (Streamlit) and list running Python processes.
# Created ~Feb 6, 2026 during initial Streamlit setup.
# REUSABLE: handy when the dashboard port is occupied or Streamlit won't start.

Get-NetTCPConnection -LocalPort 4003 -ErrorAction SilentlyContinue | Select-Object LocalPort, State, OwningProcess | Format-Table -AutoSize
Get-Process python -ErrorAction SilentlyContinue | Select-Object Id, ProcessName, StartTime | Format-Table -AutoSize
