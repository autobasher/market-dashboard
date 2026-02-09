# List all running Python and Streamlit processes by PID and name.
# Created ~Feb 6, 2026 during initial Streamlit setup.
# REUSABLE: quick check for orphaned dashboard processes.

Get-Process | Where-Object { $_.ProcessName -match 'python|streamlit' } | Select-Object Id, ProcessName | Format-Table -AutoSize
