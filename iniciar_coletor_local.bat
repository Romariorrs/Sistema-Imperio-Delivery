@echo off
setlocal
cd /d "%~dp0"
if exist .venv\Scripts\python.exe (
  .venv\Scripts\python.exe local_macro_agent.py
) else (
  where py >nul 2>nul
  if %ERRORLEVEL%==0 (
    py -3 local_macro_agent.py
  ) else (
    python local_macro_agent.py
  )
)
pause
