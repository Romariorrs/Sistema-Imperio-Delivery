@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %ERRORLEVEL%==0 (
  set PY_CMD=py -3
) else (
  set PY_CMD=python
)

%PY_CMD% -m pip install --upgrade pip
%PY_CMD% -m pip install pyinstaller

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

%PY_CMD% -m PyInstaller --onefile --name ColetorOrdersGrowth local_orders_growth_agent.py
if not exist downloads mkdir downloads
copy /Y dist\ColetorOrdersGrowth.exe downloads\ColetorOrdersGrowth.exe >nul

echo.
echo Build concluido: downloads\ColetorOrdersGrowth.exe
pause
