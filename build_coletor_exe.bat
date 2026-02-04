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

%PY_CMD% -m PyInstaller --onefile --name ColetorMacro local_macro_agent.py
if not exist downloads mkdir downloads
copy /Y dist\ColetorMacro.exe downloads\ColetorMacro.exe >nul

echo.
echo Build concluido: downloads\ColetorMacro.exe
pause
