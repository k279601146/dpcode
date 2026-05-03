@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "WEB_DIR=%ROOT%\apps\web"
set "DESKTOP_DIR=%ROOT%\apps\desktop"
set "STATE_DIR=%ROOT%\.dpcode-clean"

if not exist "%STATE_DIR%" (
  mkdir "%STATE_DIR%"
)

echo Starting DP Code dev environment...
echo Root: %ROOT%
echo State: %STATE_DIR%

start "DP Code Web" cmd /k "cd /d "%WEB_DIR%" && node node_modules\vite\bin\vite.js --host 127.0.0.1 --port 5733 --strictPort"
timeout /t 3 /nobreak >nul

start "DP Code Desktop Bundle" cmd /k "cd /d "%DESKTOP_DIR%" && bun run dev:bundle"
timeout /t 3 /nobreak >nul

start "DP Code Desktop App" cmd /k "cd /d "%DESKTOP_DIR%" && set ELECTRON_RENDERER_PORT=5733 && set T3CODE_HOME=%STATE_DIR% && set DPCODE_HOME=%STATE_DIR% && node scripts\dev-electron.mjs"

echo.
echo Started:
echo - Web: http://127.0.0.1:5733/
echo - Desktop bundle watcher
echo - Desktop app
echo.
echo You can close this launcher window now.

endlocal
