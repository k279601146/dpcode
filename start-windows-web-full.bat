@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "WEB_DIR=%ROOT%\apps\web"
set "WS_URL=ws://127.0.0.1:3773"
set "DEV_URL=http://127.0.0.1:5733"

echo Starting DP Code browser dev environment...
echo Root: %ROOT%
echo Frontend: %DEV_URL%
echo Backend WS: %WS_URL%

start "DP Code Web Frontend" cmd /k "cd /d "%WEB_DIR%" && set VITE_WS_URL=%WS_URL% && node node_modules\vite\bin\vite.js --host 127.0.0.1 --port 5733 --strictPort"
timeout /t 3 /nobreak >nul

start "DP Code Web Backend" cmd /k "cd /d "%ROOT%" && node apps\server\dist\index.mjs --mode web --port 3773 --dev-url %DEV_URL% --no-browser"

echo.
echo Started:
echo - Browser frontend: http://127.0.0.1:5733/
echo - Browser backend:  http://127.0.0.1:3773/
echo.
echo Open http://127.0.0.1:5733/ in your browser.
echo You can close this launcher window now.

endlocal
