@echo off
setlocal

REM --- KHAI BAO DUONG DAN ---
set "PY=D:\TQL\AutoSAP\.venv\Scripts\python.exe"
set "SCRIPT=D:\TQL\AutoSAP\main.py"
set "WORKDIR=D:\TQL\AutoSAP"

REM --- KIEM TRA TON TAI ---
if not exist "%PY%" (
    echo Loi: Khong tim thay python.exe trong .venv: "%PY%"
    echo Goi y: tao lai venv: python -m venv D:\TQL\AutoSAP\.venv
    pause
    exit /b 1
)
if not exist "%SCRIPT%" (
    echo Loi: Khong tim thay file Python: "%SCRIPT%"
    pause
    exit /b 1
)

REM --- CHUYEN THU MUC LAM VIEC (neu main.py dung relative path) ---
pushd "%WORKDIR%" >nul 2>&1

echo Dang chay chuong trinh...
"%PY%" "%SCRIPT%"
set ERR=%ERRORLEVEL%

popd >nul 2>&1

echo ---
if %ERR% neq 0 (
    echo Chuong trinh ket thuc voi ma loi %ERR%.
) else (
    echo Da hoan thanh thanh cong.
)
pause
endlocal