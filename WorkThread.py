# class xử lý công việc trong thread riêng
import threading
import subprocess
from Log import Logger
# import paramiko
from sqlalchemy import create_engine, text
import csv
from pathlib import Path
# main.py
import sys, os
import pyautogui
import ctypes
import subprocess
import time
import pygetwindow as gw
import os
import win32process
# import pygetwindow as gw
import psutil

# Force pythonnet to use .NET Core
os.environ['PYTHONNET_RUNTIME'] = 'coreclr'

# 1) Thêm THƯ MỤC chứa DLL vào PATH (để clr tìm thấy AdomdClient.dll)
dll_dir = os.path.abspath(r".\adomd_nuget\lib\net8.0")
os.environ['PATH'] += os.pathsep + dll_dir
# Thêm cả thư mục native nếu cần
native_dir = os.path.abspath(r".\adomd_nuget\runtimes\win-x64\native")
os.environ['PATH'] += os.pathsep + native_dir

# 2) Load the assembly manually
import pythonnet
pythonnet.load('coreclr')

import clr
clr.AddReference(os.path.join(os.getcwd(), 'modun/Microsoft.AnalysisServices.Runtime.Core.dll'))
clr.AddReference(os.path.join(os.getcwd(), 'modun/Microsoft.AnalysisServices.Runtime.Windows.dll'))
clr.AddReference(os.path.join(os.getcwd(), 'modun/Microsoft.AnalysisServices.AdomdClient.dll'))
# 3) BÂY GIỜ mới import pyadomd
from pyadomd import Pyadomd
import pandas as pd
# Import thêm cho SQL Server
import pyodbc
from sqlalchemy import create_engine
import urllib.parse
import subprocess
import re



class WorkThread(threading.Thread):
    # SQL
    server = "10.239.1.54"
    database = "Data_qad"
    schema = "dbo"
    username = "sa"
    password = "123456"
    pyautogui.PAUSE = 0.2
    def __init__(self):
        threading.Thread.__init__(self)
        self.log = Logger()
        
        
        # self.log.info("Application initialized")
    
    def find_powerbi_port(self) -> str | None:
        """Tìm port của Power BI Desktop (msmdsrv.exe) trên máy local."""
        try:
            tasklist = subprocess.check_output(["tasklist", "/FI", "IMAGENAME eq msmdsrv.exe", "/FO", "CSV", "/NH"], text=True)
            if not tasklist.strip():
                return None

            # Lấy PID từ tasklist output: "msmdsrv.exe","1234",...
            pid = tasklist.split(",")[1].strip('"')
            netstat = subprocess.check_output(["netstat", "-ano"], text=True)
            # Tìm line có PID và đang LISTENING trên localhost
            pattern = re.compile(r"^\s*TCP\s+127\.0\.0\.1:(\d+)\s+.*LISTENING\s+" + re.escape(pid), re.MULTILINE)
            match = pattern.search(netstat)
            if match:
                return match.group(1)
        except Exception:
            return None

    def Check_Status(self, SYSTEM):
        try:
            self.log.info(f"Check trạng thái {SYSTEM}")
            conect = self.conn()
            # Kiểm tra nếu Eror
            with conect.begin() as connection:
                result = connection.execute(text(f"SELECT TOP (1) [ID] ,[SYSTEM] ,[TIME] FROM [ACWO].[dbo].[DataStatus]  where system = '{SYSTEM}' order by id desc"))
                row = result.fetchone()
                
                if row:
                    self.log.info(f"Dữ liệu gần nhất: ID={row[0]}, SYSTEM={row[1]}, TIME={row[2]}")
                    print(f"Dữ liệu: {row[2]}")
                    return row[2]
                else:
                    self.log.info("Không có dữ liệu.")
                    print("Không có dữ liệu.")
        except Exception as e:
            self.log.error(f"error: {e}")
    
    
    def Insert_SQL(self, SQL):
        try:
            conect = self.conn()
            # Kiểm tra nếu Eror
            with conect.begin() as connection:
                result = connection.execute(text(f"{SQL}"))
                if SQL.strip().upper().startswith('SELECT'):
                    row = result.fetchone()
                    
                    if row:
                        self.log.info(f"Dữ liệu gần nhất: ID={row[0]}, SYSTEM={row[1]}, TIME={row[2]}")
                        print(f"Dữ liệu: {row}")
                        # return row
                    else:
                        self.log.info("Không có dữ liệu.")
                        print("Không có dữ liệu.")
                else:
                    self.log.info("SQL executed successfully")
        except Exception as e:
            self.log.error(f"error: {e}")
            
            
    def conn(self):
        # Database connection and data processing
        try:
            server = self.server
            database = self.database
            username = self.username
            password = self.password
            driver = 'ODBC Driver 18 for SQL Server'

            connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes'
            engine = create_engine(connection_string)
            self.log.info("connect success")
            return engine
        except Exception as e:
            self.log.error(f"Error connecting to database: {e}")

    def PBIToSql(self):
        try:
            # 3a) Port có thể được truyền qua môi trường (tiện khi chạy nhiều lần)
            port = os.environ.get("PBIPORT") or self.find_powerbi_port() or "51328"
            if os.environ.get("PBIPORT"):
                print(f"Using port from PBIPORT environment variable: {port}")
            elif port != "51328":
                print(f"Detected Power BI port: {port}")
            else:
                print("Using default port 51328 (may be wrong). Set PBIPORT or update PORT variable.")

            conn_str = f"Data Source=localhost:{port}"
            with Pyadomd(conn_str) as conn:
                # 1) Liệt kê các bảng (Dimensions) trong model
                cube = conn.conn.Cubes[0]
                dimensions = cube.Dimensions

                tables = []
                for i in range(dimensions.Count):
                    dim = dimensions[i]
                    tables.append({
                        'TABLE_NAME': dim.Name,
                        'HIERARCHIES': dim.Hierarchies.Count,
                        'ATTRIBUTES': dim.AttributeHierarchies.Count,
                    })

                tables_df = pd.DataFrame(tables)
                print("\n=== Tables in the model ===")
                print(tables_df.to_string(index=False))

                # 2) Hiển thị cột/thuộc tính cho bảng đầu tiên (nếu cần)
                if not tables_df.empty:
                    first_table = tables_df.loc[0, 'TABLE_NAME']
                    dim = next(d for d in dimensions if d.Name == first_table)
                    columns = [dim.AttributeHierarchies[i].Name for i in range(dim.AttributeHierarchies.Count)]
                    print(f"\n=== Columns in table '{first_table}' ===")
                    for col in columns:
                        print(f" - {col}")

                # 3) Refresh metadata (and optionally data)
                #    This calls AdomdConnection.RefreshMetadata() which refreshes metadata
                #    and helps ensure the schema is up-to-date.
                #
                #    Note: RefreshMetadata does NOT refresh underlying data sources.
                #    For a full data refresh, use Power BI refresh workflows (Power BI API / REST / UI).
                do_refresh = True
                if do_refresh:
                    conn.conn.RefreshMetadata()
                    print('\nModel metadata refresh triggered.')

            

                # 5) Đẩy dữ liệu từ tất cả bảng PBI vào SQL Server
                print("\n=== Đẩy dữ liệu vào SQL Server ===")

                # Cấu hình SQL Server (thay đổi theo server của bạn)
                sql_server = "10.239.1.54"  # Thay bằng server name (ví dụ: "DESKTOP-ABC\SQLEXPRESS")
                sql_database = "DB_SAP_DWH"  # Thay bằng database name (tạo database trước nếu chưa có)
                sql_username = "sa"  # Thay bằng username
                sql_password = "123456"  # Thay bằng password

                
                import re

                def strip_table_prefix(colname: str) -> str:
                    # Nếu dạng "SOMETHING[NAME]" thì lấy NAME
                    m = re.match(r'^.*\[(.+?)\]$', colname)
                    return m.group(1) if m else colname

                
                try:
                    # Tạo connection string cho SQL Server
                    sql_conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"
                    sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(sql_conn_str)}")

                    # Test connection
                    with sql_engine.connect() as test_conn:
                        print("✅ Kết nối SQL Server thành công!")

                    # Lặp qua tất cả bảng và đẩy dữ liệu
                    for table_name in tables_df['TABLE_NAME']:
                        if table_name == 'Measures':  # Bỏ qua bảng Measures (không phải data table)
                            continue

                        print(f"\nĐang xử lý bảng: {table_name}")

                        try:
                            # Query toàn bộ dữ liệu từ bảng PBI (giới hạn 1000 hàng để demo)
                            dax_query = f"EVALUATE ({table_name})"
                            with conn.cursor().execute(dax_query) as cur:
                                cols = [c[0] for c in cur.description]
                                rows = [list(r) for r in cur.fetchall()]
                                clean = [strip_table_prefix(s) for s in cols]
                                # print(clean)
                                
                            
                            

                            if not rows:
                                print(f"  Bảng {table_name} trống, bỏ qua.")
                                continue

                            df = pd.DataFrame(rows, columns=clean)

                            # Đẩy vào SQL Server (replace nếu bảng đã tồn tại)
                            df.to_sql(table_name, sql_engine, if_exists='replace', index=False)
                            print(f"  ✅ Đã đẩy {len(df)} hàng vào bảng {table_name} trong SQL Server")

                        except Exception as e:
                            print(f"  ❌ Lỗi khi xử lý bảng {table_name}: {e}")

                    print("\n🎉 Hoàn tất đẩy dữ liệu từ Power BI vào SQL Server!")

                except Exception as e:
                    print(f"❌ Lỗi kết nối SQL Server: {e}")
                    print("💡 Hãy kiểm tra:")
                    print("  - SQL Server có chạy không?")
                    print("  - Database 'PowerBI_Data' có tồn tại không?")
                    print("  - Username/password đúng không?")
                    print("  - ODBC Driver 17 for SQL Server có cài không?")
        except Exception as e:
            print("Failed to connect to Power BI Analysis Services:")
            print(f"  Connection string: {conn_str}")
            print(f"  Error: {e}")
            raise
        
        
    def timanh05(self, img):
            try:
                button_location = pyautogui.locateOnScreen(img, confidence=0.5)
                # print(button_location)
                # pyautogui.moveTo(button_location)
                return True
            except Exception as e:
                # print("Không tìm thấy nút")
                return False

    def timanh08(self, img):
            try:
                button_location = pyautogui.locateOnScreen(img, confidence=0.8)
                # print(button_location)
                pyautogui.moveTo(button_location)
                return button_location
            except Exception as e:
                # print("Không tìm thấy nút")
                return False
            
    # kiểm tra app đã mở hay chưa?
    def is_app_open(self ,window_title):
                return any(window_title in title for title in gw.getAllTitles())
                    

    def bring_app_to_front(self, window_title):
                try:
                    window = gw.getWindowsWithTitle(window_title)[0]
                    # Khôi phục cửa sổ nếu nó đang ở trạng thái minimize
                    if window.isMinimized:
                        window.restore()
                        # Đưa cửa sổ lên trên cùng
                        window.activate()
                        print(f"Cửa sổ '{window_title}' đã được đưa lên trên cùng.")
                except IndexError:
                    print(f"Cửa sổ '{window_title}' không tìm thấy.")


    def minimize_window(self, window_title: str):
            try:
                window = gw.getWindowsWithTitle(window_title)[0]
                if not window.isMinimized:
                    window.minimize()   # Thu nhỏ xuống taskbar
                    print(f"Đã thu nhỏ: '{window_title}'")
                else:
                    print(f"Cửa sổ '{window_title}' đã ở trạng thái thu nhỏ.")
            except IndexError:
                print(f"Không tìm thấy cửa sổ: '{window_title}'")


    def openapps_btn(self, path):
                    try:
                        os.startfile(path)
                    except FileNotFoundError:
                        print("Không tìm thấy app(Sai Link)")
                        # messagebox.showinfo("Thông Báo", "Không tìm thấy app(Sai Link)")
                    except Exception as e:
                        print(f"Lỗi: {e}")
                        

    def check_openapp(self, Title, FilePath):
            if self.is_app_open(Title):
                self.bring_app_to_front(Title)
            else:
                self.openapps_btn(FilePath)
                
    
    def force_kill_window(self, window_title: str):
        try:
            window = gw.getWindowsWithTitle(window_title)[0]
            hwnd = window._hWnd

            # lấy PID
            tid, pid = win32process.GetWindowThreadProcessId(hwnd)
            p = psutil.Process(pid)
            p.terminate()  # hoặc p.kill() nếu cần mạnh hơn

            print(f"Đã ép tắt ứng dụng '{window_title}' (PID: {pid}).")
        except IndexError:
            print(f"Không tìm thấy cửa sổ: '{window_title}'")
        except Exception as e:
            print("Lỗi:", e)
    
       
    def Load_Data(self):
        try:
            self.log.info("Load dữ liệu PBI")
            Refresh = r'IMG/Refresh.png'
            Loading = r'IMG/Loading.png'
            title = "DW_SAP_PBI"
            filepath = r"DW_SAP_PBI.pbix"
            self.force_kill_window(title)
            self.check_openapp(title, filepath)
            
            while True:
                self.bring_app_to_front(title)
                print("Ấn nút làm mới")
                kiemtra = self.timanh08(Loading)
                pyautogui.click(kiemtra)
                print("Kiểm tra đã ấn được chưa")
                kiemtraload = self.timanh05(Refresh)
                time.sleep(2)
                if kiemtraload:
                    print("Ấn OK")
                    break
                
            while True:
                print("Kiểm tra xem load xong chưa")
                kiemtra = self.timanh05(Refresh)
                time.sleep(3)
                if kiemtra is False:
                    print("Load Xong Roi")
                    break
            self.log.info("Load Dữ liệu xong")
            self.minimize_window(title)
            self.log.info("Đẩy dữ liệu vào PBI")
            self.PBIToSql()
            self.log.info("Đẩy thành công")
            self.force_kill_window(title)
            self.log.info("Tắt PBI")
            return True
        
        except Exception as e:
            self.log.info(f"có lỗi trong quá trình thực hiện {e}")
            return False
            
        
        
        
    
   

   

   

    
  