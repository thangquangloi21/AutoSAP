# class xử lý công việc trong thread riêng
import re
import subprocess
import sys
import threading
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple, Union

import pandas as pd  # type: ignore[reportMissingModuleSource]
import pyodbc  # type: ignore[reportMissingImports]

from Log import Logger

APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
RESOURCE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))

SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


@dataclass(frozen=True)
class ExportPaths:
    vbs_script: Path
    export_folder: Path
    excel_file: Path
    csv_file: Path


@dataclass(frozen=True)
class SqlConfig:
    driver: str
    server: str
    database: str
    table: str
    staging_table: str
    username: str
    password: str
    trusted_connection: bool
    bcp_path: str
    first_row: str
    code_page: str




class WorkThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.log = Logger(log_dir="Logs")

        # self.log.info("Application initialized")

    

    def Check_Status(self, SYSTEM):
        self.log.info(f"Check trạng thái {SYSTEM}")
        try:
            conn = pyodbc.connect(
                "DRIVER={SQL Server};"
                "SERVER=10.239.1.54;"
                "DATABASE=ACWO;"
                "UID=sa;"
                "PWD=123456;"
            )
            cursor = conn.cursor()

            cursor.execute("""
                SELECT TOP 1 [ID], [SYSTEM], [TIME]
                FROM [ACWO].[dbo].[DataStatus]
                WHERE [SYSTEM] = ?
                ORDER BY [ID] DESC
            """, SYSTEM)

            row = cursor.fetchone()
            conn.close()

            if row:
                self.log.info(f"Dữ liệu gần nhất: ID={row[0]}, SYSTEM={row[1]}, TIME={row[2]}")
                print(f"Dữ liệu: {row[2]}")
                return row[2]

            self.log.info("Không có dữ liệu.")
            print("Không có dữ liệu.")
            return "error"

        except Exception as e:
            self.log.error(f"error: {e}")
            return "error"

    
    
    def resolve_path(self ,path_value: Union[Path, str], resource: bool = False) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        if resource:
            app_path = APP_DIR / path
            if app_path.exists():
                return app_path
            return RESOURCE_DIR / path
        return APP_DIR / path


    def get_config(self) -> Tuple[ExportPaths, SqlConfig]:
        export_paths = ExportPaths(
            vbs_script=self.resolve_path("Script/ZPPI189.vbs", resource=True),
            export_folder=self.resolve_path("Data"),
            excel_file=self.resolve_path("Data") / "INT189.XLSX",
            csv_file=self.resolve_path("Data/INT189.csv"),
        )
        
        sql_config = SqlConfig(
            driver="SQL Server",
            server="10.239.1.54",
            database="SAPData",
            table="ZPPI189",
            staging_table="ZPPI189_new",
            username="sa",
            password="123456",
            trusted_connection=False,
            bcp_path="bcp",
            first_row="2",
            code_page="65001",
        )
        
        return export_paths, sql_config


    def setup_logging(self):
        self.log = Logger(log_dir="Logs")
        self.log.info("Log file: %s", self.log.log_path)
        return self.log.log_path


    def log_subprocess_output(self, name: str, result: subprocess.CompletedProcess[str]) -> None:
        self.log.info(f"{name} return code: {result.returncode}")
        if result.stdout:
            self.log.info(f"{name} stdout:\n{result.stdout.strip()}")
        if result.stderr:
            self.log.warning(f"{name} stderr:\n{result.stderr.strip()}")

    def _is_vbs_failure(self, result: subprocess.CompletedProcess[str]) -> bool:
        if result.returncode != 0:
            return True

        output_text = "\n".join(filter(None, [result.stdout, result.stderr])).lower()
        failure_markers = [
            "could not be found by id",
            "control could not be found",
            "the control could not be found",
        ]
        return any(marker in output_text for marker in failure_markers)

    def run_sap_export(self, vbs_script: Path) -> None:
        self.log.info("Run SAP export script")
        vbs_result = subprocess.run(["cscript", "//NoLogo", str(vbs_script)], capture_output=True, text=True)
        self.log_subprocess_output(f"cscript {vbs_script.name}", vbs_result)
        if self._is_vbs_failure(vbs_result):
            combined_output = "\n".join(filter(None, [vbs_result.stdout.strip(), vbs_result.stderr.strip()]))
            raise RuntimeError(f"VBS script failed: {combined_output or 'unknown error'}")


    def validate_excel_file(self, excel_file: Path, started_at: datetime) -> None:
        if not excel_file.exists():
            raise FileNotFoundError(f"Excel export file does not exist: {excel_file}")
        if excel_file.stat().st_size == 0:
            raise RuntimeError(f"Excel export file is empty: {excel_file}")
        modified_at = datetime.fromtimestamp(excel_file.stat().st_mtime)
        if modified_at < started_at:
            raise RuntimeError(f"Excel export file was not refreshed in this run: {excel_file}")
        self.log.info(f"Excel file size: {excel_file.stat().st_size} bytes")



    def convert_excel_to_csv(self, input_file: Path, output_file: Path) -> int:
        self.log.info("Read Excel file")

        df =  pd.read_excel(
            input_file,
            engine="openpyxl",
            dtype=str,
            keep_default_na=False
            
        )
        

        self.log.info(f"Excel rows: {len(df)}")

        if df.empty:
            raise RuntimeError("Excel export contains no data rows")

        self.log.info("Write CSV file")

        output_file.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(
            output_file,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        self.log.info(f"Converted: {input_file} -> {output_file}")

        return len(df)


    def build_connection_string(self, sql_config: SqlConfig) -> str:
        parts = [
            f"DRIVER={{{sql_config.driver}}}",
            f"SERVER={sql_config.server}",
            f"DATABASE={sql_config.database}",
        ]
        if sql_config.trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.extend([f"UID={sql_config.username}", f"PWD={sql_config.password}"])
        return ";".join(parts)


    def truncate_table(self, sql_config: SqlConfig) -> None:
        self.log.info(f"Connect SQL and truncate {sql_config.table}")
        conn: Any = pyodbc.connect(self.build_connection_string(sql_config))  # type: ignore[reportUnknownMemberType]
        conn.execute(f"TRUNCATE TABLE {sql_config.table}")  # type: ignore[reportUnknownMemberType]
        conn.commit()  # type: ignore[reportUnknownMemberType]
        conn.close()  # type: ignore[reportUnknownMemberType]
        self.log.info(f"Truncated table {sql_config.table}")


    def update_status(self, sql_config: SqlConfig) -> None:
        self.log.info("Cập nhật thời gian làm mới thành công")
        conn: Any = pyodbc.connect(self.build_connection_string(sql_config))  # type: ignore[reportUnknownMemberType]
        conn.execute("INSERT INTO ACWO.dbo.DataStatus ([SYSTEM], [TIME]) VALUES ( 'INT189', FORMAT(GETDATE(), 'HH:mm dd-MM-yyyy'))")  # type: ignore[reportUnknownMemberType]
        conn.commit()  # type: ignore[reportUnknownMemberType]
        conn.close()  # type: ignore[reportUnknownMemberType]
        self.log.info("Cập nhật thời gian thành công")



    def del_Data(self, sql_config: SqlConfig) -> None:
        self.log.info("Deleting data...")

        conn = pyodbc.connect(self.build_connection_string(sql_config))

        try:
            conn.execute("""
                DELETE A
                FROM [SAPData].[dbo].[ZPPI189] A
                INNER JOIN (
                    SELECT DISTINCT
                        [Work Order],
                        [MES Component]
                    FROM [SAPData].[dbo].[ZPPI189_new]
                ) B
                    ON A.[Work Order] = B.[Work Order]
                AND A.[MES Component] = B.[MES Component]
            """)
            conn.commit()
            self.log.info("Xóa dữ liệu thành công")
        except Exception as e:
            conn.rollback()
            self.log.error(f"Lỗi: {e}")
            raise
        finally:
            conn.close()

    def run_bcp_import(self, sql_config: SqlConfig, csv_file: Path, table: str) -> None:
        self.log.info(f"Run BCP import to {table}")

        # Validate CSV file exists
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        if "." in table:
            destination = f"{sql_config.database}.{table}"
        else:
            destination = f"{sql_config.database}..{table}"

        csv_path = str(csv_file.resolve())  # Absolute path

        command = [
            sql_config.bcp_path,
            destination,
            "in",
            f'"{csv_path}"',  # Quote the path
            "-c",
            "-C",
            sql_config.code_page,
            "-t;",
            r"-r\n",
            "-F",
            sql_config.first_row,
            "-S",
            sql_config.server,
        ]
        if sql_config.trusted_connection:
            command.append("-T")
        else:
            command.extend(["-U", sql_config.username, "-P", sql_config.password])

        self.log.info(f"BCP command: {' '.join(command)}")
        result: subprocess.CompletedProcess[Any] = subprocess.run(" ".join(command), capture_output=True, text=True, shell=True)
        self.log_subprocess_output("bcp import", result)
        if result.returncode != 0:
            raise RuntimeError("BCP import failed")


    def replace_target_from_staging(self, sql_config: SqlConfig) -> None:
        self.log.info(f"Replace {sql_config.table} from staging table {sql_config.staging_table}")
        conn: Any = pyodbc.connect(self.build_connection_string(sql_config))  # type: ignore[reportUnknownMemberType]
        try:
            conn.autocommit = False  # type: ignore[reportUnknownMemberType]
            # conn.execute(f"TRUNCATE TABLE {sql_config.table}")  # type: ignore[reportUnknownMemberType]
            conn.execute(f"INSERT INTO {sql_config.table} SELECT * FROM {sql_config.staging_table}")  # type: ignore[reportUnknownMemberType]
            conn.commit()  # type: ignore[reportUnknownMemberType]
        except Exception:
            conn.rollback()  # type: ignore[reportUnknownMemberType]
            raise
        finally:
            conn.close()  # type: ignore[reportUnknownMemberType]
        self.log.info("Replaced target table from staging")


    def import_csv_to_sql(self ,sql_config: SqlConfig, csv_file: Path) -> None:
        # Nếu có staging_table thì vào đây
        if sql_config.staging_table:
            # xóa Data trong bang phu DB 
            self.truncate_table(replace(sql_config, table=sql_config.staging_table))
            # insert vao bang phu
            self.run_bcp_import(sql_config, csv_file, sql_config.staging_table)
            # xoa du lieu trong bang chinh key wo-mescomp
            self.del_Data(sql_config)
            self.replace_target_from_staging(sql_config)
            return
        # # Nếu không có bảng phụ thì insert vào luôn
        # truncate_table(sql_config)
        # run_bcp_import(sql_config, csv_file, sql_config.table)


    def ZPPI189(self):
        try:
            log_file = self.setup_logging()
            export_paths, sql_config = self.get_config()
            self.log.info("Start XuatDuLieuSAP")
            self.log.info(f"VBS script: {export_paths.vbs_script}")
            self.log.info(f"Excel input: {export_paths.excel_file}")
            self.log.info(f"CSV output: {export_paths.csv_file}")
            self.log.info(f"SQL target: {sql_config.database}.{sql_config.table}")

            started_at = datetime.now()
            self.run_sap_export(export_paths.vbs_script)
            self.validate_excel_file(export_paths.excel_file, started_at)
            self.convert_excel_to_csv(export_paths.excel_file, export_paths.csv_file)
            self.import_csv_to_sql(sql_config, export_paths.csv_file)
            self.update_status(sql_config)
            self.log.info("Finished XuatDuLieuSAP successfully")
            return True
        except Exception as e:
            return False
        finally:
            return True



