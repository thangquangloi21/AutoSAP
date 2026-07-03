from WorkThread import WorkThread
import re
from pyadomd import Pyadomd
import pandas as pd
import os

def strip_table_prefix(colname: str) -> str:
    """Nếu dạng "SOMETHING[NAME]" thì lấy NAME"""
    m = re.match(r'^.*\[(.+?)\]$', colname)
    return m.group(1) if m else colname

def test_query_pbi_data():
    """Test query dữ liệu từ Power BI"""
    try:
        wt = WorkThread()
        
        # Tìm port của Power BI
        port = os.environ.get("PBIPORT") or wt.find_powerbi_port() or "51328"
        print(f"🔗 Sử dụng port: {port}")
        
        conn_str = f"Data Source=localhost:{port}"
        
        with Pyadomd(conn_str) as conn:
            # 1) Liệt kê các bảng trong model
            print("\n📋 === Liệt kê các bảng trong model ===")
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
            print(tables_df.to_string(index=False))
            
            # 2) Query dữ liệu từ từng bảng
            print("\n\n📊 === Query dữ liệu từ các bảng ===")
            
            for table_name in tables_df['TABLE_NAME']:
                if table_name == 'Measures':
                    continue
                
                print(f"\n🔹 Bảng: {table_name}")
                print("-" * 50)
                
                try:
                    # Query dữ liệu từ bảng PBI
                    dax_query = f"EVALUATE ({table_name})"
                    with conn.cursor().execute(dax_query) as cur:
                        cols = [c[0] for c in cur.description]
                        rows = [list(r) for r in cur.fetchall()]
                        clean = [strip_table_prefix(s) for s in cols]
                    
                    if not rows:
                        print(f"  ⚠️  Bảng trống, bỏ qua.")
                        continue
                    
                    df = pd.DataFrame(rows, columns=clean)
                    print(f"  ✅ Tổng {len(df)} hàng, {len(df.columns)} cột")
                    print(f"\n  Các cột: {', '.join(df.columns.tolist())}")
                    print(f"\n  Dữ liệu mẫu (5 hàng đầu):")
                    print(df.head().to_string(index=False))
                    
                except Exception as e:
                    print(f"  ❌ Lỗi: {e}")
            
            print("\n\n✅ Hoàn tất test query dữ liệu!")
            
    except Exception as e:
        print(f"❌ Lỗi kết nối Power BI: {e}")
        print("💡 Hãy kiểm tra:")
        print("  - Power BI Desktop có mở không?")
        print("  - Port đúng không?")

if __name__ == "__main__":
    test_query_pbi_data()