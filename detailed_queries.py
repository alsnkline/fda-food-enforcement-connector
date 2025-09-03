#!/usr/bin/env python3
"""
Detailed queries to explore specific aspects of the FDA data
"""

import duckdb

def run_detailed_queries():
    """Run detailed queries on the FDA data"""
    
    conn = duckdb.connect('files/warehouse.db')
    
    print("🔍 Detailed FDA Data Analysis")
    print("=" * 50)
    
    # 1. Show all columns
    print("\n📋 All Columns in the Table:")
    columns = conn.execute("DESCRIBE tester.food_enforcement_records").fetchall()
    for i, col in enumerate(columns, 1):
        print(f"  {i:2d}. {col[0]} ({col[1]})")
    
    # 2. Geographic distribution
    print("\n🗺️  Geographic Distribution:")
    states = conn.execute("""
        SELECT state, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE state IS NOT NULL 
        GROUP BY state 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()
    for state, count in states:
        print(f"    {state}: {count} recalls")
    
    # 3. Product types
    print("\n🍎 Product Types:")
    products = conn.execute("""
        SELECT product_type, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE product_type IS NOT NULL 
        GROUP BY product_type 
        ORDER BY count DESC
    """).fetchall()
    for product_type, count in products:
        print(f"    {product_type}: {count}")
    
    # 4. Voluntary vs Mandated recalls
    print("\n⚖️  Voluntary vs Mandated Recalls:")
    voluntary = conn.execute("""
        SELECT voluntary_mandated, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE voluntary_mandated IS NOT NULL 
        GROUP BY voluntary_mandated 
        ORDER BY count DESC
    """).fetchall()
    for vol_type, count in voluntary:
        print(f"    {vol_type}: {count}")
    
    # 5. Year distribution
    print("\n📅 Recalls by Year (based on report_date):")
    years = conn.execute("""
        SELECT SUBSTR(report_date, 1, 4) as year, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE report_date IS NOT NULL 
        GROUP BY SUBSTR(report_date, 1, 4) 
        ORDER BY year DESC
    """).fetchall()
    for year, count in years:
        print(f"    {year}: {count} recalls")
    
    # 6. Sample of detailed product descriptions
    print("\n📝 Sample Product Descriptions:")
    descriptions = conn.execute("""
        SELECT recall_number, product_description, classification, reason_for_recall
        FROM tester.food_enforcement_records 
        WHERE product_description IS NOT NULL 
        ORDER BY report_date DESC 
        LIMIT 3
    """).fetchall()
    for recall in descriptions:
        print(f"\n  Recall: {recall[0]} ({recall[2]})")
        print(f"  Product: {recall[1][:100]}...")
        print(f"  Reason: {recall[3][:100]}...")
    
    # 7. Check for any nested data that might not have been flattened
    print("\n🔍 Checking for potential nested data:")
    sample_record = conn.execute("SELECT * FROM tester.food_enforcement_records LIMIT 1").fetchone()
    columns = [desc[0] for desc in conn.description]
    
    for col, val in zip(columns, sample_record):
        if val and isinstance(val, str) and (val.startswith('[') or val.startswith('{')):
            print(f"    {col}: {val[:100]}...")
    
    conn.close()
    print("\n✅ Detailed analysis complete!")

if __name__ == "__main__":
    run_detailed_queries()
