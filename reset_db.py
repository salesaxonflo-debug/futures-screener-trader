import sqlite3

conn = sqlite3.connect("paper_trading.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS account_state;")
cursor.execute("DROP TABLE IF EXISTS open_positions;")
cursor.execute("DROP TABLE IF EXISTS order_history;")
conn.commit()
conn.close()
print("Database cleared. The account balance will reset to $100.00 on the next start.")