from flask import Flask, render_template, request, send_file
import mysql.connector
import pandas as pd
from datetime import datetime
import os

app = Flask(__name__)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'YOUR PASSWORD', 
    'database': 'real_estate'
}

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT COUNT(*) AS total FROM Properties")
    total_properties = cursor.fetchone()['total']

    cursor.execute("SELECT COUNT(*) AS total FROM Transactions")
    total_transactions = cursor.fetchone()['total']

    cursor.execute("SELECT AVG(sale_price) AS avg_price FROM Transactions")
    avg_sale_price = round(cursor.fetchone()['avg_price'], 2)

    cursor.close()
    conn.close()

    return render_template('index.html', 
        total_properties=total_properties,
        total_transactions=total_transactions,
        avg_sale_price=avg_sale_price
    )

@app.route('/properties')
def properties():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT p.*, a.name AS agent_name 
        FROM Properties p
        JOIN Agents a ON p.agent_id = a.agent_id
    """)
    props = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('properties.html', properties=props)

@app.route('/reports')
def reports():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT city, state, AVG(price) AS avg_price, COUNT(*) AS count
        FROM Properties GROUP BY city, state
    """)
    avg_prices = cursor.fetchall()

    cursor.execute("SELECT * FROM HighDemandAreas")
    high_demand = cursor.fetchall()

    cursor.execute("""
        SELECT p.city, t.sale_date, t.sale_price,
        AVG(t.sale_price) OVER (PARTITION BY p.city ORDER BY t.sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
        FROM Transactions t
        JOIN Properties p ON t.property_id = p.property_id
        ORDER BY p.city, t.sale_date
    """)
    trends = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template('reports.html', 
        avg_prices=avg_prices, 
        high_demand=high_demand, 
        trends=trends
    )

@app.route('/export/<report>')
def export_csv(report):
    conn = get_db_connection()
    filename = f"exports/{report}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs('exports', exist_ok=True)

    queries = {
        'properties': "SELECT * FROM Properties",
        'transactions': "SELECT * FROM Transactions",
        'avg_prices': """
            SELECT city, state, AVG(price) AS avg_price, COUNT(*) AS count
            FROM Properties GROUP BY city, state
        """,
        'trends': """
            SELECT p.city, t.sale_date, t.sale_price,
            AVG(t.sale_price) OVER (PARTITION BY p.city ORDER BY t.sale_date 
                            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
            FROM Transactions t JOIN Properties p ON t.property_id = p.property_id
        """,
        'high_demand': "SELECT * FROM HighDemandAreas"
    }

    if report not in queries:
        return "Report not found", 400

    df = pd.read_sql(queries[report], conn)
    df.to_csv(filename, index=False)
    conn.close()

    return send_file(filename, as_attachment=True)

@app.route('/export')
def export():
    return render_template('export.html')

if __name__ == '__main__':

    app.run(debug=True)

