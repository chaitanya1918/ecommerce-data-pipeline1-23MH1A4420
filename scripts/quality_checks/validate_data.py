
def check_null_values(connection, schema):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) FROM {schema}.customers
        WHERE customer_id IS NULL;
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

def check_duplicates(connection, schema):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT customer_id, COUNT(*)
            FROM {schema}.customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) t;
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

def check_referential_integrity(connection, schema):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {schema}.transactions t
        LEFT JOIN {schema}.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL;
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

def check_data_ranges(connection, schema):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {schema}.products
        WHERE selling_price <= 0 OR cost_price <= 0;
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result

def calculate_quality_score(results):
    penalties = 0
    penalties += results["nulls"] * 5
    penalties += results["duplicates"] * 5
    penalties += results["referential_issues"] * 10
    penalties += results["range_issues"] * 5

    score = max(0, 100 - penalties)
    return score