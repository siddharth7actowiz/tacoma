from mysql.connector import pooling, Error


class TacomaPipeline:

    def __init__(self):
        self.pool = pooling.MySQLConnectionPool(
            pool_name="tacoma_pool",
            pool_size=10,
            host="localhost",
            user="root",
            password="actowiz",
            database="tacoma"
        )

    # -----------------------------
    # OPEN SPIDER
    # -----------------------------
    def open_spider(self, spider):
        self.conn = self.pool.get_connection()
        self.cursor = self.conn.cursor()
        self.create_table()

    # -----------------------------
    # CLOSE SPIDER
    # -----------------------------
    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    # -----------------------------
    # CREATE TABLES
    # -----------------------------
    def create_table(self):
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tacoma_products_urls (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    category VARCHAR(255),
                    sub_category TEXT,
                    name TEXT,
                    url VARCHAR(500) UNIQUE,
                    status VARCHAR(20) DEFAULT 'pending'
                )
            """)

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    p_id VARCHAR(255),
                    url TEXT,
                    description TEXT,
                    img_url TEXT,
                    price VARCHAR(50),
                    shipping_weight VARCHAR(250),
                    in_stock VARCHAR(50),
                    specification TEXT
                )
            """)

            self.conn.commit()

        except Error as e:
            print(f"Table creation error: {e}")
            self.conn.rollback()

    # -----------------------------
    # PROCESS ITEM
    # -----------------------------
    def process_item(self, item, spider):

        if spider.name == "product2":
            try:
                self.insert_product(item)

                if item.get("url"):
                    self.update_status(item["url"])

            except Error as e:
                print(f"Process item error: {e}")
                self.conn.rollback()

        return item

    # -----------------------------
    # INSERT PRODUCT
    # -----------------------------
    def insert_product(self, item):
        self.cursor.execute("""
            INSERT INTO product
            (name, p_id, url, description, img_url, price, shipping_weight, in_stock, specification)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            item.get("name"),
            item.get("p_id"),
            item.get("url"),
            item.get("description"),
            item.get("img_url"),
            item.get("price"),
            item.get("shipping_weight"),
            item.get("stock_qty"),
            item.get("specification")
        ))

        self.conn.commit()

    # -----------------------------
    # UPDATE STATUS
    # -----------------------------
    def update_status(self, url):
        self.cursor.execute("""
            UPDATE tacoma_products_urls
            SET status = 'done'
            WHERE url = %s
        """, (url,))

        self.conn.commit()
