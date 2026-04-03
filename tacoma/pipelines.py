# from mysql.connector import pooling
# from itemadapter import ItemAdapter


# class TacomaPipeline:

#     def open_spider(self, spider):
#         self.pool = pooling.MySQLConnectionPool(
#             pool_name="tacoma_pool",
#             pool_size=10,
#             host="localhost",
#             user="root",
#             password="actowiz",
#             database="scrapy_db"
#         )

#         # separate batches
#         self.batch_links = []
#         self.batch_products = []

#         self.batch_size = 100

#         conn = self.pool.get_connection()
#         cursor = conn.cursor()

#         if spider.name == "spidy":
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS tacoma_products_links (
#                     id INT AUTO_INCREMENT PRIMARY KEY,
#                     category VARCHAR(255),
#                     sub_category TEXT,
#                     name TEXT,
#                     url VARCHAR(500) UNIQUE,
#                     status VARCHAR(20)
#                 )
#             """)
#             conn.commit()

#         elif spider.name == "product":
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS tacoma_product (
#                     id INT AUTO_INCREMENT PRIMARY KEY,
#                     name VARCHAR(255),
#                     p_id VARCHAR(255),
#                     description TEXT,
#                     img_url TEXT,
#                     price DECIMAL(10,2),
#                     shipping_weight VARCHAR(250),
#                     in_stock VARCHAR(250),
#                     specification TEXT
#                 )
#             """)
#             conn.commit()

#         cursor.close()
#         conn.close()

#     def process_item(self, item, spider):
#         adapter = ItemAdapter(item)

#         #  Spider 1 (links)
#         if spider.name == "spidy":
#             url = adapter.get("prod_url")
#             if not url:
#                 return item

#             self.batch_links.append((
#                 adapter.get("category"),
#                 adapter.get("sub_category"),
#                 adapter.get("name"),
#                 url,
#                 "pending"
#             ))

#             if len(self.batch_links) >= self.batch_size:
#                 self.insert_links_batch()

#         #  Spider 2 (product details)
#         elif spider.name == "product":
#             self.batch_products.append((
#                 adapter.get("name"),
#                 adapter.get("p_id"),
#                 adapter.get("description"),
#                 adapter.get("img_url"),
#                 adapter.get("price"),
#                 adapter.get("shipping_weight"),
#                 adapter.get("in_stock"),
#                 adapter.get("specification")
#             ))

#             # update status immediately
#             self.update_status_done(adapter.get("url"))

#             if len(self.batch_products) >= self.batch_size:
#                 self.insert_product_batch()

#         return item

#     #  Insert links
#     def insert_links_batch(self):
#         conn = self.pool.get_connection()
#         cursor = conn.cursor()

#         query = """
#             INSERT IGNORE INTO tacoma_products_links 
#             (category, sub_category, name, url, status)
#             VALUES (%s, %s, %s, %s, %s)
#         """

#         cursor.executemany(query, self.batch_links)
#         conn.commit()

#         cursor.close()
#         conn.close()
#         self.batch_links = []

#     #  Insert product details (THIS WAS MISSING)~
#     def insert_product_batch(self):
#         conn = self.pool.get_connection()
#         cursor = conn.cursor()

#         query = """
#             INSERT INTO tacoma_product 
#             (name, p_id, description, img_url, price, shipping_weight, in_stock, specification)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """

#         try:
#             cursor.executemany(query, self.batch_products)
#             conn.commit()
#         except Exception as e:
#             print("PRODUCT INSERT ERROR:", e)

#         cursor.close()
#         conn.close()
#         self.batch_products = []

#     #  Fetch
#     def fetch_pending_urls(self, tab="tacoma_products_links"):
#         conn = self.pool.get_connection()
#         cursor = conn.cursor(dictionary=True)

#         cursor.execute(f"""
#             SELECT name, url 
#             FROM {tab}
#             WHERE status = 'pending'
#             ;
           
#         """)

#         rows = cursor.fetchall()
#         cursor.close()
#         conn.close()

#         return rows

#     #  Update
#     def update_status_done(self, url, tab="tacoma_products_links"):
#         if not url:
#             return

#         conn = self.pool.get_connection()
#         cursor = conn.cursor()

#         cursor.execute(f"""
#             UPDATE {tab}
#             SET status = 'done'
#             WHERE url = %s
#         """, (url,))

#         conn.commit()

#         cursor.close()
#         conn.close()

#     def close_spider(self, spider):
#         if self.batch_links:
#             self.insert_links_batch()

#         if self.batch_products:
#             self.insert_product_batch()


from mysql.connector import pooling
from itemadapter import ItemAdapter

class TacomaPipeline:

    def open_spider(self, spider):
        # 1. Setup the pool once
        self.pool = pooling.MySQLConnectionPool(
            pool_name="tacoma_pool",
            pool_size=5,
            host="localhost",
            user="root",
            password="actowiz",
            database="scrapy_db"
        )
        
        # 2. CREATE AND KEEP ONE PERSISTENT CONNECTION
        self.conn = self.pool.get_connection()
        self.cursor = self.conn.cursor(dictionary=True if spider.name == "product" else False)

        self.batch_links = []
        self.batch_products = []
        self.batch_size = 500  

        # Table creation
        if spider.name == "spidy":
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tacoma_products_links (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    category VARCHAR(255),
                    sub_category TEXT,
                    name TEXT,
                    url VARCHAR(500) UNIQUE,
                    status VARCHAR(20) DEFAULT 'pending'
                )
            """)
            self.conn.commit()

        elif spider.name == "product":
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tacoma_product (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    p_id VARCHAR(255),
                    description TEXT,
                    img_url TEXT,
                    price DECIMAL(10,2),
                    shipping_weight VARCHAR(250),
                    in_stock VARCHAR(250),
                    specification TEXT
                )
            """)
            self.conn.commit()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if spider.name == "spidy":
            url = adapter.get("prod_url")
            if url:
                self.batch_links.append((
                    adapter.get("category"), adapter.get("sub_category"),
                    adapter.get("name"), url, "pending"
                ))
                if len(self.batch_links) >= self.batch_size:
                    self.insert_links_batch()

        elif spider.name == "product":
            self.batch_products.append((
                adapter.get("name"), adapter.get("p_id"),
                adapter.get("description"), adapter.get("img_url"),
                adapter.get("price"), adapter.get("shipping_weight"),
                adapter.get("in_stock"), adapter.get("specification")
            ))
            if len(self.batch_products) >= self.batch_size:
                self.insert_product_batch()

        return item

    def fetch_pending_urls(self, tab="tacoma_products_links"):
        # Uses the existing persistent cursor
        self.cursor.execute(f"SELECT name, url FROM {tab} WHERE status = 'pending'")
        return self.cursor.fetchall()

    def insert_links_batch(self):
        if not self.batch_links: return
        query = "INSERT IGNORE INTO tacoma_products_links (category, sub_category, name, url, status) VALUES (%s, %s, %s, %s, %s)"
        try:
            self.cursor.executemany(query, self.batch_links)
            self.conn.commit()
        except Exception as e:
            print(f"Insert Error: {e}")
        self.batch_links = []

    def insert_product_batch(self):
        if not self.batch_products: return
        query = "INSERT INTO tacoma_product (name, p_id, description, img_url, price, shipping_weight, in_stock, specification) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.cursor.executemany(query, self.batch_products)
            self.conn.commit()
        except Exception as e:
            print(f"Insert Error: {e}")
        self.batch_products = []

    def close_spider(self, spider):
        # 1. Flush remaining batches
        if self.batch_links: self.insert_links_batch()
        if self.batch_products: self.insert_product_batch()
        
        # 2. CLOSE EVERYTHING ONCE AT THE END
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
