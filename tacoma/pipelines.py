from mysql.connector import pooling
from itemadapter import ItemAdapter

class TacomaPipeline:

    def open_spider(self, spider):
        # setup the pool once
        self.pool = pooling.MySQLConnectionPool(
            pool_name="tacoma_pool",
            pool_size=20,
            host="localhost",
            user="root",
            password="actowiz",
            database="scrapy_db"
        )
        
    # cration connetion pool once and resusing it 
        self.conn = self.pool.get_connection()
        self.cursor = self.conn.cursor()

    #because executemany takes lists 
        self.batch_links = []
        self.batch_products = []
        self.batch_size = 50  

    # ddl commands
        if spider.name == "spidy":
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tacoma_products2 (
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


# update function
    def update_status_done(self, url, tab="tacoma_products2"):
        if not url:
            return
        
        conn = self.pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            UPDATE {tab}
            SET status = 'done'
            WHERE url = %s
        """, (url,))

        conn.commit()
        cursor.close()
        conn.close()        


#processiong function : gettiong data from spider to insert
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        #for 2 spiders   
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
            self.update_status_done(adapter.get("url"))
            if len(self.batch_products) >= self.batch_size:
                self.insert_product_batch()      
        return item


#fetch function : fetcing urls for tacoma_products2
    def fetch_pending_urls(self, tab="tacoma_products2"):
        self.cursor.execute(f"SELECT name, url FROM {tab} WHERE status = 'pending'")
        return self.cursor.fetchall()


#insert function for products links
    def insert_links_batch(self):
        if not self.batch_links: return
        query = "INSERT IGNORE INTO tacoma_products2 (category, sub_category, name, url, status) VALUES (%s, %s, %s, %s, %s)"
        try:
            self.cursor.executemany(query, self.batch_links)
            self.conn.commit()
        except Exception as e:
            print(f"Insert Error: {e}")
        self.batch_links = []


#insert function for products
    def insert_product_batch(self):
        if not self.batch_products: return
        query = "INSERT INTO tacoma_product (name, p_id, description, img_url, price, shipping_weight, in_stock, specification) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.cursor.executemany(query, self.batch_products)
            self.conn.commit()
        except Exception as e:
            print(f"Insert Error: {e}")
        self.batch_products = []
        

#closing connetion and cursor objects
    def close_spider(self):
        # flush remaining batches
        if self.batch_links: self.insert_links_batch()
        if self.batch_products: self.insert_product_batch()
        #closing
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

