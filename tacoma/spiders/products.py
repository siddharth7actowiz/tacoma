import scrapy
import json
import re
import gzip
import os
import mysql.connector

class products_scrap(scrapy.Spider):
    name = "product2"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.tacomascrew.com",
        "Referer": "https://www.tacomascrew.com/"
    }

    # ---------------- START REQUEST ----------------
    def start_requests(self):
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="tacoma"
        )

        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT name, url
            FROM tacoma_products_urls
            WHERE status = 'pending'
        """)

        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        for row in rows:
            url = row["url"].replace(
                "https://www.tacomascrew.com",
                "https://www.tacomascrew.com/api/v1/catalogpages?path="
            )

            yield scrapy.Request(
                url=url,
                callback=self.parse_api,
                meta={
                    "name": row["name"],
                    "url": row["url"]
                }
            )

    # ---------------- CATEGORY ----------------
    def parse_api(self, response):
        data = response.json()

        cat_id = data.get("category", {}).get("id")
        if not cat_id:
            return

        yield scrapy.Request(
            url=f"https://www.tacomascrew.com/api/v1/products/?categoryId={cat_id}&page=1&pageSize=5",
            callback=self.parse_product_list,
            meta=response.meta
        )

    # ---------------- PRODUCT LIST ----------------
    def parse_product_list(self, response):
        data = response.json()
        cat_id = data.get("category", {}).get("id")

        for prod in data.get("products", []):
            p_id = prod.get("id")
            if not p_id:
                continue

            api = (
                f"https://www.tacomascrew.com/api/v1/products/{p_id}"
                f"?addToRecentlyViewed=true&applyPersonalization=true"
                f"&categoryId={cat_id}&expand=documents,specifications,styledproducts,htmlcontent,attributes"
            )

            yield scrapy.Request(
                url=api,
                headers=self.headers,
                callback=self.parse_prods,
                meta={
                    **response.meta,
                    "product_id": p_id
                }
            )

    # ---------------- PRODUCT DETAILS ----------------
    def parse_prods(self, response):
        data = response.json()
        product = data.get("product", {})

        product_id = response.meta["product_id"]

        # SAVE PRODUCT JSON
        folder = r"D:\Scrapy\Tacoma\tacoma\tacoma\product_pages"
        os.makedirs(folder, exist_ok=True)

        with open(f"{folder}\\{product_id}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        # description
        desc = product.get("htmlContent") or ""
        desc_list = [x.strip() for x in re.split(r"<br\s*/?>|•", desc) if x.strip()]

        # specifications
        att_data = []
        for att in product.get("attributeTypes", []):
            key = att.get("name")
            for val in att.get("attributeValues", []):
                att_data.append({
                    "key": key,
                    "value": val.get("value")
                })

        item = {
            "name": response.meta["name"],
            "p_id": product.get("name"),
            "url": response.meta["url"],   # IMPORTANT FIX
            "img_url": product.get("largeImagePath"),
            "description": json.dumps(desc_list),
            "shipping_weight": product.get("shippingWeight"),
            "specification": json.dumps(att_data),
        }

        # ---------------- INVENTORY ----------------
        yield scrapy.Request(
            url="https://www.tacomascrew.com/api/v1/realtimeinventory",
            method="POST",
            headers=self.headers,
            body=json.dumps({"productIds": [product_id]}),
            callback=self.parse_inventory,
            meta={
                **response.meta,
                "item": item,
                "product_id": product_id
            }
        )

    # ---------------- INVENTORY ----------------
    def parse_inventory(self, response):
        data = response.json()
        product_id = response.meta["product_id"]

        #  SAVE INVENTORY JSON.GZ
        folder = r"D:\Scrapy\Tacoma\tacoma\tacoma\inventory_pages"
        os.makedirs(folder, exist_ok=True)

        with gzip.open(f"{folder}\\{product_id}_inventory.json.gz", "wt", encoding="utf-8") as f:
            json.dump(data, f)

        

        inventory = response.json().get("realTimeInventoryResults", [{}])[0]
        available=inventory.get("inventoryAvailabilityDtos",[])[0]
        stock_qty = available.get("availability").get("message")

        # ---------------- PRICE ----------------
        yield scrapy.Request(
            url="https://www.tacomascrew.com/api/v1/realtimepricing",
            method="POST",
            headers=self.headers,
            body=json.dumps({
                "productPriceParameters": [{
                    "productId": product_id,
                    "unitOfMeasure": "EA",
                    "qtyOrdered": 1
                }]
            }),
            callback=self.parse_price,
            meta={
                **response.meta,
                "stock_qty": stock_qty,
                
            }
        )

    # ---------------- PRICE ----------------
    def parse_price(self, response):
        data = response.json()
        product_id = response.meta["product_id"]

        #  SAVE PRICE JSON.GZ
        folder = r"D:\Scrapy\Tacoma\tacoma\tacoma\price_pages"
        os.makedirs(folder, exist_ok=True)

        with gzip.open(f"{folder}\\{product_id}_price.json.gz", "wt", encoding="utf-8") as f:
            json.dump(data, f)

        price = None
        price_value = None

        results = data.get("realTimePricingResults", [])
        if results:
            r = results[0]
            price = r.get("extendedActualPriceDisplay")
            price_value = r.get("extendedActualPrice")

        item = response.meta["item"]

        item.update({
            "price": price,
            "price_value": price_value,
            "stock_qty": response.meta["stock_qty"],
            
        })

        yield item
