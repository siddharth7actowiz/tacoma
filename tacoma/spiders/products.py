import scrapy
import json
from ..pipelines import TacomaPipeline
import re

class products_scrap(scrapy.Spider):
    name = "product"

    def start_requests(self):

        pipe = TacomaPipeline()
        pipe.open_spider(self)
        rows = pipe.fetch_pending_urls()

        if not rows:
            return

        for row in rows:
            url = row["url"].replace("https://www.tacomascrew.com","https://www.tacomascrew.com/api/v1/catalogpages?path=")

            yield scrapy.Request(
                url=url,
                callback=self.parse_api,  
                meta={
                    "name": row["name"],
                    "url": row["url"]   
                }
            )

    #get categoryId
    def parse_api(self, response):
        name = response.meta["name"]
        original_url = response.meta["url"]

        data = response.json()
        cat = data.get("category", {})
        cat_id = cat.get("id")

        if not cat_id:
            return

        # get product list first (IMPORTANT FIX)
        api = f"https://www.tacomascrew.com/api/v1/products/?categoryId={cat_id}&page=1&pageSize=5"

        yield scrapy.Request(
            url=api,
            callback=self.parse_product_list,
            meta={
                "name": name,
                "url": original_url
            }
        )

    # STEP 2: get product IDs
    def parse_product_list(self, response):
        name = response.meta["name"]
        original_url = response.meta["url"]

        data = response.json()
        cat_id=data.get("category",{}).get("id")
        products = data.get("products", [])

        for prod in products:
            p_id = prod.get("id")

            if not p_id:
                continue

            headers = {
                "accept": "application/json",
                "user-agent": "Mozilla/5.0"
            }

            # product detail API
            api = (
                f"https://www.tacomascrew.com/api/v1/products/{p_id}?addToRecentlyViewed=true&applyPersonalization=true&categoryId={cat_id}&expand=documents,specifications,styledproducts,htmlcontent,attributes,crosssells,pricing,relatedproducts,brand&getLastPurchase=true&includeAlternateInventory=true&includeAttributes=IncludeOnProduct,NotFromCategory&replaceProducts=false"
            )

            yield scrapy.Request(
                url=api,
                callback=self.parse_prods,
                headers=headers,
                meta={
                    "name": name,
                    "url": original_url
                }
            )

    # STEP 3: final data
    def parse_prods(self, response):
        data = response.json()

        product = data.get("product", {})

        desc = product.get("htmlContent")
        desc_list = [ item.strip() for item in re.split(r"<br\s*/?>|•", desc) if item.strip()]

        id=product.get("name")
        img=product.get("largeImagePath")
        price=product.get("basicListPrice")
        weight=product.get("shippingWeight")
        stock=product.get("availability").get("message")

        #specifications
        att_data=[]
        for att in product.get("attributeTypes",[]):
            temp={}
            temp["key"]=att.get("name")
            for val in att.get("attributeValues",[]):
                    temp["value"]=val.get("value")
                    att_data.append(temp)
                

        yield {
            "name": response.meta["name"],
            "p_id":id,
            "img_url":img,
            "price":price,
            "description": json.dumps(desc_list),
            "shipping_weight":weight,
            "in_stock":stock,
            "specification":json.dumps(att_data)
        }