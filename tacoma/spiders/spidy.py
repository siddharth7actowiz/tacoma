import scrapy


class tacomaspider(scrapy.Spider):
    name = "spidy"

    start_urls = [
        "https://www.tacomascrew.com/all-categories"
    ]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    def __init__(self, *args, **kwargs):
        super(tacomaspider, self).__init__(*args, **kwargs)
        self.seen_products = set()
        self.visited_paths = set()

   
    def parse(self, response):
        categories = response.xpath("//div[contains(@class,'category-card')]//a/@href").getall()

        for cat in categories:
            yield scrapy.Request(
                f"https://www.tacomascrew.com/api/v1/catalogpages?path={cat}",
                headers=self.headers,
                callback=self.parse_category,
                meta={
                    "main_category": cat.split("/")[-1],
                    "all_sub_cat": [],
                    "current_path": cat
                }
            )

   
    def parse_category(self, response):
        data = response.json()
        category = data.get("category", {})

        current_path = response.meta["current_path"]

        
        if current_path in self.visited_paths:
            return
        self.visited_paths.add(current_path)

        category_id = category.get("id")
        name = (category.get("shortDescription") or "").strip()

        main_category = response.meta["main_category"]
        all_sub_cat = response.meta["all_sub_cat"] + [name] if name else response.meta["all_sub_cat"]

       
        for sub in category.get("subCategories", []):
            sub_path = sub.get("path")

            if sub_path:
                yield scrapy.Request(
                    f"https://www.tacomascrew.com/api/v1/catalogpages?path={sub_path}",
                    headers=self.headers,
                    callback=self.parse_category,
                    meta={
                        "main_category": main_category,
                        "all_sub_cat": all_sub_cat,
                        "current_path": sub_path
                    }
                )

       
        if category_id:
            yield scrapy.Request(
                self.build_product_api(category_id, 1),
                headers=self.headers,
                callback=self.parse_product,
                meta={
                    "main_category": main_category,
                    "all_sub_cat": all_sub_cat,
                    "category_id": category_id,
                    "page": 1
                }
            )

   
    def build_product_api(self, category_id, page):
        return (
            "https://www.tacomascrew.com/api/v1/products/"
            f"?categoryId={category_id}&page={page}&pageSize=96"
        )

   
    def parse_product(self, response):
        data = response.json()

        main_category = response.meta["main_category"]
        all_sub_cat = response.meta["all_sub_cat"]
        category_id = response.meta["category_id"]
        page = response.meta["page"]

        for prod in data.get("products", []):
            url = prod.get("productDetailUrl")

            if not url:
                continue

            full_url = response.urljoin(url)

            if full_url in self.seen_products:
                continue
            self.seen_products.add(full_url)

            # joining sub cats with > for better read
            sub_cat = " > ".join([x for x in all_sub_cat if x])
            yield {
                "category": main_category,
                "sub_category":sub_cat if sub_cat else main_category,
                "name": (prod.get("shortDescription") or "").strip(),
                
                "prod_url": full_url,
                "status":"pending"
            }

        pagination = data.get("pagination", {})

        if pagination.get("currentPage", page) < pagination.get("numberOfPages", 1):
            next_page = page + 1

            yield scrapy.Request(
                self.build_product_api(category_id, next_page),
                headers=self.headers,
                callback=self.parse_product,
                meta={
                    "main_category": main_category,
                    "all_sub_cat": all_sub_cat,
                    "category_id": category_id,
                    "page": next_page
                }
            )