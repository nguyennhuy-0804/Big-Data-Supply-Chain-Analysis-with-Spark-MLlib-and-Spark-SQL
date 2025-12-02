from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder \
    .appName("SupplyChainAnalysis") \
    .getOrCreate()
df = spark.read.csv(r"C:/Users/nny08/PycharmProjects/PythonProject2/Data Source/Dataclean.csv", header=True,
                    inferSchema=True)
df.printSchema()

df = df.withColumn("Order Date", to_date("Order Date (DateOrders)", "M/d/yyyy H:mm"))

customers = df.select(
    "Customer Id", "Customer Fname", "Customer Lname", "Customer Email",
    "Customer Segment", "Customer City", "Customer Country", "Customer State", "Customer Zipcode"
).dropDuplicates()

orders = df.select(
    "Order Id", "Order Customer Id", "Order Date",
    "Sales", "Order Item Quantity", "Order Item Id", "Order Item Product Price", "Order Item "
                                                                                 "Total", "Order Item Discount",
    "Order Item Discount Rate", "Order Status", "Order City", "Order State",
    "Order Country", "Order Region", "Product Card Id"
).dropDuplicates()

# products table
products = df.select(
    "Product Card Id", "Product Name", "Product Category Id", "Category Id", "Category Name", "Product Card Id",
    "Department Id", "Department Name", "Product Price", "Product Status"
).dropDuplicates()

# shipping_info table
shipping_info = df.select(
    "Order Id", "Shipping Mode", "Order Date",
    "Days for shipment (scheduled)", "Days for shipping (real)",
    "Delivery Status", "Late_delivery_risk"
).dropDuplicates()

# financials table
finance = df.select(
    "Order Id", "Sales per customer", "Benefit per order",
    "Order Profit Per Order", "Order Item Profit Ratio"
).dropDuplicates()

# (Tuỳ chọn) Tạo bảng tạm để truy vấn SQL nếu cần
customers.createOrReplaceTempView("customers")
products.createOrReplaceTempView("products")
orders.createOrReplaceTempView("orders")
shipping_info.createOrReplaceTempView("shipping_info")
finance.createOrReplaceTempView("finance")

# Câu 1. Trung bình lợi nhuận trên mỗi đơn hàng theo ngành hàng
print("Câu 1. Trung bình lợi nhuận trên mỗi đơn hàng theo ngành hàng")
spark.sql("""SELECT p.`Department Name`,
                    ROUND(AVG(f.`Order Profit Per Order`), 2) AS avg_profit_per_order
             FROM products p
                      JOIN orders o ON p.`Product Card Id` = o.`Product Card Id`
                      JOIN finance f ON f.`Order Id` = o.`Order Id`
             GROUP BY p.`Department Name`
             ORDER BY avg_profit_per_order DESC;
          """
          ).show()

# Câu 2. Top 5 sản phẩm có doanh thu cao nhất nhưng lợi nhuận thấp (low-margin bestsellers)
print("Câu 2. Top 5 sản phẩm có doanh thu cao nhất nhưng lợi nhuận thấp")
spark.sql("""SELECT p.`Product Name`,
                    ROUND(SUM(o.Sales), 2)                     AS total_sales,
                    ROUND(SUM(f.`Order Profit Per Order`), 2)  AS total_profit,
                    ROUND(AVG(f.`Order Item Profit Ratio`), 4) AS avg_profit_ratio
             FROM orders o
                      JOIN products p ON o.`Product Card Id` = p.`Product Card Id`
                      JOIN finance f ON o.`Order Id` = f.`Order Id`
             GROUP BY p.`Product Name`
             HAVING total_sales > 10000
                AND avg_profit_ratio < 0.1
             ORDER BY total_sales DESC LIMIT 5"""
          ).show(truncate=False)

# Câu 3. Phân tích lợi nhuận các ngành hàng trong các tháng
print("Câu 3. Phân tích lợi nhuận các ngành hàng trong các tháng")
spark.sql("""SELECT
                 MONTH (o.`order date`) AS month, p.`Department Name`, ROUND(SUM (f.`Order Profit Per Order`), 2) AS monthly_profit
             FROM orders o
                 JOIN products p
             ON o.`Product Card Id` = p.`Product Card Id`
                 JOIN finance f ON o.`Order Id` = f.`Order Id`
             GROUP BY month, p.`Department Name`
             ORDER BY month, monthly_profit DESC;
          """
          ).show()

# Câu 4. Phân tích mức độ rời bỏ của khách hàng
print("Câu 4. Phân tích mức độ rời bỏ của khách hàng")
spark.sql("""
          WITH last_purchase AS (SELECT `Order Customer Id` AS customer_id,
                                        MAX(`Order Date`)   as last_order_date
                                 FROM orders
                                 GROUP BY `Order Customer Id`),


               churn_risk AS (SELECT customer_id,
                                     last_order_date,
                                     DATEDIFF(DATE('2018-01-01'), last_order_date) AS days_since_last_purchase,
                                     CASE
                                         WHEN DATEDIFF(DATE('2018-01-01'), last_order_date) <= 90 THEN 'Low Risk'
                                         WHEN DATEDIFF(DATE('2018-01-01'), last_order_date) <= 180 THEN 'Medium Risk'
                                         ELSE 'High Risk'
                                         END                                       AS churn_segment
                              FROM last_purchase),

               orders_with_risk AS (SELECT o.*,
                                           cr.churn_segment
                                    FROM orders o
                                             JOIN churn_risk cr
                                                  ON o.`Order Customer Id` = cr.customer_id),


               orders_with_customer AS (SELECT ow.*,
                                               c.`Customer Segment`
                                        FROM orders_with_risk ow
                                                 JOIN customers c
                                                      ON ow.`Order Customer Id` = c.`Customer Id`)


          SELECT churn_segment,
                 `Customer Segment`,
                 ROUND(AVG(Sales), 2)                      AS avg_sales,
                 ROUND(AVG(`Order Item Quantity`), 2)      AS avg_quantity,
                 ROUND(AVG(`Order Item Discount Rate`), 4) AS avg_discount_rate
          FROM orders_with_customer
          GROUP BY churn_segment, `Customer Segment`
          ORDER BY `Customer Segment`;
          """).show()

# Câu 5. Phân tích khoảng thời gian giữa 2 lần mua hàng
print("Câu 5. Phân tích khoảng thời gian giữa 2 lần mua hàng")
spark.sql("""
          WITH customer_orders AS (SELECT o.`Order Customer Id`,
                                          COUNT(DISTINCT o.`Order Id`) AS purchase_count,
                                          MIN(o.`Order Date`),
                                          MAX(o.`Order Date`),
                                          DATEDIFF(
                                                  MAX(o.`Order Date`),
                                                  MIN(o.`Order Date`)
                                          )                            AS active_days
                                   FROM orders o
                                   WHERE o.`Order Date` IS NOT NULL
                                   GROUP BY o.`Order Customer Id`),
               customer_metrics AS (SELECT o.`Order Customer Id`,
                                           CASE
                                               WHEN ROUND(active_days / (purchase_count - 1), 2) < 30
                                                   THEN 'Frequent (<30 days)'
                                               WHEN ROUND(active_days / (purchase_count - 1), 2) BETWEEN 30 AND 90
                                                   THEN 'Regular (30–90 days)'
                                               ELSE 'Infrequent (>90 days)'
                                               END                                      AS purchase_frequency,
                                           ROUND(active_days, 2)                        AS avg_days_active,
                                           ROUND(active_days / (purchase_count - 1), 2) AS avg_days_between,
                                           purchase_count
                                    FROM customer_orders o),
               final_metrics AS (SELECT m.purchase_frequency,
                                        COUNT(DISTINCT m.`Order Customer Id`)     AS customer_count,
                                        ROUND(AVG(m.purchase_count), 2)           AS avg_purchase_count,
                                        ROUND(AVG(m.avg_days_active), 2)          AS avg_days_active,
                                        ROUND(AVG(m.avg_days_between), 2)         AS avg_days_between,
                                        ROUND(AVG(f.`Sales per customer`), 2)     AS avg_total_spent,
                                        ROUND(AVG(f.`Order Profit Per Order`), 2) AS avg_order_profit
                                 FROM customer_metrics m
                                          JOIN finance f ON m.`Order Customer Id` = f.`Order Id`
                                 GROUP BY m.purchase_frequency)
          SELECT *
          FROM final_metrics
          """).show()

# Câu 6. Xác định khu vực có tỷ lệ đơn hàng giao trễ cao nhất, làm nhiều khách hàng chịu ảnh hưởng nhất
print("Câu 6. Xác định khu vực có tỷ lệ đơn hàng giao trễ cao nhất, làm nhiều khách hàng chịu ảnh hưởng nhất")
spark.sql("""
          SELECT *
          FROM (SELECT o.`Order Region`                AS region,
                       COUNT(DISTINCT c.`Customer Id`) AS affected_customers,
                       ROUND(
                               SUM(CASE
                                       WHEN s.`Days for shipping (real)` > s.`Days for shipment (scheduled)`
                                           THEN 1
                                       ELSE 0
                                   END) * 100.0 / COUNT(*),
                               2
                       )                               AS late_rate
                FROM orders o
                         JOIN customers c ON o.`Order Customer Id` = c.`Customer Id`
                         JOIN shipping_info s ON o.`Order Id` = s.`Order Id`
                GROUP BY o.`Order Region`) t
          WHERE late_rate > 30
          ORDER BY late_rate DESC
          """).show()

# Câu 7. Xác định xu hướng giảm lợi nhuận theo tháng của ở ngành hàng
print("Câu 7. Xác định xu hướng giảm lợi nhuận theo tháng của ở ngành hàng")
spark.sql("""
          WITH monthly_profit AS (SELECT p.`Department Name`,
              MONTH (o.`order date`) AS month
             , ROUND(SUM (f.`Order Profit Per Order`)
             , 2) AS monthly_profit
          FROM orders o
              JOIN products p
          ON o.`Product Card Id` = p.`Product Card Id`
              JOIN finance f ON o.`Order Id` = f.`Order Id`
          GROUP BY p.`Department Name`, month
              ),
              profit_diff AS (
          SELECT *, ROUND(monthly_profit - LAG(monthly_profit) OVER (PARTITION BY `Department Name` ORDER BY month), 2) AS profit_change
          FROM monthly_profit
              )
          SELECT *
          FROM profit_diff
          WHERE profit_change < 0
          ORDER BY profit_change;
          """).show(10)

# Câu 8. Xác định vùng có khách hàng trung thành nhất
print("Câu 8. Xác định vùng có khách hàng trung thành nhất")
spark.sql("""
          SELECT o.`Order Region`                                 AS Region,
                 o.`Order Customer Id`                            AS CustomerID,
                 COUNT(*)                                         AS Total_Orders,
                 SUM(o.Sales)                                     AS Total_Sales,
                 SUM(CASE
                         WHEN s.`Days for shipping (real)` <= s.`Days for shipment (scheduled)` THEN 1
                         ELSE 0 END)                              AS OnTime_Deliveries,
                 ROUND(SUM(CASE
                               WHEN s.`Days for shipping (real)` <= s.`Days for shipment (scheduled)` THEN 1
                               ELSE 0 END) * 100.0 / COUNT(*), 2) AS OnTime_Rate
          FROM orders o
                   JOIN shipping_info s ON o.`Order Id` = s.`Order Id`
          GROUP BY Region, CustomerID
          HAVING Total_Orders > 2
             AND Total_Sales > 1000
             AND OnTime_Rate > 70
          ORDER BY Total_Sales DESC
          """).show(10)

# Câu 9. Hiệu quả marketing theo phân khúc khách hàng & mức giảm giá (Discount Tier Analysis)
print("Câu 9. Hiệu quả marketing theo phân khúc khách hàng & mức giảm giá")
spark.sql("""
          SELECT c.`Customer Segment`,
                 COUNT(DISTINCT c.`Customer Id`)                                               AS total_customers,
                 COUNT(o.`Order Id`)                                                           AS total_orders,
                 ROUND(AVG(o.`Order Item Discount Rate`), 2)                                   AS avg_discount_rate,
                 SUM(CASE WHEN o.`Order Item Discount Rate` >= 0.2 THEN 1 ELSE 0 END)          AS high_discount_orders,
                 ROUND(SUM(CASE WHEN o.`Order Item Discount Rate` >= 0.2 THEN 1 ELSE 0 END) * 100.0 /
                       COUNT(o.`Order Id`),
                       2)                                                                      AS high_discount_ratio_pct,
                 SUM(o.`Sales`)                                                                AS total_sales,
                 SUM(f.`Order Profit Per Order`)                                               AS total_profit,
                 ROUND(SUM(f.`Order Profit Per Order`) * 100.0 / NULLIF(SUM(o.`Sales`), 0), 2) AS profit_margin_pct
          FROM orders o
                   JOIN customers c ON o.`Order Customer Id` = c.`Customer Id`
                   JOIN finance f ON o.`Order Id` = f.`Order Id`
          GROUP BY c.`Customer Segment`
          ORDER BY total_profit DESC
          """).show()

# Câu 10. Phân tích khách hàng theo mức độ hoạt động (RFM)
print("Câu 10. Phân tích khách hàng theo mức độ hoạt động (RFM)")
spark.sql("""
WITH last_order AS (
   SELECT
       o.`Order Customer Id` AS customer_id,
       MAX(o.`Order Date`) AS last_purchase_date,
       COUNT(o.`Order Id`) AS frequency,
       SUM(o.`Sales`) AS total_spent
   FROM orders o
   GROUP BY o.`Order Customer Id`
),
rfm_analysis AS (
   SELECT
       c.`Customer Id`,
       c.`Customer Fname`,
       datediff(current_date(), l.last_purchase_date) AS recency,
       l.frequency,
       l.total_spent AS monetary,
       NTILE(5) OVER (ORDER BY datediff(current_date(), l.last_purchase_date) DESC) AS r_score,
       NTILE(5) OVER (ORDER BY l.frequency) AS f_score,
       NTILE(5) OVER (ORDER BY l.total_spent) AS m_score
   FROM customers c
   JOIN last_order l ON c.`Customer Id` = l.customer_id
)
SELECT
   CONCAT(r_score, f_score, m_score) AS rfm_segment,
   COUNT(*) AS customer_count,
   ROUND(AVG(recency), 2) AS avg_recency,
   ROUND(AVG(frequency), 2) AS avg_frequency,
   ROUND(AVG(monetary), 2) AS avg_monetary
FROM rfm_analysis
GROUP BY rfm_segment
ORDER BY rfm_segment
""").show(truncate=False)



