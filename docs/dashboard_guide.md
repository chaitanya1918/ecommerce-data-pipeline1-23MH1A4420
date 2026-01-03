text

# 📊 Dashboard User Guide

## **Analytics Generated** (11 CSV Reports)

data/processed/analytics/

├── query1_top_products.csv # Top 10 products by revenue

├── query2_monthly_trend.csv # Monthly revenue trend

├── query3_customer_segmentation.csv # RFM analysis (VIP/Loyal)

├── query4_category_performance.csv # Category revenue/profit

├── query5_weekly_patterns.csv # Day-of-week sales patterns

├── query6_geographic_analysis.csv # Top states by orders

├── query7_payment_methods.csv # Payment method distribution

├── query8_profit_margins.csv # Product margin analysis

├── query9_customer_lifetime_value.csv # CLV segments

├── query10_repeat_purchase_rate.csv # Customer retention metrics

└── query11_pareto_analysis.csv # 80/20 revenue concentration

text

## **How to Create BI Dashboard** (Tableau/PowerBI)

### **Step 1: Import CSVs**

Open Tableau Public / PowerBI Desktop

Connect → "Text Files" → Select all 11 CSVs from data/processed/analytics/

Tableau: "Multiple Tables" | PowerBI: "Get Data → Folder"

text

### **Step 2: Key Visualizations** (Copy-Paste Ready)

TOP PRODUCTS BAR CHART

X: product_name - Y: total_revenue - Top 10 filter

MONTHLY TREND LINE CHART

X: month_year - Y: total_revenue - Trend line

CUSTOMER SEGMENT PIE CHART

Dimension: customer_segment - Measure: total_revenue

WEEKLY HEATMAP

X: day_of_week - Y: hour_of_day - Color: transaction_count

GEOGRAPHY MAP

Latitude/Longitude: state_coords - Color: total_orders

text

### **Step 3: Export Instructions**

PowerBI: File → Export → PDF/PowerPoint

text

## **Sample Insights to Highlight**

Electronics dominates (45% revenue)

Weekends generate 28% sales uplift

Top 10% customers = 35% revenue (Pareto)

Premium products yield 25%+ margins

Top 5 states = 68% orders