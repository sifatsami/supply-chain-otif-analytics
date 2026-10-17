# Supply Chain Fulfillment & OTIF Risk Intelligence Platform

End-to-end supply chain data pipeline and BI dashboard that diagnoses why orders ship late and how much revenue that puts at risk, across 180,000+ orders.

Raw data moves through a **Medallion architecture (Bronze → Silver → Gold)** built in **Apache Spark (PySpark) and Delta Lake** on Linux (CachyOS), lands in a **Kimball star schema**, and is consumed by **Power BI Desktop**.

---

## Key Findings

- **The bottleneck is delivery, not the warehouse.** Overall OTIF (On-Time In-Full) is **40.88%**, even though the In-Full rate is **95.70%**. On-Time Delivery sits at **42.72%**, so shipping delays drive the failures, not stockouts.
- **Revenue exposure:** late deliveries put **$19M of $33M total sales** at risk.
- **Second Class shipping is the main offender.** On-time rate is only **~20.27%**, versus **60.21%** for Standard Class and **52.17%** for Same Day.
- **Regional concentration:** Western Europe and Central America carry the most delayed-order revenue, at over **$2.0M** and **$1.6M** respectively.
![Power BI Dashboard](https://github.com/sifatsami/supply-chain-otif-analytics/blob/main/PowerBI.png?raw=true)
---

## Architecture

```text
[ Raw Transactional Data ]
            │
            ▼  Bronze: ingestion & schema enforcement
[ PySpark / Delta Lake (Linux / CachyOS) ]
            │
            ▼  Silver: cleansing, SLA validation, OTIF flags
[ Delta Lake Parquet Optimization ]
            │
            ▼  Gold: dimensional modeling / star schema
[ Gold Parquet Tables ] ──(local transfer)──► [ Windows VM ]
                                                    │
                                                    ▼
                                         [ Power BI Desktop ]
                                         ├─ DAX model (13 measures)
                                         └─ Executive OTIF Risk Dashboard
```

**Stack**

| Layer | Tools |
|---|---|
| Data engineering | Python, Apache Spark (PySpark), Delta Lake, Linux (CachyOS) |
| Modeling | Kimball star schema (1 fact table, 4 dimensions) |
| BI & analytics | Microsoft Power BI, DAX |

---

## Data Model (Gold Layer)

A central fact table of **180,519 rows** connects to four dimensions through 1-to-many relationships.

| Table | Description |
|---|---|
| `fact_fulfillment` | Order-line events: shipment durations, scheduled benchmarks, sales amounts, and binary flags (`is_late`, `is_in_full`, `is_otif`) |
| `dim_date` | Calendar table marked as the official Date Table, with custom month-sort indexes |
| `dim_customer` | Customer segments, regional geography, market structure |
| `dim_shipping` | Carrier service levels (Standard Class, Second Class, First Class, Same Day) |
| `dim_product` | Product catalog metadata and category classifications |

---

## Core DAX Measures

All calculations live in a single `_Measures` table.

### Delivery & fulfillment reliability

```dax
Total Orders =
COUNTROWS(fact_fulfillment)

Orders On-Time =
CALCULATE(
    COUNTROWS(fact_fulfillment),
    fact_fulfillment[is_late] = 0
)

Orders In-Full =
CALCULATE(
    COUNTROWS(fact_fulfillment),
    fact_fulfillment[is_in_full] = 1
)

Orders OTIF =
CALCULATE(
    COUNTROWS(fact_fulfillment),
    fact_fulfillment[is_otif] = 1
)

OTIF Rate % =
DIVIDE([Orders OTIF], [Total Orders], 0)
```

### Lead time drift

```dax
Avg Real Shipping Days =
AVERAGE(fact_fulfillment[actual_shipping_days])

Avg Scheduled Shipping Days =
AVERAGE(fact_fulfillment[scheduled_shipping_days])

Lead Time Drift (Days) =
[Avg Real Shipping Days] - [Avg Scheduled Shipping Days]
```

### Financial exposure

```dax
Total Sales =
SUM(fact_fulfillment[sales_amount])

Revenue at Risk =
CALCULATE(
    [Total Sales],
    fact_fulfillment[is_late] = 1
)
```

---

## Dashboard

The Power BI report is built in three tiers:

1. **Executive KPI header:** five tiles for OTIF Rate (40.88%), On-Time Rate (42.72%), In-Full Rate (95.70%), Total Sales ($33M), and Revenue at Risk ($19M).
2. **Volume and reliability trend:** dual-axis chart of monthly order volume against delivery performance, separating seasonal surges from genuine reliability dips.
3. **Root-cause diagnostics:**
   - *Carrier performance:* SLA compliance by shipping method.
   - *Top 5 revenue hotspots:* territories with the most revenue delayed in transit.
