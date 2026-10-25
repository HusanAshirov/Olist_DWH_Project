# Data profiling: Olist source files

Profiled on: YYYY-MM-DD
Source: `/Volumes/olist_dwh/landing/raw` (full Kaggle download)
Notebooks: `notebooks/01_profiling/01_profile_sources`, `02_referential_integrity`

## 1. Summary

| Source | Rows | Columns | Primary key | Duplicated keys |
| --- | --- | --- | --- | --- |
| orders | | | order_id | |
| order_items | | | order_id + order_item_id | |
| payments | | | order_id + payment_sequential | |
| reviews | | | review_id | |
| customers | | | customer_id | |
| products | | | product_id | |
| sellers | | | seller_id | |
| geolocation | | | none (zip prefix repeats) | |
| category_translation | | | product_category_name | |

## 2. Issues found

One row per issue. The "Silver rule" column is what you will build in Phase 4.

| # | Source | Column(s) | Issue | Rows affected | Silver rule | Severity |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | | | | | | |

Severity: **Critical** = would break joins or totals; **High** = wrong numbers in reports; **Low** = cosmetic.

### Things to check for (confirm or reject each one with your own numbers)

- [ ] orders: which statuses exist, and do non-delivered orders have null delivery dates?
- [ ] orders: delivered orders with no delivered date; dates out of order
- [ ] reviews: duplicated review_id; orders with more than one review
- [ ] customers: customer_id vs customer_unique_id counts; customers at more than one zip code
- [ ] products: null category; misspelled column names; null dimensions or weight
- [ ] category_translation: categories in products that have no English name; hidden BOM in header
- [ ] geolocation: many rows per zip prefix; same city spelled several ways
- [ ] payments: payment_type `not_defined`; 0 installments; payments not matching items + freight
- [ ] zip codes in customers or sellers that are missing from geolocation

## 3. Referential integrity results

| Check | Failing rows | Decision |
| --- | --- | --- |
| order_items without an order | | |
| orders without a customer | | |
| order_items with unknown product | | |
| products with no category | | |
| categories missing from translation | | |
| customer zip not in geolocation | | |

## 4. Decisions for silver

Summarize the rules here once the table above is filled in, e.g.:

- Keys: ...
- Deduplication: ...
- Missing references: map to unknown member (key -1) in gold
- Data types: ...
