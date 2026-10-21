# Silver → gold reporting marts

The user confirmed matching counts and completed audits after the combined landing→bronze→silver
pipeline. Gold is implemented here but requires Databricks acceptance before BI/reporting use.

## Business questions and tables

| Mart | Grain | Reporting purpose |
|---|---|---|
| mart_daily_summary | One observed Chicago start date | Trips, known gross revenue, known distance, coverage, distance bounds |
| mart_taxi_revenue_daily | Date + known taxi | Vehicle revenue ranking; never described as driver earnings |
| mart_company_payment_daily | Date + company + payment type | Company performance and payment mix, including unavailable members |
| mart_area_demand_daily | Date + pickup/dropoff role + area key | Pickup and dropoff demand, including missing area key 0 |
| mart_distance_extremes | Date + extreme type + trip ID | All ties for longest and shortest-positive reported trip distance |
| reporting_loads | One completed gold build | Source silver load ID, pinned table versions, accepted/quarantine counts |

Every mart contains gold_load_id for publication checks. Schemas and grain keys are defined in
`src/chicago_taxi/transformations/gold/contracts.py` and `metadata/gold_tables.yml`.

## Metrics and limitations

- USD revenue means reported gross trip_total, not profit or driver earnings. Taxi IDs identify vehicles.
- known_revenue_usd sums only nonnull trip totals; if all totals are unavailable, the sum stays null.
  revenue_trip_count and revenue_coverage expose how much of the trip population has this measure.
- Distance fields use silver kilometre values. Distance coverage is separate from revenue coverage.
  No road route is reconstructed. shortest_positive_trip_km excludes zero; longest_trip_km includes zero.
- identified_taxi_trip_count / trip_count gives taxi_identity_coverage. Unknown taxis are excluded from
  taxi rankings; their trips/revenue remain in the other applicable marts. Taxi mart totals reconcile
  only to the identified-vehicle population, not the whole city.
- revenue_rank is a per-date dense rank: ties share rank, zero reported revenue is valid, and a taxi
  with no known revenue has null rank. For a selected multi-day period, recompute rankings from summed
  known_revenue_usd; do not add daily ranks or use them as period ranks.
- Do not add/average averages or coverage fractions across dates/groups. Recompute weighted averages
  as sum of known measure / sum of its known-value trip count, and coverage as summed counts / trips.
- Pickup and dropoff each count the trip once. Filter one area_role for city totals. Summing both roles
  doubles the trip and revenue population. Area names remain null until authoritative enrichment.
- All longest/shortest ties are retained. If a trip qualifies for both types, it appears once per type.
  Never use this extremes table as a trip-count population. There are no distance rows for all-null dates;
  an all-zero date has longest rows but no shortest-positive row.
- Dates are Chicago local trip-start dates from silver. Missing dates are not filled with synthetic zeros.
- Passenger gender is unavailable. This project cannot answer which gender takes more trips.
- Gold excludes quarantine. Audit counts retain that exclusion so BI must report accepted-trip coverage,
  rather than claim all extracted records were suitable for reporting. Initial live data is one test day,
  not evidence of full-year trends. Weather and community-name enrichment remain separate work.

## Input consistency and publication

Gold requires a completed silver warehouse_loads audit. It pins all seven silver Delta table versions,
rejects any version committed after that audit (including a partially published silver rebuild), checks
that the audit identity did not change while versions were captured, and revalidates silver counts/joins.
Versions and the silver load ID are retained in gold reporting_loads for lineage. Concurrent silver/gold
writers and manual table edits are not supported; stop competing work for acceptance and publication.

Gold validates per-date trip counts, known revenue/distance counts and totals for daily, company/payment,
each area role and the identified-taxi subset. It also checks each mart's declared unique grain.

Gold rebuilds full snapshots. Each Delta table overwrite is atomic, but the full group/audit is not one
transaction. Block dashboard refreshes during rebuild/failure. Resume reporting only when the whole job
succeeds, reporting_loads contains its ID, and every nonempty mart has that same gold_load_id. A failed
partial publication requires a full rerun. Empty marts legitimately contain no load ID rows.
Annual-scale build cost/performance has not been benchmarked. No ADF gold activity, schedule, CSV export,
MySQL publication, Tableau workbook or Power BI deployment is created by this change.

## First acceptance after merge (PowerShell or CMD, inside the repository)

```text
git switch main
git pull origin main
python -m build --wheel
```

1. Upload `dist/chicago_taxi_azure_lakehouse-0.6.0-py3-none-any.whl` to landing/raw_files/libraries.
2. Import `databricks/notebooks/build_taxi_gold.py` into Databricks Workspace.
3. Choose Serverless and set the Environment dependency to:
   `/Volumes/chicago_taxi_dev/landing/raw_files/libraries/chicago_taxi_azure_lakehouse-0.6.0-py3-none-any.whl`.
   Use only this version in the gold environment. Existing ingestion, bronze and silver jobs need no update.
4. Run imports/widgets, leave catalog chicago_taxi_dev, then run the load cell.
5. Expect status success, gold_load_id, source silver_load_id, fact_rows and quarantined_rows.
   If it reports unpublished silver changes, finish/recover the saved silver job, then rerun gold.
6. In SQL, verify the date totals against silver:

```sql
SELECT COUNT(*) AS accepted_trips, COUNT(trip_total) AS revenue_trip_count,
       SUM(trip_total) AS known_revenue_usd, COUNT(trip_km) AS distance_trip_count, SUM(trip_km) AS total_km
FROM chicago_taxi_dev.silver.fact_trip;
SELECT SUM(trip_count) AS accepted_trips, SUM(revenue_trip_count) AS revenue_trip_count,
       SUM(known_revenue_usd) AS known_revenue_usd, SUM(distance_trip_count) AS distance_trip_count,
       SUM(total_km) AS total_km
FROM chicago_taxi_dev.gold.mart_daily_summary;
SELECT * FROM chicago_taxi_dev.gold.reporting_loads ORDER BY completed_at_utc DESC;
```

Use the same frozen silver snapshot; a later silver rebuild can legitimately change comparisons.

7. Review initial reporting outputs:

```sql
SELECT * FROM chicago_taxi_dev.gold.mart_taxi_revenue_daily
WHERE revenue_rank <= 10 ORDER BY trip_date, revenue_rank, taxi_id;
SELECT * FROM chicago_taxi_dev.gold.mart_distance_extremes
ORDER BY trip_date, extreme_type, trip_id;
SELECT * FROM chicago_taxi_dev.gold.mart_area_demand_daily
WHERE area_role = 'pickup' ORDER BY trip_date, trip_count DESC;
```

8. Rerun the load cell without changing silver. Mart business values and grain counts must stay stable;
   gold_load_id changes and a new completed audit is expected. Verify join/revenue and load ID checks.
9. After acceptance, save job_build_taxi_gold: serverless, the 0.6.0 dependency, catalog parameter
   chicago_taxi_dev, max concurrency 1, retries 0, no schedule. Test the saved job before ADF integration.

## Testing

CI covers actual Spark mart transformations, precision/null/zero/coverage behavior, ties and ranking,
area-role separation, per-date reconciliation and schema contracts. Publication guards are unit-tested
with simulated Delta history/audit responses. The offline e2e now continues from fixture API rows through
landing files, bronze, silver and gold transformations. It excludes actual Delta storage, Azure, ADF,
subscription capacity and external BI; those remain live acceptance work.

## Reported live acceptance

The user completed the gold build using Standard v6, confirmed matching silver/gold totals and an audit, then successfully tested saved gold job 800504364677614 through ADF. Follow `adf_gold_handoff.md` for the combined four-stage run.
