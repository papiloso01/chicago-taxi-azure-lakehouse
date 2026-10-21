# Official community areas: names, boundaries and Tableau maps

## Scope and source

Source: [City of Chicago Boundaries – Community Areas](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas/igwz-8jzy).
The implementation was checked against a read-only public response: 77 rows, IDs 1–77, named areas and valid polygon boundaries.
This does not establish Azure ingestion or Tableau map acceptance.

This is a current reference snapshot, not a historical boundary model. Applying it to January–May 2025
labels trips by their existing area IDs; it does not prove historical boundary equivalence or reconstruct routes.

| Layer | Artifact | Grain |
|---|---|---|
| Landing | community_areas/run_id=…/data.json, manifest, audit | One immutable full API response |
| Bronze | community_area_snapshots | source_run_id + area_id; original row payload retained |
| Silver | ref_community_area | 77 authoritative IDs, names, GeoJSON and in-boundary label points |
| Silver | community_area_loads | Completed reference publication and Delta version |
| Silver | warehouse_geography | Reference Delta version bound to one silver_load_id |
| Silver | dim_community_area | Observed trip IDs/unknown key, names resolved from the published reference |
| Gold | mart_area_demand_daily | Existing grain/schema; now contains names |
| Gold | mart_area_map_daily | Date + area_role + known area_key; demand metrics, geometry and map coordinates |
| File | community_areas.geojson | 77 full boundaries with area_id, area_name, reference_run_id properties |

Reference inputs must contain exactly 77 unique IDs covering 1–77, nonblank unique names, consistent duplicate
ID fields, finite longitude/latitude positions in a broad Chicago region, closed rings and nonempty valid
Polygon/MultiPolygon topology. Geometry is not silently repaired. Shapely creates a representative point
inside each polygon; these map coordinates are **area label points, not pickup/dropoff coordinates**.

## Publication and rebuild protection

Reference ingestion validates the whole raw snapshot before Delta writes. Bronze merges immutable run/ID
keys. The current silver reference is overwritten, then its completed audit records the exact version.
A replay of the latest successfully published run skips rewriting it; older snapshots cannot replace a
newer publication. If an incomplete reference write exists without a completed audit, recover ingestion.

Silver 0.7.0 reads the latest **completed** reference version and records that version in warehouse_geography
before its completed warehouse audit. It rebuilds dimension names from that lookup every time. Unknown
area key 0 stays unavailable; no name or geometry is invented for it. Trips/facts are not spatially reallocated.

Gold 0.7.0 reads the reference version bound to its source silver_load_id. A legacy silver publication lacking
geographic lineage is rejected once a reference has been configured; rebuild silver with 0.7.0 first. Map joins
must preserve every known-area demand row and its trip/revenue metrics. Unknown-area demand stays in the
existing demand mart but is excluded from the map. Existing gold grains/schemas remain unchanged; the map
is an additional optional table, and its reference version is added to the existing audit versions JSON.

Writers must be serialized. Current-reference overwrite, audit append, silver tables and gold tables are separate
transactions; stop dashboard refreshes/readers during activation or failure. Complete/recover each stage before
running the next. Do not run an old silver package after enrichment: upgrade all active silver writers so names
cannot be cleared by legacy code. Delta history retention must cover versions referenced by published lineage.
Historical snapshots can become unreadable after VACUUM/retention expiry; do not treat version numbers as archival backups.

## Activate after PR review/merge

From the repository folder in Windows PowerShell or CMD:

```text
git switch main
git pull origin main
python -m build --wheel
```

1. Record current gold trip/revenue totals. Stop pipeline runs and Tableau refreshes/open dashboard sessions.
2. Upload `dist/chicago_taxi_azure_lakehouse-0.7.0-py3-none-any.whl` into landing → raw_files → libraries.
3. Update the active **silver and gold** notebook/job Environment dependencies to the 0.7.0 wheel (one version per
   environment). Silver Job ID: 654018398720089; gold Job ID: 800504364677614. The notebook source files are unchanged.
   Use Python >=3.12; retain gold's working Standard v6. Ingestion/bronze taxi jobs can keep their tested dependencies.
4. Import `databricks/notebooks/load_community_areas.py`. Choose Serverless Standard v6 and dependency:
   `/Volumes/chicago_taxi_dev/landing/raw_files/libraries/chicago_taxi_azure_lakehouse-0.7.0-py3-none-any.whl`.
5. Run imports/widgets. Keep catalog chicago_taxi_dev and the default volume root. Leave reference_run_id blank
   to fetch the public source, then run the load cell. Expect 77 rows, a source_run_id, reference_version and geojson_path.
   To recover/replay without fetching, set reference_run_id to that successful landing run and rerun.
6. Verify:

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT area_id) AS unique_ids,
       MIN(area_id) AS first_id, MAX(area_id) AS last_id
FROM chicago_taxi_dev.silver.ref_community_area;
SELECT * FROM chicago_taxi_dev.silver.community_area_loads ORDER BY completed_at_utc DESC;
```

Expect 77, 77, 1, 77. The completed audit is authoritative; inspect its recorded version if a newer failed write exists.

7. Run the saved silver job with 0.7.0, then the saved gold job with 0.7.0. This reuses the current January–May bronze
   dataset; it does not require a five-month API extraction. A new gold audit must reference the new silver load and
   include ref_community_area in silver_versions_json.
8. Verify the name coverage and totals:

```sql
SELECT area_key, area_name, is_unknown
FROM chicago_taxi_dev.silver.dim_community_area ORDER BY area_key;
SELECT COUNT(*) AS unnamed_known_areas
FROM chicago_taxi_dev.silver.dim_community_area WHERE area_key <> 0 AND area_name IS NULL;
SELECT SUM(trip_count) AS trips, SUM(known_revenue_usd) AS revenue_usd
FROM chicago_taxi_dev.gold.mart_daily_summary;
SELECT area_role, SUM(trip_count) AS mapped_trips, SUM(known_revenue_usd) AS mapped_revenue
FROM chicago_taxi_dev.gold.mart_area_map_daily GROUP BY area_role;
SELECT area_role, SUM(trip_count) AS known_area_trips, SUM(known_revenue_usd) AS known_area_revenue
FROM chicago_taxi_dev.gold.mart_area_demand_daily WHERE area_key <> 0 GROUP BY area_role;
```

Known areas must be named; key 0 remains null. Daily trips/revenue must match the baseline. Map totals must match
known-area demand **per role**; do not compare against totals including unknown area 0 or combine both roles.

9. Replay silver then gold once more: names must persist and business totals stay unchanged. Complete the existing
   four-stage ADF pipeline test using its current parameters only after standalone acceptance; save/publish as needed.
   Do not rerun a five-month extraction just to refresh names. No reference ADF activity or schedule is added here.

## Tableau Cloud: named demand and live point map

1. Refresh the existing area-demand data source. Replace area_id on the bar chart with area_name. Keep pickup filtering
   and exclusion of key 0. The existing date calculations remain valid.
2. Add a separate Databricks data source for `chicago_taxi_dev.gold.mart_area_map_daily`.
3. Create a worksheet. Set map_latitude/map_longitude geographic roles to Latitude/Longitude and place longitude on
   Columns, latitude on Rows. Use AVG for coordinates; put area_key on Detail and area_name on Label/Tooltip.
4. Filter area_role to pickup. Put SUM(trip_count) on Size/Colour. Add the existing workbook start/end date calculation
   to this new source and filter it to True. Points represent area-level demand, not exact taxi locations.
5. Add the sheet to the dashboard. Validate period totals against SQL. Do not SUM coordinates or geometry fields.

## Polygon boundaries

The reference notebook returns a GeoJSON path inside its immutable landing folder. Download that file from Catalog →
landing → raw_files → community_areas → run_id=… . Tableau Cloud Creator can upload a GeoJSON file as a spatial source.
It contains boundaries/names only; it is not a dynamic demand extract. To colour polygons by demand, relate/join the
file's unique area_id to area_key in the map/demand mart using supported Tableau data modelling, filtering one area_role
and the selected dates. Confirm count/revenue reconciliation after modelling. A manually uploaded file needs manual
replacement after an approved boundary refresh. The live point map needs no file join and remains the initial acceptance path.

Official Tableau reference: https://help.tableau.com/current/pro/desktop/en-us/maps_shapefiles.htm
Shapely reference: https://shapely.readthedocs.io/en/stable/reference/shapely.MultiPolygon.html

## Testing and limitations

Functional geometry/manifest checks, HTTP fixture integration, Spark name/map reconciliation, reference-version lineage
and offline source-to-gold enrichment tests run in the existing four CI suites. Unit publication tests simulate Delta
history/audit behavior; actual Delta MERGE/overwrite, serverless Shapely installation, recovery and Tableau spatial modelling
still require live acceptance. The public 77-row validation is separate from those cloud tests.

This PR does not change taxi business rules, infer driver/gender, add weather data, deploy cloud jobs, publish a workbook,
create alerts/schedules, or guarantee annual-scale performance. Bronze/silver/gold runtime measurements and late-arrival
policy remain future work.
