import duckdb
import xarray as xr

query = """
-- via Python's duckdb.connect()
INSTALL spatial;   LOAD spatial;
INSTALL h3;        LOAD h3;

-- 1) build the set of H3 cells covering query polygon
WITH cells AS (
  SELECT unnest(
    h3_polyfill(
      ST_GeomFromText(:place_wkt,4326),
      8           -- same resolution as your Zarr
    )
  ) AS h3_index
)
-- read only the needed partitions, then filter by h3_index
SELECT
  date,
  h3_index,
  zarr_path
FROM
  parquet_scan('/data/ortho_catalog/*/*/*.parquet')
JOIN
  cells USING (h3_index)
WHERE
  date = DATE '2025-01-01';
"""

conn = duckdb.connect()
conn.execute("INSTALL spatial; LOAD spatial; INSTALL h3; LOAD h3;")

# define building polygon in WKT
place_wkt = "POLYGON((...))"

query = """…the SQL above…"""

# run it and open the resulting Zarr stores as one xarray dataset:
paths = conn.execute(query, {"place_wkt": place_wkt}).df()["zarr_path"].tolist()
ds = xr.open_mfdataset(
    [f"/data/{p}" for p in paths],
    chunks={"time":1, "y":256, "x":256},
    concat_dim="time"   # or "h3_index"
)
