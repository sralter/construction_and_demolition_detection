import os
import pandas as pd
import h3
from datetime import date

def ingest_zarr(zarr_src, date_acq, res=8, out_meta_root="data/ortho_catalog"):
    # 1. read the .zarr store’s footprint (here as its H3 index)
    #    let’s assume you’ve pre‐computed the h3_index for the tile:
    h3_index = compute_h3_for_zarr(zarr_src, res)
    date_str = date_acq.isoformat()             # “2025-01-01”
    
    # 2. build the metadata record
    centroid = h3.h3_to_geo(h3_index)           # (lat, lon)
    rec = {
      "date":       date_acq,
      "h3_index":   h3_index,
      "zarr_path":  os.path.relpath(zarr_src, start="data"),
      "centroid_x": centroid[1],
      "centroid_y": centroid[0],
    }
    df = pd.DataFrame([rec])
    
    # 3. write to partitioned parquet
    out_dir = os.path.join(out_meta_root,
                           f"date={date_str}",
                           f"h3={h3_index}")
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(os.path.join(out_dir, "metadata.parquet"),
                  index=False)