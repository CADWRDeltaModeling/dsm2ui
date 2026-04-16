import os
import pandas as pd
import dms_datastore
from dms_datastore import read_ts
import pyhecdss as dss
from pathlib import Path
import tqdm


# this should be a util function
def find_lastest_fname(pattern, dir="."):
    d = Path(dir)
    fname, mtime = None, 0
    for f in d.glob(pattern):
        fmtime = f.stat().st_mtime
        if fmtime > mtime:
            mtime = fmtime
            fname = f.absolute()
    return fname, mtime


def read_from_datastore_write_to_dss(
    datastore_dir, dssfile, param, repo_level="screened", unit_name=None
):
    """
    Reads datastore timeseries files and writes to a DSS file

    Parameters
    ----------
    datastore_dir : str
        Directory where Datastore files are stored
    repo_level : str
        default is screened
    dssfile : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    """
    inventory_file, mtime = find_lastest_fname(
        f"inventory_datasets_{repo_level}*.csv", datastore_dir
    )
    print("Using inventory file:", inventory_file)
    inventory = pd.read_csv(inventory_file)
    param_inventory = inventory[inventory["param"] == param]
    apart = "DMS-DATASTORE"
    fpart = os.path.basename(inventory_file).split("_")[-1].split(".csv")[0]
    with dss.DSSFile(dssfile, create_new=True) as f:
        for idx, row in tqdm.tqdm(
            param_inventory.iterrows(), total=len(param_inventory)
        ):
            filepattern = os.path.join(datastore_dir, repo_level, row["file_pattern"])
            ts = read_ts(filepattern)
            print("Reading ", filepattern)
            if pd.isna(row["subloc"]):
                bpart = row["station_id"]
            else:
                bpart = row["station_id"] + row["subloc"]
            epart = ts.index.freqstr
            pathname = f'/{apart}/{bpart}/{row["param"]}///{fpart}/'
            print("Writing to ", pathname)
            f.write_rts(
                pathname,
                ts,
                unit_name if unit_name is not None else row["unit"],
                "INST-VAL",
            )
    print("Done")


def write_station_lat_lng(datastore_dir, station_file, param, repo_level="screened"):
    """
    Writes station metadata to a csv file.

    Columns written: station_id, station_name, agency, lat, lon,
    utm_easting, utm_northing (x/y from the inventory, UTM Zone 10N).

    Parameters
    ----------
    datastore_dir : str
        Directory where Datastore files are stored
    station_file : str
        Filename to write to
    param : str
        e.g one of "flow","elev", "ec", etc.
    repo_level : str
        default is screened
    """
    inventory_file, mtime = find_lastest_fname(
        f"inventory_datasets_{repo_level}*.csv", datastore_dir
    )
    print("Using inventory file:", inventory_file)
    inventory = pd.read_csv(inventory_file)
    inventory = inventory[inventory["param"] == param]
    inventory = inventory.drop_duplicates(subset=["station_id"])
    cols = ["station_id"]
    rename = {}
    if "name" in inventory.columns:
        cols.append("name")
        rename["name"] = "station_name"
    if "agency" in inventory.columns:
        cols.append("agency")
    cols += ["lat", "lon"]
    if "x" in inventory.columns and "y" in inventory.columns:
        cols += ["x", "y"]
        rename.update({"x": "utm_easting", "y": "utm_northing"})
    inventory = inventory[cols].rename(columns=rename)
    inventory.to_csv(station_file, index=False)
    print("Wrote to ", station_file)
    print("Done")
