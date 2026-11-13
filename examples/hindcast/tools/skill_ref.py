"""The numbers tests/hindcast.sh checks, got independently of Hindcast: ecCodes
decodes the fixtures, numpy scores 2026-11-12 +24h against 2026-11-13's
analysis, and each city's grid point is the nearest by great-circle
distance. Run once by hand, not in CI:

    pixi exec -s python=3.12 -s numpy -s python-eccodes \
        python -I examples/hindcast/tools/skill_ref.py examples/hindcast/fixtures
"""
import sys, numpy as np, eccodes
fx = sys.argv[1]
def field(cycle, lead):
    with open(f"{fx}/gfs.{cycle}/00/atmos/gfs.t00z.pgrb2.1p00.f{lead:03d}", "rb") as f:
        h = eccodes.codes_grib_new_from_file(f)
        assert eccodes.codes_get(h, "shortName") == "2t", eccodes.codes_get(h, "shortName")
        v = eccodes.codes_get_values(h); lat = eccodes.codes_get_array(h, "latitudes"); lon = eccodes.codes_get_array(h, "longitudes")
        eccodes.codes_release(h)
    return v, lat, lon
a, lat, lon = field("20261113", 0)
f, lat2, lon2 = field("20261112", 24)
assert (lat == lat2).all() and (lon == lon2).all() and len(a) == 65160
w = np.cos(np.radians(lat)); d = f - a
print("bias %.6f mae %.6f rmse %.6f" % ((w*d).sum()/w.sum(), (w*abs(d)).sum()/w.sum(), np.sqrt((w*d*d).sum()/w.sum())))
print("unweighted bias %.6f rmse %.6f" % (d.mean(), np.sqrt((d*d).mean())))
cities = [("New York",40.71,-74.01),("London",51.51,-0.13),("Tokyo",35.68,139.69),("Sydney",-33.87,151.21),
 ("Sao Paulo",-23.55,-46.63),("Cairo",30.04,31.24),("Mumbai",19.08,72.88),("Moscow",55.76,37.62),
 ("Lagos",6.52,3.38),("Anchorage",61.22,-149.90)]
for name, la, lo in cities:
    # nearest point by great-circle distance, not by the grid's arithmetic
    lon180 = np.where(lon >= 180, lon - 360, lon)
    dist = np.arccos(np.clip(np.sin(np.radians(la))*np.sin(np.radians(lat)) + np.cos(np.radians(la))*np.cos(np.radians(lat))*np.cos(np.radians(lo - lon180)), -1, 1))
    i = int(dist.argmin())
    print("%-10s %6.1f %7.1f  forecast %.4f analysis %.4f error %+.4f" % (name, lat[i], lon180[i], f[i], a[i], f[i]-a[i]))
