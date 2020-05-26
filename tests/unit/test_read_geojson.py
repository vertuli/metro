from update_routes import update_routes
from update_paths import update_paths
from read_geojson import read_geojson


def test_update_routes():
    routes = update_routes.update_routes("lametro-rail")
    assert len(routes) > 0


def test_update_paths():
    paths = update_paths.update_paths("lametro-rail")
    assert len(paths) > 0


def test_get_active_paths():
    paths = read_geojson.get_active_paths("lametro-rail")
    assert len(paths) > 0


def test_get_all_paths_by_iso_date():
    paths = read_geojson.get_all_paths_by_iso_date("lametro-rail", "2020-05-24")
    assert len(paths) > 0


def test_get_paths():
    paths = read_geojson.get_paths("lametro-rail", "2020-05-24", "801")
    assert len(paths) > 0
