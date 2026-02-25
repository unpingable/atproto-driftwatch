import pytest
pytest.importorskip("fastapi")

def test_sim_labeler_importable():
    import importlib
    mod = importlib.import_module("labeler.sim_labeler")
    assert hasattr(mod, "app")
    assert callable(getattr(mod, "app")) or isinstance(mod.app, object)
