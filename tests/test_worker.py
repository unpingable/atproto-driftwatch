def test_worker_module_imports():
    # This test requires DuckDB to be installed because the worker imports DB helpers.
    import pytest
    pytest.importorskip("duckdb")
    import importlib
    mod = importlib.import_module("labeler.worker")
    assert hasattr(mod, "main")
    assert callable(mod.main)
