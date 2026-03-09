def test_worker_module_imports():
    import importlib
    mod = importlib.import_module("labeler.worker")
    assert hasattr(mod, "main")
    assert callable(mod.main)
