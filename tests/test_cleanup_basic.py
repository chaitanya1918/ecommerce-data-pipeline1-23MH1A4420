from scripts.cleanup_old_data import cleanup_for_test

def test_cleanup_runs():
    assert cleanup_for_test() is True
