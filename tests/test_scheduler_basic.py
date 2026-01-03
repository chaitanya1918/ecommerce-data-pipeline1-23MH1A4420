from scripts.scheduler import scheduler_for_test

def test_scheduler_runs():
    assert scheduler_for_test() is True
