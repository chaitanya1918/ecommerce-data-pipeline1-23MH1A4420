from scripts.pipeline_orchestrator import run_pipeline_for_test

def test_pipeline_runs():
    result = run_pipeline_for_test()
    assert result["status"] == "success"
