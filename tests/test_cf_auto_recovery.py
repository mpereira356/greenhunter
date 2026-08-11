import app.services.scraper as scraper


def _reset_state():
    scraper._CF_CONSECUTIVE_CHALLENGES = 0
    scraper._CF_RESTART_SCHEDULED = False


def test_third_cloudflare_challenge_schedules_one_restart(monkeypatch):
    _reset_state()
    scheduled = []
    notices = []
    monkeypatch.setenv("BETSAPI_CF_AUTO_RESTART_AFTER", "3")
    monkeypatch.setattr(scraper, "_schedule_worker_restart", scheduled.append)
    monkeypatch.setattr(scraper, "_send_cf_recovery_alert", lambda url, count: notices.append((url, count)))

    assert scraper._record_cf_challenge("https://example/1") is False
    assert scraper._record_cf_challenge("https://example/2") is False
    assert scraper._record_cf_challenge("https://example/3") is True
    assert scraper._record_cf_challenge("https://example/4") is False
    assert scheduled == [2.0]
    assert notices == [("https://example/3", 3)]


def test_cloudflare_auto_restart_can_be_disabled(monkeypatch):
    _reset_state()
    scheduled = []
    monkeypatch.setenv("BETSAPI_CF_AUTO_RESTART_AFTER", "0")
    monkeypatch.setattr(scraper, "_schedule_worker_restart", scheduled.append)

    for _ in range(5):
        assert scraper._record_cf_challenge("https://example") is False
    assert scheduled == []


def test_stuck_first_challenge_forces_recovery(monkeypatch):
    _reset_state()
    scheduled = []
    notices = []
    monkeypatch.setenv("BETSAPI_CF_RECYCLE_ON_TIMEOUT", "1")
    monkeypatch.setattr(scraper, "_schedule_worker_restart", scheduled.append)
    monkeypatch.setattr(scraper, "_send_cf_recovery_alert", lambda url, count: notices.append((url, count)))

    assert scraper._force_cf_timeout_recovery("https://example/stuck") is True
    assert scraper._force_cf_timeout_recovery("https://example/stuck-again") is False
    assert scheduled == [2.0]
    assert notices == [("https://example/stuck", 1)]
