"""Unit tests for logbook campaign_goal validation."""

import pytest

from server.logbooks import CAMPAIGN_GOAL_MAX_CHARS, validate_campaign_goal


@pytest.mark.unit
class TestValidateCampaignGoal:
    def test_empty(self):
        assert validate_campaign_goal(None) == ""
        assert validate_campaign_goal("") == ""
        assert validate_campaign_goal("  \n  ") == ""

    def test_strips(self):
        assert validate_campaign_goal("  hello  ") == "hello"

    def test_too_long(self):
        with pytest.raises(ValueError, match="at most"):
            validate_campaign_goal("x" * (CAMPAIGN_GOAL_MAX_CHARS + 1))

    def test_non_string(self):
        with pytest.raises(ValueError, match="must be a string"):
            validate_campaign_goal(42)
