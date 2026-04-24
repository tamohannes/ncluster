"""Unit tests for server/team.py CRUD (team_members + ppp_accounts)."""

import pytest

from server.db import init_db
from server.team import (
    add_ppp_account,
    add_team_member,
    get_ppp_account,
    get_team_member,
    list_ppp_account_names,
    list_ppp_accounts,
    list_team_members,
    list_team_usernames,
    ppp_id_map,
    remove_ppp_account,
    remove_team_member,
    reorder_ppp_accounts,
    reorder_team_members,
    update_ppp_account,
    update_team_member,
)


@pytest.fixture(autouse=True)
def _init_team_db(_isolate_db):
    init_db()


@pytest.mark.unit
class TestAddTeamMember:
    def test_basic_round_trip(self):
        result = add_team_member("testuser", display_name="Test User", email="test@example.com")
        assert result["status"] == "ok"
        m = result["member"]
        assert m["username"] == "testuser"
        assert m["display_name"] == "Test User"
        assert m["email"] == "test@example.com"

    def test_username_only(self):
        result = add_team_member("alice")
        assert result["status"] == "ok"
        assert result["member"]["display_name"] == ""
        assert result["member"]["email"] == ""

    def test_duplicate_rejected(self):
        add_team_member("alice")
        result = add_team_member("alice")
        assert result["status"] == "error"
        assert "already exists" in result["error"]

    def test_empty_username_rejected(self):
        assert add_team_member("")["status"] == "error"

    def test_invalid_username_rejected(self):
        for bad in ["1user", "with space", "with!bang", "-leadhyphen"]:
            assert add_team_member(bad)["status"] == "error", f"{bad!r} should be rejected"

    def test_dotted_username_allowed(self):
        # Common in some institutional usernames (first.last)
        assert add_team_member("alice.smith")["status"] == "ok"

    def test_position_auto_increments(self):
        add_team_member("a")
        add_team_member("b")
        add_team_member("c")
        members = list_team_members()
        assert [m["username"] for m in members] == ["a", "b", "c"]
        assert [m["position"] for m in members] == [0, 1, 2]


@pytest.mark.unit
class TestUpdateTeamMember:
    def test_update_display_name(self):
        add_team_member("alice")
        update_team_member("alice", display_name="Alice Smith")
        assert get_team_member("alice")["display_name"] == "Alice Smith"

    def test_update_email(self):
        add_team_member("alice")
        update_team_member("alice", email="alice@example.com")
        assert get_team_member("alice")["email"] == "alice@example.com"

    def test_update_missing_rejected(self):
        result = update_team_member("ghost", email="g@x")
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_no_op_returns_existing(self):
        add_team_member("alice")
        result = update_team_member("alice")
        assert result["status"] == "ok"


@pytest.mark.unit
class TestRemoveTeamMember:
    def test_remove_round_trip(self):
        add_team_member("alice")
        result = remove_team_member("alice")
        assert result["status"] == "ok"
        assert result["removed"] == "alice"
        assert get_team_member("alice") is None

    def test_remove_missing_rejected(self):
        assert remove_team_member("ghost")["status"] == "error"


@pytest.mark.unit
class TestListTeam:
    def test_empty(self):
        assert list_team_members() == []
        assert list_team_usernames() == []

    def test_ordered_by_position(self):
        add_team_member("c", position=2)
        add_team_member("a", position=0)
        add_team_member("b", position=1)
        assert list_team_usernames() == ["a", "b", "c"]


@pytest.mark.unit
class TestReorderTeamMembers:
    def test_reorder_round_trip(self):
        add_team_member("a")
        add_team_member("b")
        add_team_member("c")
        reorder_team_members(["c", "a", "b"])
        assert list_team_usernames() == ["c", "a", "b"]

    def test_reorder_unknown_user_errors(self):
        add_team_member("a")
        result = reorder_team_members(["a", "ghost"])
        assert result["status"] == "error"


# ─── PPP accounts ──────────────────────────────────────────────────────────

@pytest.mark.unit
class TestAddPppAccount:
    def test_basic_round_trip(self):
        result = add_ppp_account("test_ppp_account", ppp_id="10595", description="primary PPP")
        assert result["status"] == "ok"
        a = result["account"]
        assert a["name"] == "test_ppp_account"
        assert a["ppp_id"] == "10595"

    def test_id_optional(self):
        result = add_ppp_account("alpha")
        assert result["status"] == "ok"
        assert result["account"]["ppp_id"] == ""

    def test_id_coerced_to_string(self):
        result = add_ppp_account("alpha", ppp_id=12345)
        assert result["account"]["ppp_id"] == "12345"

    def test_duplicate_rejected(self):
        add_ppp_account("alpha")
        result = add_ppp_account("alpha")
        assert result["status"] == "error"
        assert "already exists" in result["error"]

    def test_invalid_name_rejected(self):
        for bad in ["", "1abc", "with space", "-lead"]:
            assert add_ppp_account(bad)["status"] == "error"


@pytest.mark.unit
class TestUpdatePppAccount:
    def test_update_id(self):
        add_ppp_account("alpha", ppp_id="111")
        update_ppp_account("alpha", ppp_id="222")
        assert get_ppp_account("alpha")["ppp_id"] == "222"

    def test_update_description(self):
        add_ppp_account("alpha")
        update_ppp_account("alpha", description="primary")
        assert get_ppp_account("alpha")["description"] == "primary"

    def test_update_missing_rejected(self):
        assert update_ppp_account("ghost", ppp_id="1")["status"] == "error"


@pytest.mark.unit
class TestPppIdMap:
    def test_returns_only_accounts_with_ids(self):
        add_ppp_account("withid", ppp_id="100")
        add_ppp_account("nopid")
        m = ppp_id_map()
        assert m == {"withid": "100"}


@pytest.mark.unit
class TestPppListing:
    def test_empty(self):
        assert list_ppp_accounts() == []
        assert list_ppp_account_names() == []

    def test_ordered_by_position(self):
        add_ppp_account("c", position=2)
        add_ppp_account("a", position=0)
        add_ppp_account("b", position=1)
        assert list_ppp_account_names() == ["a", "b", "c"]

    def test_reorder_round_trip(self):
        add_ppp_account("a")
        add_ppp_account("b")
        add_ppp_account("c")
        reorder_ppp_accounts(["c", "b", "a"])
        assert list_ppp_account_names() == ["c", "b", "a"]


@pytest.mark.unit
class TestRemovePpp:
    def test_remove_round_trip(self):
        add_ppp_account("alpha")
        result = remove_ppp_account("alpha")
        assert result["status"] == "ok"
        assert get_ppp_account("alpha") is None

    def test_remove_missing_rejected(self):
        assert remove_ppp_account("ghost")["status"] == "error"
