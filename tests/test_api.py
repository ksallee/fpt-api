import os

os.environ["FPT_BASE_CLASS"] = "mockgun"

import pytest
from shotgun_api3.shotgun import ServerCapabilities
from shotgun_api3.lib.mockgun import Shotgun as Mockgun

# Set schema paths for Mockgun
schema_path = os.path.join(os.path.dirname(__file__), "resources", "schema.pickle")
schema_entity_path = os.path.join(os.path.dirname(__file__), "resources", "schema_entity.pickle")
Mockgun.set_schema_paths(schema_path, schema_entity_path)


@pytest.fixture(scope="session")
def fpt():
    """Create mockgun instance with real schema."""
    from fpt_api import FPT
    mock = FPT("https://test.shotgunstudio.com", "script", "script_key", connect=False)

    # Set up server capabilities
    mock._server_caps = ServerCapabilities(
        host=mock.config.server,
        meta={
            "version": [9, 0, 0],
            "api_max_entities_per_page": 500,
            "api_max_entities_total": 10000,
            "user_authentication_method": "default"
        }
    )

    # Create test shots
    for i in range(10):
        mock.create("Shot", {
            "code": f"bunny_010_{i:02d}",
            "description": "A shot",
        })

    # Create test assets
    assets = {
        "Flash": ("Very fast asset", "ip"),
        "Hamster": ("Small furry asset", "fin"),
        "Anders": ("Mysterious asset", "wtg"),
        "Buck": ("Strong asset", "omt"),
        "JoJo": ("Adventure asset", "hld"),
    }

    for name, (description, status) in assets.items():
        mock.create("Asset", {
            "code": name,
            "description": description,
            "sg_status_list": status,
            "shots": [{"type": "Shot", "id": 10}]
        })

    return mock


def test_find(fpt):
    """Test find method with query fields."""
    # Test basic find
    shots = fpt.find(
        "Shot",
        [["id", "in", [9, 10]]],
        # TODO: most of these fields don't work with mockgun:
        #       - if it's a field that needs a call to summarize, it's not implemented in mockgun.
        #       - mockgun doesn't care about limit and order...
        # [
        #     "code",
        #     "sg_assets",
        #     "sg_assets_count",
        #     "sg_one_asset",
        #     "sg_two_assets_description",
        #     "sg_assets_status_percentage",
        #     "sg_code_and_description"
        # ]
        ["code", "sg_assets"]
    )
    assert len(shots) == 2
    assert shots[0]["code"] == "bunny_010_08"
    assert shots[1]["code"] == "bunny_010_09"
    assert shots[0]["sg_assets"] == ""
    assert shots[1]["sg_assets"] == "Anders, Buck, Flash, Hamster, JoJo"
    # assert shots[0]["sg_code_and_description"] == "bunny_010_08 A shot"
    # assert shots[1]["sg_code_and_description"] == "bunny_010_09 A shot"
    # assert shots[0]["sg_two_assets_description"] == ""
    # assert shots[1]["sg_two_assets_description"] == "Mysterious asset, Strong asset, Very fast asset, Small furry asset, Adventure asset"
    # assert shots[0]["sg_assets_count"] == "0"
    # assert shots[1]["sg_assets_count"] == "5"
    # assert shots[0]["sg_one_asset"] == ""
    # assert shots[1]["sg_one_asset"] == "Flash"
    # assert shots[0]["sg_assets_status_percentage"] == ""
    # assert shots[1]["sg_assets_status_percentage"] == "20% ip"


def test_find_one(fpt):
    """Test find_one method with query fields."""
    # Test basic find_one
    shot = fpt.find_one(
        "Shot",
        [["id", "is", 10]],
        ["code"]
    )
    assert shot["code"] == "bunny_010_09"

    # Test find_one with query fields
    shot = fpt.find_one(
        "Shot",
        [["id", "is", 10]],
        # TODO: most of these fields don't work with mockgun:
        #       - if it's a field that needs a call to summarize, it's not implemented in mockgun.
        #       - mockgun doesn't care about limit and order...
        # [
        #     "code",
        #     "sg_assets",
        #     "sg_assets_count",
        #     "sg_one_asset",
        #     "sg_two_assets_description",
        #     "sg_assets_status_percentage",
        #     "sg_code_and_description"
        # ]
        ["code", "sg_assets"]
    )
    assert shot["sg_assets"] == "Anders, Buck, Flash, Hamster, JoJo"
    # assert shot["sg_code_and_description"] == "bunny_010_09 A shot"
    # assert shot["sg_two_assets_description"] == "Mysterious asset, Strong asset, Very fast asset, Small furry asset, Adventure asset"
    # assert shot["sg_assets_count"] == "5"
    # assert shot["sg_one_asset"] == "Flash"
    # assert shot["sg_assets_status_percentage"] == "20% ip"


def test_query_field_already_filled(fpt):
    """A query field the server already returned is kept, not computed again."""
    shot = fpt.create("Shot", {"code": "bunny_020_00"})
    fpt._db["Shot"][shot["id"]]["sg_assets"] = "filled by the server"
    found = fpt.find_one("Shot", [["id", "is", shot["id"]]], ["code", "sg_assets"])
    assert found["sg_assets"] == "filled by the server"
