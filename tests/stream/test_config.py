"""The configuration loader (task S section 1)."""
import dataclasses
import os
import tomllib
from pathlib import Path

import pytest

from cdc_v2 import config
from cdc_v2.config import ConfigError

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config" / "example.toml"
PLANTED = "plantedmarker"  # the tests check that no message holds it
ENV = {"PG_HOST": "localhost", "PG_PORT": "55664", "PG_DATABASE": "db", "PG_USER": "cdc_v2_runtime",
       "PG_PASSWORD": PLANTED, "PG_REPLICA_HOST": "localhost", "PG_REPLICA_PORT": "55665"}


def _doc():
    with open(EXAMPLE, "rb") as f:
        return tomllib.load(f)


def test_the_example_sets_every_key_and_loads():
    doc = _doc()
    set_keys = set()
    for section, (_cls, keys) in config.SECTIONS.items():
        if section == "source.nodes.*":
            for name, t in doc["source"]["nodes"].items():
                set_keys |= {"source.nodes.<name>.%s" % k for k in t}
            continue
        table = doc
        for part in ("apply" if section == "apply.group" else section).split("."):
            table = table[part]
        shown = "apply" if section == "apply.group" else section
        set_keys |= {"%s.%s" % (shown, k) for k in table if k in keys}
    missing = [k for k in config.documented_keys() if k not in set_keys and not
               (k.startswith("source.nodes.<name>.") and (k[len("source.nodes.<name>."):] in ("host", "port", "dbname",
                                                                                              "user")))]
    assert not missing, missing
    cfg = config.load(EXAMPLE, ENV)
    assert cfg.stream.node == "replica" and cfg.engine.port == 55665 and cfg.engine.password == ""
    assert cfg.source.nodes["primary"].password.reveal() == PLANTED     # a callable reads it when connecting
    assert cfg.group.max_group_delay_s == 1.0 and cfg.run.max_runtime_s == 7.5 * 3600
    assert cfg.engine.heap_mb == 512 and cfg.engine.frame_max_bytes == (1 << 30) + (16 << 20)


def test_defaults_of_the_example_are_the_dataclass_defaults():
    cfg = config.load(EXAMPLE, ENV)
    for name in ("group", "stream", "supervisor", "leader"):
        obj = getattr(cfg, name)
        default = type(obj)(**({"slot": obj.slot} if name == "stream" else {}))
        assert obj == default, name


def test_every_settings_field_is_a_key_or_derived():
    for section, (cls, keys) in config.SECTIONS.items():
        fields = {f.name for f in dataclasses.fields(cls)}
        mapped = {field for field, _kind in keys.values()}
        derived = {f for (s, f) in config.DERIVED if s == section}
        if section == "apply":
            mapped |= {"local_temp_directory", "local_max_temp_directory_size"}
        unknown = fields - mapped - derived
        assert not unknown, (section, sorted(unknown))


def test_the_document_lists_every_key_and_is_generated():
    text = (ROOT / "docs" / "configuration.md").read_text()
    assert text == config.render_document(), "run: uv run python -m cdc_v2.config > docs/configuration.md"
    for k in config.documented_keys():
        assert "`%s`" % k in text, k


@pytest.mark.parametrize("edit,where", [
    (lambda d: d["stream"].update(no_such_key=1), "stream.no_such_key"),
    (lambda d: d["stream"].update(marker_timeout=60), "stream.marker_timeout"),          # a plain number
    (lambda d: d["stream"].update(marker_timeout="60 s"), "stream.marker_timeout"),
    (lambda d: d["apply"].update(max_group_bytes="128MB"), "apply.max_group_bytes"),
    (lambda d: d["apply"].update(max_group_attempts=0), "apply"),
    (lambda d: d["leader"].update(heartbeat_interval="15s"), "leader"),                    # interval >= stale / 3
    (lambda d: d["stream"].update(attach_timeout="30s"), "stream.attach_timeout"),
    (lambda d: d["stream"].update(heartbeat_interval="90s"), "stream"),
    (lambda d: d["supervisor"].update(restart_after="10s"), "supervisor.restart_after"),
    (lambda d: d.update(nonsense={}), "[nonsense]"),
    (lambda d: d["source"]["nodes"]["primary"].update(password=PLANTED), "source.nodes.primary.password"),
    (lambda d: d["source"]["nodes"]["primary"].update(password_env="NOT_SET_ANYWHERE"), "password_env"),
    (lambda d: d["source"]["nodes"]["primary"].update(port="5432"), "source.nodes.*.port"),
    (lambda d: d["engine"].update(heap=512), "engine.heap"),
])
def test_a_bad_file_is_refused_with_the_key_and_without_a_secret(edit, where):
    doc = _doc()
    edit(doc)
    with pytest.raises(ConfigError) as e:
        config.from_dict(doc, ENV)
    text = str(e.value)
    assert where in text, text
    assert PLANTED not in text and PLANTED not in repr(e.value)


def test_a_missing_variable_is_named_without_values():
    env = dict(ENV)
    del env["PG_REPLICA_HOST"]
    with pytest.raises(ConfigError) as e:
        config.load(EXAMPLE, env)
    assert "PG_REPLICA_HOST" in str(e.value) and PLANTED not in str(e.value)


def test_durations_and_sizes_have_one_spelling():
    assert config.duration("7h30m", "k") == 27000.0
    assert config.duration("250ms", "k") == 0.25
    assert config.size("512MiB", "k") == 512 << 20
    for bad in ("30", 30, "30 s", "1d", "", "s"):
        with pytest.raises(ConfigError):
            config.duration(bad, "k")
    for bad in ("512", 512, "512MB", "1.5GiB", "GiB"):
        with pytest.raises(ConfigError):
            config.size(bad, "k")
