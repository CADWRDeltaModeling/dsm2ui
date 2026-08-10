"""Tests for dsm2ui.calib.calib_config_builder.

Covers ``_unique_study_labels`` (leaf-folder-name collision disambiguation) and
its integration into ``build_calib_config`` -- see the "setup-compare ends up
with only one study" bug: two study folders that share a leaf directory name
but differ in their parent directories used to collapse into a single
``study_files_dict`` key.
"""

import yaml

from dsm2ui.calib.calib_config_builder import _unique_study_labels, build_calib_config


def _make_study(tmp_path, *parts, modifier="run1"):
    """Create a minimal fake study folder with an output/hydro_echo.inp file."""
    study_dir = tmp_path.joinpath(*parts)
    output_dir = study_dir / "output"
    output_dir.mkdir(parents=True)
    echo_file = output_dir / "hydro_echo.inp"
    echo_file.write_text(
        "ENVVAR\n"
        "NAME VALUE\n"
        f"DSM2MODIFIER {modifier}\n"
        "START_DATE 01OCT2020\n"
        "END_DATE 30SEP2022\n"
        "END\n"
    )
    return study_dir


class TestUniqueStudyLabels:
    def test_no_collision_uses_leaf_name(self, tmp_path):
        folder_a = _make_study(tmp_path, "baseline")
        folder_b = _make_study(tmp_path, "alternative")
        labels = _unique_study_labels([str(folder_a), str(folder_b)])
        assert labels == ["baseline", "alternative"]

    def test_common_leaf_name_disambiguated_by_parent(self, tmp_path):
        folder_a = tmp_path / "scenario1" / "v821"
        folder_b = tmp_path / "scenario2" / "v821"
        folder_a.mkdir(parents=True)
        folder_b.mkdir(parents=True)
        labels = _unique_study_labels([str(folder_a), str(folder_b)])
        assert len(set(labels)) == 2
        assert labels == ["scenario1_v821", "scenario2_v821"]

    def test_still_colliding_after_one_parent_level_goes_deeper(self, tmp_path):
        # Both share leaf AND immediate parent name; only grandparent differs.
        folder_a = tmp_path / "planA" / "output" / "v821"
        folder_b = tmp_path / "planB" / "output" / "v821"
        folder_a.mkdir(parents=True)
        folder_b.mkdir(parents=True)
        labels = _unique_study_labels([str(folder_a), str(folder_b)])
        assert len(set(labels)) == 2

    def test_identical_folder_passed_twice_does_not_hang(self, tmp_path):
        folder = tmp_path / "only_one"
        folder.mkdir()
        # Same folder twice can never be disambiguated -- must not loop forever.
        labels = _unique_study_labels([str(folder), str(folder)])
        assert len(labels) == 2


class TestBuildCalibConfigStudyLabels:
    def test_colliding_leaf_names_produce_two_study_entries(self, tmp_path):
        folder_a = _make_study(tmp_path, "scenario1", "v821", modifier="scenario1run")
        folder_b = _make_study(tmp_path, "scenario2", "v821", modifier="scenario2run")

        output_file = tmp_path / "compare_config.yml"
        result_path = build_calib_config(
            study_folders=[str(folder_a), str(folder_b)],
            postprocessing_folder=None,
            output_file=str(output_file),
            module="hydro",
            observed_files=None,
        )

        with open(result_path) as f:
            config = yaml.safe_load(f)

        # Regression: previously both folders resolved to the leaf name "v821",
        # collapsing study_files_dict to a single entry.
        assert len(config["study_files_dict"]) == 2
        assert len(config["postpro_model_dict"]) == 2
