"""Tests for dsm2ui.calib.calibplot pure-function helpers and data-masking logic.

No real files are required — only synthetic pandas DataFrames and the
in-process HoloViews/hvplot extension.
"""

import numpy as np
import pandas as pd
import pytest
import holoviews as hv

from dsm2ui.calib.calibplot import (
    _smart_title,
    _normalize_timewindow,
    parse_time_window,
    time_window_exclusion,
    threshold_exclusion,
    time_window_and_threshold_exclusion,
    tsplot,
    scatterplot,
    calculate_metrics,
)

# Initialise once per session — required before any Curve/Overlay is constructed.
hv.extension("bokeh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n=30, start="2020-01-01", col="value", seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {col: rng.random(n)},
        index=pd.date_range(start, periods=n, freq="D"),
    )


# ---------------------------------------------------------------------------
# _smart_title
# ---------------------------------------------------------------------------

class TestSmartTitle:
    def test_all_caps_long_converted(self):
        assert _smart_title("FLOW") == "Flow"
        assert _smart_title("STAGE") == "Stage"
        assert _smart_title("DISPERSION") == "Dispersion"

    def test_short_abbreviation_unchanged(self):
        assert _smart_title("EC") == "EC"
        assert _smart_title("DO") == "DO"

    def test_mixed_case_unchanged(self):
        assert _smart_title("Flow") == "Flow"
        assert _smart_title("stage") == "stage"

    def test_non_string_passthrough(self):
        assert _smart_title(None) is None
        assert _smart_title(42) == 42


# ---------------------------------------------------------------------------
# _normalize_timewindow
# ---------------------------------------------------------------------------

class TestNormalizeTimewindow:
    def test_none_returns_none(self):
        assert _normalize_timewindow(None) is None

    def test_already_normalized_passthrough(self):
        tw = "2015-03-01:2024-09-30"
        assert _normalize_timewindow(tw) == tw

    def test_dsm2_format_converted(self):
        result = _normalize_timewindow("01MAR2015 - 30SEP2024")
        assert result == "2015-03-01:2024-09-30"

    def test_dsm2_jan_dec_roundtrip(self):
        result = _normalize_timewindow("01JAN2020 - 31DEC2020")
        assert result.startswith("2020-01-01")
        assert result.endswith("2020-12-31")

    def test_already_normalized_with_colon_not_touched(self):
        tw = "2000-01-01:2000-12-31"
        assert _normalize_timewindow(tw) == tw


# ---------------------------------------------------------------------------
# parse_time_window
# ---------------------------------------------------------------------------

class TestParseTimeWindow:
    def test_returns_two_date_lists(self):
        result = parse_time_window("2020-01-01:2020-12-31")
        assert result == [[2020, 1, 1], [2020, 12, 31]]

    def test_bad_input_returns_empty_list(self):
        result = parse_time_window("not-a-timewindow")
        assert result == []

    def test_none_input_returns_empty_list(self):
        result = parse_time_window(None)
        assert result == []

    def test_dsm2_format_accepted(self):
        result = parse_time_window("01MAR2015 - 30SEP2024")
        assert len(result) == 2
        assert result[0][0] == 2015
        assert result[1][0] == 2024

    def test_mid_year_dates(self):
        result = parse_time_window("2019-06-15:2021-03-20")
        assert result == [[2019, 6, 15], [2021, 3, 20]]


# ---------------------------------------------------------------------------
# time_window_exclusion
# ---------------------------------------------------------------------------

class TestTimeWindowExclusion:
    def setup_method(self):
        self.df = _make_df(n=30, start="2020-01-01")

    def test_in_window_rows_flagged_false(self):
        result = time_window_exclusion(self.df, "2020-01-05_2020-01-10")
        assert "keep_tw" in result.columns
        mask = (result.index >= "2020-01-05") & (result.index < "2020-01-10")
        assert not result.loc[mask, "keep_tw"].any()
        assert result.loc[~mask, "keep_tw"].all()

    def test_invert_selection_flips_flags(self):
        result = time_window_exclusion(
            self.df, "2020-01-05_2020-01-10", invert_selection=True
        )
        mask = (result.index >= "2020-01-05") & (result.index < "2020-01-10")
        assert result.loc[mask, "keep_tw"].all()
        assert not result.loc[~mask, "keep_tw"].any()

    def test_none_exclusion_all_true(self):
        result = time_window_exclusion(self.df, None)
        assert result["keep_tw"].all()

    def test_empty_string_exclusion_all_true(self):
        result = time_window_exclusion(self.df, "")
        assert result["keep_tw"].all()

    def test_multiple_windows_both_flagged(self):
        result = time_window_exclusion(
            self.df, "2020-01-03_2020-01-06,2020-01-15_2020-01-18"
        )
        assert not result.loc["2020-01-03":"2020-01-05", "keep_tw"].any()
        assert not result.loc["2020-01-15":"2020-01-17", "keep_tw"].any()
        assert result.loc["2020-01-10":"2020-01-12", "keep_tw"].all()

    def test_invert_none_exclusion_all_false(self):
        result = time_window_exclusion(self.df, None, invert_selection=True)
        assert not result["keep_tw"].any()


# ---------------------------------------------------------------------------
# threshold_exclusion
# ---------------------------------------------------------------------------

class TestThresholdExclusion:
    def setup_method(self):
        self.df = _make_df(n=20, start="2020-01-01")
        self.df.iloc[5, 0] = 999.0
        self.df.iloc[10, 0] = 500.0

    def test_above_threshold_flagged_false(self):
        result = threshold_exclusion(self.df, self.df.copy(), upper_threshold=100.0)
        assert "keep_threshold" in result.columns
        assert not result.iloc[5]["keep_threshold"]
        assert not result.iloc[10]["keep_threshold"]
        assert result.iloc[0]["keep_threshold"]

    def test_none_threshold_all_kept(self):
        result = threshold_exclusion(self.df, self.df.copy(), upper_threshold=None)
        assert result["keep_threshold"].all()

    def test_invert_keeps_only_above_threshold(self):
        result = threshold_exclusion(
            self.df, self.df.copy(), upper_threshold=100.0, invert_selection=True
        )
        assert result.iloc[5]["keep_threshold"]
        assert result.iloc[10]["keep_threshold"]
        assert not result.iloc[0]["keep_threshold"]

    def test_returns_copy_not_original(self):
        df_copy = self.df.copy()
        threshold_exclusion(df_copy, self.df.copy(), upper_threshold=100.0)
        # Original should not gain keep_threshold column
        assert "keep_threshold" not in self.df.columns


# ---------------------------------------------------------------------------
# time_window_and_threshold_exclusion
# ---------------------------------------------------------------------------

class TestTimeWindowAndThresholdExclusion:
    def setup_method(self):
        self.df = _make_df(n=30, start="2020-01-01")
        self.df.iloc[20, 0] = 999.0  # high value outside the tw exclusion range

    def test_helper_columns_removed(self):
        result = time_window_and_threshold_exclusion(
            self.df, self.df.copy(), "2020-01-05_2020-01-10"
        )
        assert "keep_tw" not in result.columns
        assert "keep_threshold" not in result.columns

    def test_excluded_tw_rows_are_nan(self):
        result = time_window_and_threshold_exclusion(
            self.df, self.df.copy(), "2020-01-05_2020-01-10"
        )
        mask = (result.index >= "2020-01-05") & (result.index < "2020-01-10")
        assert result.loc[mask].isnull().values.all()

    def test_above_threshold_set_to_nan(self):
        result = time_window_and_threshold_exclusion(
            self.df, self.df.copy(), None, upper_threshold=100.0
        )
        assert pd.isna(result.iloc[20, 0])
        assert not pd.isna(result.iloc[0, 0])

    def test_no_exclusion_preserves_non_nan_values(self):
        result = time_window_and_threshold_exclusion(
            self.df, self.df.copy(), None
        )
        # Nothing is NaN because no time window and default threshold is 999999
        assert result.notna().values.all()


# ---------------------------------------------------------------------------
# tsplot
# ---------------------------------------------------------------------------

class TestTsplot:
    def setup_method(self):
        self.df1 = _make_df(n=30, start="2020-01-01", col="obs")
        self.df2 = _make_df(n=30, start="2020-01-01", col="model", seed=1)

    def test_returns_overlay(self):
        result = tsplot([self.df1, self.df2], ["obs", "model"])
        assert isinstance(result, hv.Overlay)

    def test_overlay_element_count(self):
        result = tsplot([self.df1, self.df2], ["obs", "model"])
        assert len(result) == 2

    def test_none_entry_does_not_raise(self):
        result = tsplot([self.df1, None], ["obs", "missing"])
        assert isinstance(result, hv.Overlay)

    def test_with_timewindow_zoom(self):
        result = tsplot(
            [self.df1, self.df2],
            ["obs", "model"],
            timewindow="2020-01-05:2020-01-20",
            zoom_inst_plot=True,
        )
        assert isinstance(result, hv.Overlay)

    def test_with_dsm2_format_timewindow(self):
        result = tsplot(
            [self.df1, self.df2],
            ["obs", "model"],
            timewindow="01JAN2020 - 30JAN2020",
            zoom_inst_plot=True,
        )
        assert isinstance(result, hv.Overlay)


# ---------------------------------------------------------------------------
# scatterplot
# ---------------------------------------------------------------------------

class TestScatterplot:
    def setup_method(self):
        self.df1 = _make_df(n=30, start="2020-01-01", col="obs")
        self.df2 = _make_df(n=30, start="2020-01-01", col="model", seed=1)

    def test_returns_without_error(self):
        result = scatterplot([self.df1, self.df2], ["obs", "model"])
        assert result is not None

    def test_alternate_index_x(self):
        result = scatterplot([self.df1, self.df2], ["obs", "model"], index_x=1)
        assert result is not None


# ---------------------------------------------------------------------------
# calculate_metrics
# ---------------------------------------------------------------------------

class TestCalculateMetrics:
    def setup_method(self):
        n = 50
        rng = np.random.default_rng(42)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        obs = rng.random(n)
        model = obs + rng.normal(0, 0.05, n)
        self.df_obs = pd.DataFrame({"obs": obs}, index=idx)
        self.df_model = pd.DataFrame({"model": model}, index=idx)

    def test_returns_dataframe(self):
        result = calculate_metrics([self.df_obs, self.df_model], ["obs", "model"])
        assert isinstance(result, pd.DataFrame)

    def test_expected_columns_present(self):
        result = calculate_metrics([self.df_obs, self.df_model], ["obs", "model"])
        expected = {
            "regression_slope", "r2", "rmse", "nash_sutcliffe", "kling_gupta",
            "mean_error", "percent_bias",
        }
        assert expected.issubset(set(result.columns))

    def test_good_match_slope_near_one(self):
        result = calculate_metrics([self.df_obs, self.df_model], ["obs", "model"])
        assert abs(result.iloc[0]["regression_slope"] - 1.0) < 0.2

    def test_empty_series_returns_none(self):
        empty = pd.DataFrame({"obs": []}, index=pd.DatetimeIndex([]))
        result = calculate_metrics([empty, empty], ["obs", "model"])
        assert result is None
