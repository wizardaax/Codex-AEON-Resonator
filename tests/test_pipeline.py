"""
tests/test_pipeline.py

Pytest suite for Codex-AEON-Resonator pipeline.
================================================
Author:  Adam Snellman | Brisbane, Australia | 2026
Repo:    https://github.com/wizardaax/Codex-AEON-Resonator

Run:
    python -m pytest tests/ -v

Design constraints (100-year contract):
- stdlib + pytest only for the pure-Python layer.
- numpy / scipy / networkx gated via pytest.importorskip so the suite
  degrades gracefully on a bare Python install rather than error-bombing.
- No mocking of pure functions; real calls with known inputs.
- No deletions; no network I/O; no file writes.
"""

from __future__ import annotations

import sys
import os
import io

import pytest

# ---------------------------------------------------------------------------
# Make sure the repo root is on sys.path regardless of how pytest is invoked
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# ===========================================================================
#  SECTION 1 — extraction_topology.py
#  Requires: networkx  (skipped gracefully if absent)
# ===========================================================================

class TestExtractionTopologyImport:
    """Module-level smoke tests that do not require networkx."""

    def test_constants_are_strings(self):
        """LEADER / MID_TIER / BASE / EDGE_* must all be non-empty strings."""
        nx_mod = pytest.importorskip("networkx")  # noqa: F841  (only needed for import guard)
        from pipeline.extraction_topology import (
            LEADER, MID_TIER, BASE,
            EDGE_COMMAND, EDGE_INSULATION, EDGE_EXTRACTION, EDGE_LOYALTY,
        )
        for val in (LEADER, MID_TIER, BASE,
                    EDGE_COMMAND, EDGE_INSULATION, EDGE_EXTRACTION, EDGE_LOYALTY):
            assert isinstance(val, str) and val, f"Expected non-empty str, got {val!r}"

    def test_verified_instances_length(self):
        """VERIFIED_INSTANCES must contain exactly 7 historical records."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import VERIFIED_INSTANCES
        assert len(VERIFIED_INSTANCES) == 7

    def test_verified_instances_ids_unique(self):
        """Every instance id must be unique."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import VERIFIED_INSTANCES
        ids = [inst.id for inst in VERIFIED_INSTANCES]
        assert len(ids) == len(set(ids)), "Duplicate instance ids found"

    def test_historical_instance_fields_non_empty(self):
        """Every HistoricalInstance must have non-empty required string fields."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import VERIFIED_INSTANCES
        required = ("id", "period", "label_leader", "label_mid_tier",
                    "label_base", "scarce_resource", "commitment_signal",
                    "insulation_mechanism")
        for inst in VERIFIED_INSTANCES:
            for field in required:
                val = getattr(inst, field)
                assert isinstance(val, str) and val, (
                    f"Instance {inst.id!r}: field {field!r} is empty or non-string"
                )


class TestExtractionTopologyBuild:
    """ExtractionTopology.build() — structure correctness."""

    def test_canonical_build_node_count(self):
        """Canonical graph must have exactly 3 nodes."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        topo = ExtractionTopology.build()
        assert topo.graph.number_of_nodes() == 3

    def test_canonical_build_edge_count(self):
        """Canonical graph must have exactly 4 directed edges."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        topo = ExtractionTopology.build()
        assert topo.graph.number_of_edges() == 4

    def test_canonical_build_instance_is_none(self):
        """build() with no args must leave instance as None."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        topo = ExtractionTopology.build()
        assert topo.instance is None

    def test_build_with_instance_sets_labels(self):
        """When an instance is provided, graph node labels come from the instance."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology, VERIFIED_INSTANCES
        inst = VERIFIED_INSTANCES[0]  # SOLOMONIC
        topo = ExtractionTopology.build(instance=inst)
        node_labels = list(topo.graph.nodes())
        assert inst.label_leader   in node_labels
        assert inst.label_mid_tier in node_labels
        assert inst.label_base     in node_labels

    def test_build_with_instance_stores_instance(self):
        """instance attribute is set when provided."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology, VERIFIED_INSTANCES
        inst = VERIFIED_INSTANCES[0]
        topo = ExtractionTopology.build(instance=inst)
        assert topo.instance is inst

    def test_build_all_instances_count(self):
        """build_all_instances() must return one topology per VERIFIED_INSTANCES entry."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import (
            build_all_instances, VERIFIED_INSTANCES,
        )
        topos = build_all_instances()
        assert len(topos) == len(VERIFIED_INSTANCES)

    def test_build_all_instances_each_has_instance(self):
        """Every topology returned by build_all_instances() has a non-None instance."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import build_all_instances
        for topo in build_all_instances():
            assert topo.instance is not None


class TestExtractionTopologyMetrics:
    """ExtractionTopology.metrics() — numeric correctness."""

    def test_canonical_metrics_nodes(self):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        assert m["nodes"] == 3

    def test_canonical_metrics_edges(self):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        assert m["edges"] == 4

    def test_canonical_metrics_density(self):
        """Undirected density of K3 is 1.0; 4-edge directed on 3 nodes is 0.667."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        # density must be in (0, 1]
        assert 0 < m["density"] <= 1.0

    def test_canonical_metrics_clustering(self):
        """Average clustering must be a float in [0, 1]."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        assert 0.0 <= m["clustering"] <= 1.0

    def test_canonical_metrics_open_triangles(self):
        """Fully connected 3-node graph has 0 open triangles."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        assert m["open_triangles"] == 0

    def test_canonical_metrics_strongly_connected(self):
        """The canonical directed graph is NOT strongly connected.

        LEADER has no incoming edges (no path leads back to it from BASE or
        MID_TIER), so the graph is weakly connected but not strongly connected.
        The metrics() dict must report this accurately.
        """
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        assert m["is_strongly_connected"] is False

    def test_canonical_metrics_weakly_connected(self):
        """The canonical directed graph must be weakly connected."""
        nx = pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        topo = ExtractionTopology.build()
        assert nx.is_weakly_connected(topo.graph)

    def test_metrics_keys_present(self):
        """All expected keys must appear in the returned dict."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        m = ExtractionTopology.build().metrics()
        expected_keys = {
            "nodes", "edges", "density", "clustering",
            "transitivity", "open_triangles", "is_strongly_connected",
        }
        assert expected_keys.issubset(m.keys())


class TestExtractionTopologyIsomorphism:
    """ExtractionTopology.is_isomorphic_to() — structural comparison."""

    def test_canonical_is_isomorphic_to_itself(self):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        canonical = ExtractionTopology.build()
        assert canonical.is_isomorphic_to(canonical)

    def test_all_instances_isomorphic_to_canonical(self):
        """Every historical instance must be structurally isomorphic to canonical."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology, build_all_instances
        canonical = ExtractionTopology.build()
        for topo in build_all_instances():
            assert canonical.is_isomorphic_to(topo), (
                f"Instance {topo.instance.id!r} failed isomorphism check"
            )

    def test_verify_isomorphism_returns_true(self):
        """verify_isomorphism() helper must return True for all known instances."""
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import verify_isomorphism, build_all_instances
        # suppress stdout from the print-heavy helper
        captured = io.StringIO()
        sys.stdout = captured
        try:
            result = verify_isomorphism(build_all_instances())
        finally:
            sys.stdout = sys.__stdout__
        assert result is True


class TestExtractionTopologyPrint:
    """Print helpers must not raise and must produce non-empty output."""

    def test_print_metrics_no_raise_canonical(self, capsys):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology
        ExtractionTopology.build().print_metrics()
        captured = capsys.readouterr()
        assert "EXTRACTION TOPOLOGY METRICS" in captured.out
        assert "CANONICAL" in captured.out

    def test_print_metrics_with_instance(self, capsys):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import ExtractionTopology, VERIFIED_INSTANCES
        inst = VERIFIED_INSTANCES[0]  # SOLOMONIC
        ExtractionTopology.build(instance=inst).print_metrics()
        captured = capsys.readouterr()
        assert inst.id in captured.out

    def test_print_instance_summary_no_raise(self, capsys):
        pytest.importorskip("networkx")
        from pipeline.extraction_topology import print_instance_summary
        print_instance_summary()
        captured = capsys.readouterr()
        assert "VERIFIED EXTRACTION TOPOLOGY INSTANCES" in captured.out
        assert "7" in captured.out  # total count line


# ===========================================================================
#  SECTION 2 — voynich_morphological_comparison.py
#  Requires: numpy, scipy  (skipped gracefully if absent)
# ===========================================================================

class TestVoynichImport:
    """Module-level smoke tests."""

    def test_module_imports(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        import pipeline.voynich_morphological_comparison  # noqa: F401

    def test_ethiopian_endemics_count(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import ETHIOPIAN_ENDEMICS
        assert len(ETHIOPIAN_ENDEMICS) == 9

    def test_voynich_folios_count(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import VOYNICH_FOLIOS
        assert len(VOYNICH_FOLIOS) == 11

    def test_plant_metrics_fields(self):
        """All PlantMetrics entries must have positive numeric measurements."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import ETHIOPIAN_ENDEMICS
        for p in ETHIOPIAN_ENDEMICS:
            assert p.stem_height > 0, f"{p.name}: stem_height <= 0"
            assert p.leaf_lw > 0,     f"{p.name}: leaf_lw <= 0"
            assert 1 <= p.inflor <= 4, f"{p.name}: inflor out of [1,4]"

    def test_folio_metrics_fields(self):
        """All FolioMetrics entries must have valid folio names and positive values."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import VOYNICH_FOLIOS
        for f in VOYNICH_FOLIOS:
            assert isinstance(f.folio, str) and f.folio, f"Empty folio name: {f!r}"
            assert f.stem_height > 0
            assert f.leaf_lw > 0
            assert 1 <= f.inflor <= 4


class TestVoynichNormalisation:
    """compute_zscore_params() and normalise() — pure numeric correctness."""

    def test_zscore_params_shape(self):
        """means and stds must each have 3 elements (one per dimension)."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compute_zscore_params, ETHIOPIAN_ENDEMICS,
        )
        means, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        assert means.shape == (3,)
        assert stds.shape == (3,)

    def test_zscore_stds_positive(self):
        """Standard deviations must be strictly positive (no zero-division risk)."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compute_zscore_params, ETHIOPIAN_ENDEMICS,
        )
        _, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        assert all(stds > 0), f"Non-positive std detected: {stds}"

    def test_zscore_zero_std_clamped(self):
        """compute_zscore_params must replace 0.0 std with 1.0 to prevent division by zero."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            PlantMetrics, compute_zscore_params,
        )
        # All plants have identical inflor=2 -> std of inflor dimension = 0
        plants = [
            PlantMetrics("A", 1.0, 2.0, 2),
            PlantMetrics("B", 2.0, 3.0, 2),
            PlantMetrics("C", 3.0, 4.0, 2),
        ]
        _, stds = compute_zscore_params(plants)
        assert stds[2] == 1.0, "Zero std must be replaced with 1.0"

    def test_normalise_returns_correct_shape(self):
        """normalise() must return an array of the same shape as the input."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import normalise
        vec   = np.array([1.0, 2.0, 3.0])
        means = np.array([0.0, 0.0, 0.0])
        stds  = np.array([1.0, 1.0, 1.0])
        result = normalise(vec, means, stds)
        assert result.shape == (3,)

    def test_normalise_identity(self):
        """With mean=0 and std=1, normalise(x) == x."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import normalise
        vec   = np.array([5.0, -3.0, 0.0])
        means = np.array([0.0,  0.0, 0.0])
        stds  = np.array([1.0,  1.0, 1.0])
        result = normalise(vec, means, stds)
        np.testing.assert_array_almost_equal(result, vec)

    def test_normalise_known_value(self):
        """z = (x - mean) / std: (10 - 4) / 2 = 3.0."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import normalise
        vec   = np.array([10.0, 10.0, 10.0])
        means = np.array([ 4.0,  4.0,  4.0])
        stds  = np.array([ 2.0,  2.0,  2.0])
        result = normalise(vec, means, stds)
        np.testing.assert_array_almost_equal(result, np.array([3.0, 3.0, 3.0]))


class TestVoynichCompare:
    """compare() — single folio-plant distance computation."""

    def test_compare_identical_plant_and_folio_zero_distance(self):
        """A folio whose metrics exactly match a plant's must yield distance 0."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compare, compute_zscore_params,
            PlantMetrics, FolioMetrics, ETHIOPIAN_ENDEMICS,
        )
        plant  = ETHIOPIAN_ENDEMICS[0]   # Lobelia rhynchopetalum
        folio  = FolioMetrics("fXX", plant.stem_height, plant.leaf_lw, plant.inflor)
        means, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        result = compare(folio, plant, means, stds)
        assert result.raw_distance  == pytest.approx(0.0, abs=1e-9)
        assert result.norm_distance == pytest.approx(0.0, abs=1e-9)

    def test_compare_result_labels(self):
        """ComparisonResult must carry the correct folio and plant names."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compare, compute_zscore_params,
            FolioMetrics, ETHIOPIAN_ENDEMICS,
        )
        folio = FolioMetrics("f9v", 2.0, 1.5, 4)
        plant = ETHIOPIAN_ENDEMICS[1]  # Rosa abyssinica
        means, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        result = compare(folio, plant, means, stds)
        assert result.folio == "f9v"
        assert result.plant == plant.name

    def test_compare_distances_non_negative(self):
        """Raw and normalised distances must never be negative."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compare, compute_zscore_params,
            FolioMetrics, ETHIOPIAN_ENDEMICS,
        )
        folio = FolioMetrics("f1v", 1.0, 1.0, 1)
        means, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        for plant in ETHIOPIAN_ENDEMICS:
            result = compare(folio, plant, means, stds)
            assert result.raw_distance  >= 0.0
            assert result.norm_distance >= 0.0

    def test_compare_repr_contains_folio_and_plant(self):
        """ComparisonResult.__repr__ must include folio and plant names."""
        np = pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            compare, compute_zscore_params,
            FolioMetrics, ETHIOPIAN_ENDEMICS,
        )
        folio = FolioMetrics("f9v", 2.0, 1.5, 4)
        plant = ETHIOPIAN_ENDEMICS[0]
        means, stds = compute_zscore_params(ETHIOPIAN_ENDEMICS)
        result = compare(folio, plant, means, stds)
        r = repr(result)
        assert "f9v" in r
        assert plant.name in r


class TestVoynichRunPipeline:
    """run_pipeline() — full integration, known invariants."""

    def test_run_pipeline_returns_list(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline
        results = run_pipeline()
        assert isinstance(results, list)

    def test_run_pipeline_total_pairs(self):
        """Full run: 11 folios * 9 plants = 99 comparison pairs."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline
        results = run_pipeline()
        assert len(results) == 99

    def test_run_pipeline_sorted_ascending(self):
        """Results must be sorted by norm_distance ascending."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline
        results = run_pipeline()
        for i in range(len(results) - 1):
            assert results[i].norm_distance <= results[i + 1].norm_distance, (
                f"Not sorted at index {i}: "
                f"{results[i].norm_distance} > {results[i+1].norm_distance}"
            )

    def test_run_pipeline_strong_positives_exist(self):
        """At least one result must have norm_distance <= 0.10 (known strong match)."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline
        results = run_pipeline()
        strong = [r for r in results if r.norm_distance <= 0.10]
        assert strong, "Expected at least one strong positive match (norm <= 0.10)"

    def test_run_pipeline_custom_inputs(self):
        """run_pipeline with a single folio and single plant returns 1 result."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            run_pipeline, FolioMetrics, PlantMetrics,
        )
        folio = [FolioMetrics("fTEST", 2.0, 1.5, 4)]
        plant = [PlantMetrics("TestPlant", 2.0, 1.5, 4)]
        results = run_pipeline(folios=folio, plants=plant)
        assert len(results) == 1
        assert results[0].folio == "fTEST"
        assert results[0].plant == "TestPlant"

    def test_run_pipeline_empty_folios(self):
        """run_pipeline with empty folio list returns empty results."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline
        results = run_pipeline(folios=[])
        assert results == []

    def test_run_pipeline_default_args_uses_globals(self):
        """run_pipeline() with no args produces identical length to explicit call."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            run_pipeline, VOYNICH_FOLIOS, ETHIOPIAN_ENDEMICS,
        )
        results_default  = run_pipeline()
        results_explicit = run_pipeline(folios=VOYNICH_FOLIOS, plants=ETHIOPIAN_ENDEMICS)
        assert len(results_default) == len(results_explicit)


class TestVoynichPrintReport:
    """print_report() and flag_outliers() — output sanity."""

    def test_print_report_no_raise(self, capsys):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import run_pipeline, print_report
        results = run_pipeline()
        print_report(results, top_n=3)
        captured = capsys.readouterr()
        assert "VOYNICH" in captured.out
        assert "ETHIOPIAN" in captured.out

    def test_print_report_empty_results(self, capsys):
        """print_report with an empty list must not raise."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import print_report
        print_report([], top_n=3)
        captured = capsys.readouterr()
        assert "(none)" in captured.out

    def test_flag_outliers_detects_kniphofia(self, capsys):
        """Kniphofia foliosa (leaf_lw=50) must trigger the outlier warning."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            flag_outliers, ETHIOPIAN_ENDEMICS,
        )
        flag_outliers(ETHIOPIAN_ENDEMICS)
        captured = capsys.readouterr()
        assert "Kniphofia foliosa" in captured.out

    def test_flag_outliers_uniform_dataset_no_warning(self, capsys):
        """A dataset with all identical leaf_lw values must produce no warnings."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import (
            flag_outliers, PlantMetrics,
        )
        uniform = [PlantMetrics(f"Plant{i}", 1.0, 3.0, 2) for i in range(5)]
        flag_outliers(uniform)
        captured = capsys.readouterr()
        assert "WARNING OUTLIER" not in captured.out


class TestVoynichDataStructures:
    """PlantMetrics, FolioMetrics, ComparisonResult — pure dataclass checks."""

    def test_plant_metrics_namedtuple_access(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import PlantMetrics
        p = PlantMetrics("X", 1.5, 3.0, 2)
        assert p.name        == "X"
        assert p.stem_height == 1.5
        assert p.leaf_lw     == 3.0
        assert p.inflor      == 2

    def test_folio_metrics_namedtuple_access(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import FolioMetrics
        f = FolioMetrics("f99r", 2.0, 4.0, 3)
        assert f.folio       == "f99r"
        assert f.stem_height == 2.0
        assert f.leaf_lw     == 4.0
        assert f.inflor      == 3

    def test_comparison_result_dataclass(self):
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import ComparisonResult
        cr = ComparisonResult(
            folio="f9v", plant="Rosa abyssinica",
            raw_distance=0.5, norm_distance=0.05,
        )
        assert cr.folio         == "f9v"
        assert cr.plant         == "Rosa abyssinica"
        assert cr.raw_distance  == pytest.approx(0.5)
        assert cr.norm_distance == pytest.approx(0.05)

    def test_plant_metrics_is_immutable(self):
        """PlantMetrics is a NamedTuple and must raise AttributeError on mutation."""
        pytest.importorskip("numpy")
        pytest.importorskip("scipy")
        from pipeline.voynich_morphological_comparison import PlantMetrics
        p = PlantMetrics("X", 1.0, 2.0, 2)
        with pytest.raises((AttributeError, TypeError)):
            p.name = "Y"  # type: ignore[misc]
