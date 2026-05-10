-- Q03 — Eligibility-gate breakdown over silver.steamreviews
-- v2 refactor (Amendment 1): add Goat/empty/other shape counts (more interpretable than per-gate-only)
-- Output: G:/Work/Altanwir-scratch/findings-results/Q03-eligibility-counts.csv

COPY (
  SELECT
    COUNT(*) AS total_reviews,
    SUM(CASE WHEN isVaderEligible THEN 1 ELSE 0 END) AS pass_all_gates,
    SUM(CASE WHEN NOT isVaderEligible THEN 1 ELSE 0 END) AS fail_any_gate,
    ROUND(100.0 * SUM(CASE WHEN NOT isVaderEligible THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fail_any_gate,

    -- Per-gate failure rates (overlap; reviews can fail multiple)
    ROUND(100.0 * SUM(CASE WHEN reviewLength <= 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fail_length_gate,
    ROUND(100.0 * SUM(CASE WHEN asciiRatio < 0.15 AND COALESCE(uniqueWordRatio, 0) <> 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fail_ascii_gate,
    ROUND(100.0 * SUM(CASE WHEN COALESCE(uniqueWordRatio, 0) < 0.1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fail_unique_gate,
    ROUND(100.0 * SUM(CASE WHEN NOT hasCredibleText THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_fail_credible_gate,

    -- Shape-based counts (interpretable buckets of "what kind of junk got caught")
    SUM(CASE WHEN reviewLength <= 1 THEN 1 ELSE 0 END) AS empty_shape_count,
    SUM(CASE WHEN reviewLength > 1000 AND wordCount = 1 THEN 1 ELSE 0 END) AS goat_shape_count,
    SUM(CASE WHEN NOT isVaderEligible AND reviewLength > 1 AND NOT (reviewLength > 1000 AND wordCount = 1) THEN 1 ELSE 0 END) AS other_ineligible_count
  FROM silver.steamreviews
) TO 'G:/Work/Altanwir-scratch/findings-results/Q03-eligibility-counts.csv' (HEADER, DELIMITER ',');
