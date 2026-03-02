ddl
"-- ==========================================
-- TABLE: cashflow_entries
-- ==========================================
CREATE TABLE cashflow_entries(id VARCHAR PRIMARY KEY, date DATE, inflow DOUBLE, outflow DOUBLE, net_flow DOUBLE, running_balance DOUBLE, category VARCHAR);;
"
"-- ==========================================
-- TABLE: invoices
-- ==========================================
CREATE TABLE invoices(id VARCHAR PRIMARY KEY, partner_id VARCHAR, partner_name VARCHAR, invoice_number VARCHAR, amount DOUBLE, ""type"" VARCHAR, status VARCHAR, due_date DATE, paid_date DATE, category VARCHAR, description VARCHAR, created_at TIMESTAMP);;
"
"-- ==========================================
-- TABLE: partners
-- ==========================================
CREATE TABLE partners(id VARCHAR PRIMARY KEY, ""name"" VARCHAR, category VARCHAR, contact_email VARCHAR, total_revenue DOUBLE, total_expenses DOUBLE, status VARCHAR, created_at TIMESTAMP);;
"
"-- ==========================================
-- TABLE: payment_allocations
-- ==========================================
CREATE TABLE payment_allocations(""Run Date"" DATE, ""Partner Code"" VARCHAR, ""Partner Name"" VARCHAR, ""Invoice Number"" VARCHAR, ""Paymt Ref"" DOUBLE, ""Invoice Date"" DATE, ""Invoice Amount"" DOUBLE, ""Due Date"" DATE, ""Due Amount"" DOUBLE, ""Pymnt Dt"" DATE, ""Payment Amount"" DOUBLE, ""Allocated Amt"" DOUBLE);;
"
"-- ==========================================
-- VIEW: active_customers_view
-- ==========================================
CREATE VIEW active_customers_view AS SELECT ""Partner Name"", sum(""Invoice Amount"") AS total_invoice_amount, sum(""Due Amount"") AS total_due_amount, sum(""Allocated Amt"") AS total_allocated_amount, sum(""Payment Amount"") AS total_payment_amount, count(DISTINCT ""Invoice Number"") AS total_number_of_invoices, CASE  WHEN ((sum(""Due Amount"") > 0)) THEN ('Active - Has Outstanding Exposure') WHEN ((sum(""Invoice Amount"") > 0)) THEN ('Active - Has Invoice Activity') ELSE 'Inactive' END AS customer_status FROM invoice_level_view WHERE ((""Due Amount"" > 0) OR (""Invoice Amount"" > 0)) GROUP BY ""Partner Name"" ORDER BY total_due_amount DESC, total_invoice_amount DESC;;
"
"-- ==========================================
-- VIEW: aggregate_level_view
-- ==========================================
CREATE VIEW aggregate_level_view AS SELECT sum(""Invoice Amount"") AS total_invoice_amount, sum(""Due Amount"") AS overall_exposure, sum(""Allocated Amt"") AS total_allocated_amount, sum(""Payment Amount"") AS total_payment_amount, count(DISTINCT ""Invoice Number"") AS total_number_of_invoices, count(DISTINCT ""Partner Name"") AS total_number_of_partners FROM invoice_level_view;;
"
"-- ==========================================
-- VIEW: invoice_level_view
-- ==========================================
CREATE VIEW invoice_level_view AS WITH latest_invoice_data AS (SELECT partner_code AS ""Partner Code"", partner_name AS ""Partner Name"", invoice_number AS ""Invoice Number"", invoice_amount AS ""Invoice Amount"", due_amount AS ""Due Amount"", allocated_amount AS ""Allocated Amt"", payment_amount AS ""Payment Amount"", due_date AS ""Due Date"", payment_reference AS ""Paymt Ref"", payment_date AS ""Pymnt Dt"", run_date AS ""Run Date"", row_number() OVER (PARTITION BY invoice_number ORDER BY payment_reference DESC NULLS LAST) AS rn FROM v_payments_latest WHERE (invoice_number IS NOT NULL))SELECT ""Partner Code"", ""Partner Name"", ""Invoice Number"", ""Invoice Amount"", ""Due Amount"", ""Allocated Amt"", ""Payment Amount"", ""Paymt Ref"", CASE  WHEN (((""Due Date"" IS NOT NULL) AND (""Run Date"" IS NOT NULL) AND (""Due Date"" < ""Run Date""))) THEN (""Due Amount"") ELSE NULL END AS ""OverDue Amount"", CASE  WHEN (((""Due Amount"" > 0) AND (""Due Date"" IS NOT NULL) AND (CURRENT_DATE > ""Due Date""))) THEN (CAST((CURRENT_DATE - ""Due Date"") AS INTEGER)) ELSE NULL END AS overdue_days, CASE  WHEN (((""Due Amount"" > 0) AND (""Due Date"" IS NOT NULL) AND (CURRENT_DATE > ""Due Date""))) THEN (CASE  WHEN ((CAST((CURRENT_DATE - ""Due Date"") AS INTEGER) <= 0)) THEN ('Mostly On Time') WHEN ((CAST((CURRENT_DATE - ""Due Date"") AS INTEGER) <= 30)) THEN ('1-30 Days Overdue') WHEN ((CAST((CURRENT_DATE - ""Due Date"") AS INTEGER) <= 60)) THEN ('31-60 Days Overdue') WHEN ((CAST((CURRENT_DATE - ""Due Date"") AS INTEGER) <= 90)) THEN ('61-90 Days Overdue') ELSE '91+ Days Overdue' END) ELSE 'Mostly On Time' END AS aging_bucket FROM latest_invoice_data WHERE (rn = 1) ORDER BY ""Paymt Ref"" ASC NULLS LAST;;
"
"-- ==========================================
-- VIEW: partner_and_invoice_insight
-- ==========================================
CREATE VIEW partner_and_invoice_insight AS SELECT ""Partner Code"", ""Partner Name"", sum(""Invoice Amount"") AS total_invoice_amount, sum(""Due Amount"") AS total_due_amount, sum(""Allocated Amt"") AS total_allocated_amount, sum(""Payment Amount"") AS total_payment_amount, count(DISTINCT ""Invoice Number"") AS total_number_of_invoices, CASE  WHEN ((count(DISTINCT ""Invoice Number"") > 0)) THEN ((CAST(sum(COALESCE(overdue_days, 0)) AS DOUBLE) / count(DISTINCT ""Invoice Number""))) ELSE NULL END AS avg_overdue_days, ""mode""(aging_bucket) AS aging_bucket, COALESCE(sum(""OverDue Amount""), 0) AS total_overdue FROM invoice_level_view GROUP BY ""Partner Code"", ""Partner Name"" ORDER BY total_invoice_amount DESC;;
"
"-- ==========================================
-- VIEW: v_partner_behavior
-- ==========================================
CREATE VIEW v_partner_behavior AS SELECT run_date, partner_code, partner_name, invoice_amount, due_amount, allocated_amount FROM v_payments_latest;;
"
"-- ==========================================
-- VIEW: v_partner_behavior_agg
-- ==========================================
CREATE VIEW v_partner_behavior_agg AS SELECT partner_code, min(partner_name) AS partner_name, count_star() AS total_runs, sum(CASE  WHEN ((allocated_amount > 0)) THEN (1) ELSE 0 END) AS paid_runs, sum(CASE  WHEN ((allocated_amount >= due_amount)) THEN (1) ELSE 0 END) AS fully_paid_runs, sum(CASE  WHEN ((allocated_amount < due_amount)) THEN (1) ELSE 0 END) AS stressed_runs, sum(CASE  WHEN ((allocated_amount < (due_amount * 0.5))) THEN (1) ELSE 0 END) AS severely_stressed_runs, avg(invoice_amount) AS avg_invoice_amount, avg(allocated_amount) AS avg_allocated_amount, avg(due_amount) AS avg_due_amount, sum(CASE  WHEN (((run_date < (CURRENT_DATE - CAST('90 days' AS INTERVAL))) AND (allocated_amount < due_amount))) THEN ((due_amount - allocated_amount)) ELSE 0 END) AS old_overdue_amount, max(run_date) AS latest_run_date FROM v_partner_behavior GROUP BY partner_code;;
"
"-- ==========================================
-- VIEW: v_partner_behavior_flag
-- ==========================================
CREATE VIEW v_partner_behavior_flag AS SELECT t.partner_code, t.partner_name, t.run_date, t.due_amount, t.allocated_amount, t.payment_coverage_ratio, t.gap_amount, c.cashflow_engagement, CASE  WHEN (((t.payment_coverage_ratio >= 1.0) AND (c.cashflow_engagement = 'HIGH_CASHFLOW'))) THEN ('STRONG_PARTNER') WHEN (((t.payment_coverage_ratio < 1.0) AND (c.cashflow_engagement = 'HIGH_CASHFLOW'))) THEN ('ACTIVE_BUT_LAGGING') WHEN (((t.payment_coverage_ratio BETWEEN 0.7 AND 1.0) AND (t.pcr_delta >= 0))) THEN ('IMPROVING') WHEN (((t.payment_coverage_ratio < 0.7) AND (t.gap_delta > 0) AND (c.cashflow_engagement = 'LOW_CASHFLOW'))) THEN ('DETERIORATING') ELSE 'WATCH' END AS partner_behavior FROM v_partner_trend AS t INNER JOIN v_partner_cashflow_engagement AS c ON (((t.partner_code = c.partner_code) AND (t.run_date = c.run_date)));;
"
"-- ==========================================
-- VIEW: v_partner_behavior_metrics
-- ==========================================
CREATE VIEW v_partner_behavior_metrics AS WITH base AS (SELECT partner_code, partner_name, run_date, outstanding_amount, prev_outstanding_amount, gap_reduction FROM v_partner_exposure_trend), history AS (SELECT *, max(outstanding_amount) OVER (PARTITION BY partner_code) AS peak_outstanding, sum(CASE  WHEN ((gap_reduction > 0)) THEN (gap_reduction) ELSE 0 END) OVER (PARTITION BY partner_code) AS cumulative_reduction, avg((outstanding_amount - prev_outstanding_amount)) OVER (PARTITION BY partner_code ORDER BY run_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS recent_trend FROM base), metrics AS (SELECT partner_code, partner_name, run_date, outstanding_amount, peak_outstanding, cumulative_reduction, recent_trend, CASE  WHEN ((peak_outstanding > 0)) THEN ((outstanding_amount / peak_outstanding)) ELSE 0 END AS exposure_ratio, CASE  WHEN ((peak_outstanding > 0)) THEN ((cumulative_reduction / peak_outstanding)) ELSE 0 END AS recovery_ratio, CASE  WHEN ((cumulative_reduction > 0)) THEN (1) ELSE 0 END AS has_payment_history FROM history)SELECT *, CASE  WHEN (((exposure_ratio > 0.50) AND (COALESCE(recent_trend, 0) > 0))) THEN ('HIGH') WHEN ((exposure_ratio > 0.30)) THEN ('MEDIUM') ELSE 'LOW' END AS stress_level, CASE  WHEN ((has_payment_history = 0)) THEN ('UNKNOWN') WHEN ((recovery_ratio >= 0.50)) THEN ('HIGH') WHEN ((recovery_ratio >= 0.20)) THEN ('MEDIUM') ELSE 'LOW' END AS intent_level FROM metrics;;
"
"-- ==========================================
-- VIEW: v_partner_cashflow_engagement
-- ==========================================
CREATE VIEW v_partner_cashflow_engagement AS SELECT partner_code, partner_name, run_date, due_amount, allocated_amount, CASE  WHEN (((allocated_amount >= (0.5 * due_amount)) AND (allocated_amount > 0))) THEN ('HIGH_CASHFLOW') WHEN ((allocated_amount > 0)) THEN ('MODERATE_CASHFLOW') ELSE 'LOW_CASHFLOW' END AS cashflow_engagement FROM v_partner_run_state;;
"
"-- ==========================================
-- VIEW: v_partner_exposure_by_run
-- ==========================================
CREATE VIEW v_partner_exposure_by_run AS SELECT run_date, partner_code, any_value(partner_name) AS partner_name, sum(invoice_amount) AS total_invoice_amount, sum(allocated_amount) AS total_allocated_amount, (sum(invoice_amount) - sum(allocated_amount)) AS outstanding_amount FROM v_payments_normalized GROUP BY run_date, partner_code;;
"
"-- ==========================================
-- VIEW: v_partner_exposure_trend
-- ==========================================
CREATE VIEW v_partner_exposure_trend AS SELECT partner_code, partner_name, run_date, outstanding_amount, lag(outstanding_amount) OVER (PARTITION BY partner_code ORDER BY run_date) AS prev_outstanding_amount, (lag(outstanding_amount) OVER (PARTITION BY partner_code ORDER BY run_date) - outstanding_amount) AS gap_reduction FROM v_partner_exposure_by_run;;
"
"-- ==========================================
-- VIEW: v_partner_risk_debug
-- ==========================================
CREATE VIEW v_partner_risk_debug AS SELECT a.partner_code, a.total_runs, a.paid_runs, a.fully_paid_runs, a.stressed_runs, a.severely_stressed_runs, a.avg_invoice_amount, a.avg_allocated_amount, a.avg_due_amount, a.old_overdue_amount, s.score_cashflow_continuity, s.score_cashflow_strength, s.score_low_overdue, s.penalty_severe_stress, s.penalty_old_overdue, s.net_risk_score, s.risk_bucket FROM v_partner_behavior_agg AS a INNER JOIN v_partner_risk_scored AS s ON ((a.partner_code = s.partner_code));;
"
"-- ==========================================
-- VIEW: v_partner_risk_final
-- ==========================================
CREATE VIEW v_partner_risk_final AS WITH stats AS (SELECT quantile_cont(net_risk_score, 0.25) AS p25, quantile_cont(net_risk_score, 0.75) AS p75 FROM v_partner_risk_scored)SELECT s.partner_code, a.partner_name, s.net_risk_score, CASE  WHEN ((s.net_risk_score >= stats.p75)) THEN ('LOW_RISK') WHEN ((s.net_risk_score >= stats.p25)) THEN ('MEDIUM_RISK') ELSE 'HIGH_RISK' END AS risk_bucket FROM v_partner_risk_scored AS s INNER JOIN v_partner_behavior_agg AS a ON ((s.partner_code = a.partner_code)) CROSS JOIN stats;;
"
"-- ==========================================
-- VIEW: v_partner_risk_latest
-- ==========================================
CREATE VIEW v_partner_risk_latest AS WITH ordered AS (SELECT partner_code, partner_name, run_date, outstanding_amount, gap_reduction, row_number() OVER (PARTITION BY partner_code ORDER BY run_date) AS rn FROM v_partner_exposure_trend), windowed AS (SELECT *, avg(outstanding_amount) OVER (PARTITION BY partner_code ORDER BY rn ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS avg_outstanding_6, sum(CASE  WHEN ((gap_reduction > 0)) THEN (gap_reduction) ELSE 0 END) OVER (PARTITION BY partner_code ORDER BY rn ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS serviced_last_6, sum(outstanding_amount) OVER (PARTITION BY partner_code ORDER BY rn ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS exposure_last_6 FROM ordered), metrics AS (SELECT *, CASE  WHEN ((avg_outstanding_6 > 0)) THEN ((outstanding_amount / avg_outstanding_6)) ELSE 1 END AS stress_multiplier, CASE  WHEN ((exposure_last_6 > 0)) THEN ((serviced_last_6 / exposure_last_6)) ELSE 0 END AS servicing_ratio FROM windowed), classified AS (SELECT *, CASE  WHEN ((stress_multiplier > 1.5)) THEN ('HIGH') WHEN ((stress_multiplier > 1.2)) THEN ('MEDIUM') ELSE 'LOW' END AS stress_level, CASE  WHEN ((servicing_ratio >= 0.40)) THEN ('HIGH') WHEN ((servicing_ratio >= 0.15)) THEN ('MEDIUM') ELSE 'LOW' END AS intent_level FROM metrics)SELECT partner_code, partner_name, run_date, stress_level, intent_level, stress_multiplier, servicing_ratio, CASE  WHEN (((stress_level = 'HIGH') AND (intent_level = 'LOW'))) THEN ('HIGH') WHEN (((stress_level = 'MEDIUM') AND (intent_level = 'LOW'))) THEN ('MEDIUM') ELSE 'LOW' END AS risk_bucket FROM classified QUALIFY (run_date = max(run_date) OVER (PARTITION BY partner_code));;
"
"-- ==========================================
-- VIEW: v_partner_risk_scored
-- ==========================================
CREATE VIEW v_partner_risk_scored AS SELECT partner_code, ((CAST(paid_runs AS DOUBLE) / ""nullif""(total_runs, 0)) * 50) AS score_cashflow_continuity, (least((avg_allocated_amount / ""nullif""(avg_invoice_amount, 0)), 1.5) * 40) AS score_cashflow_strength, CASE  WHEN (((avg_due_amount / ""nullif""(avg_invoice_amount, 0)) <= 0.03)) THEN (20) WHEN (((avg_due_amount / ""nullif""(avg_invoice_amount, 0)) <= 0.08)) THEN (10) ELSE 0 END AS score_low_overdue, (least(severely_stressed_runs, 8) * 5) AS penalty_severe_stress, CASE  WHEN (((old_overdue_amount / ""nullif""(avg_invoice_amount, 0)) >= 2)) THEN (15) ELSE 0 END AS penalty_old_overdue, ((((((CAST(paid_runs AS DOUBLE) / ""nullif""(total_runs, 0)) * 50) + (least((avg_allocated_amount / ""nullif""(avg_invoice_amount, 0)), 1.5) * 40)) + CASE  WHEN (((avg_due_amount / ""nullif""(avg_invoice_amount, 0)) <= 0.03)) THEN (20) WHEN (((avg_due_amount / ""nullif""(avg_invoice_amount, 0)) <= 0.08)) THEN (10) ELSE 0 END) - (least(severely_stressed_runs, 8) * 5)) - CASE  WHEN (((old_overdue_amount / ""nullif""(avg_invoice_amount, 0)) >= 2)) THEN (15) ELSE 0 END) AS net_risk_score FROM v_partner_behavior_agg;;
"
"-- ==========================================
-- VIEW: v_partner_run_state
-- ==========================================
CREATE VIEW v_partner_run_state AS WITH canonical_partner AS (SELECT partner_code, max(partner_name) AS partner_name FROM v_payments_normalized GROUP BY partner_code)SELECT p.partner_code, c.partner_name, p.run_date, sum(COALESCE(p.due_amount, 0)) AS due_amount, sum(COALESCE(p.allocated_amount, 0)) AS allocated_amount, round((sum(COALESCE(p.allocated_amount, 0)) / ""nullif""(sum(COALESCE(p.due_amount, 0)), 0)), 3) AS payment_coverage_ratio, (sum(COALESCE(p.due_amount, 0)) - sum(COALESCE(p.allocated_amount, 0))) AS gap_amount FROM v_payments_normalized AS p INNER JOIN canonical_partner AS c ON ((p.partner_code = c.partner_code)) GROUP BY p.partner_code, c.partner_name, p.run_date;;
"
"-- ==========================================
-- VIEW: v_partner_stress_window
-- ==========================================
CREATE VIEW v_partner_stress_window AS SELECT partner_code, count_star() AS total_days, sum(CASE  WHEN (((allocated_amount / nullif(due_amount, 0)) < 1)) THEN (1) ELSE 0 END) AS stressed_days, sum(CASE  WHEN (((allocated_amount / nullif(due_amount, 0)) < 0.5)) THEN (1) ELSE 0 END) AS severely_stressed_days, avg((allocated_amount / nullif(due_amount, 0))) AS avg_coverage_ratio, max(run_date) AS latest_run_date FROM v_partner_behavior GROUP BY partner_code;;
"
"-- ==========================================
-- VIEW: v_partner_trend
-- ==========================================
CREATE VIEW v_partner_trend AS SELECT partner_code, partner_name, run_date, due_amount, allocated_amount, payment_coverage_ratio, gap_amount, allocated_amount AS allocation_intensity, round((allocated_amount / ""nullif""(due_amount, 0)), 3) AS allocation_scale_ratio, lag(payment_coverage_ratio) OVER (PARTITION BY partner_code ORDER BY run_date) AS prev_pcr, lag(gap_amount) OVER (PARTITION BY partner_code ORDER BY run_date) AS prev_gap, CASE  WHEN ((lag(payment_coverage_ratio) OVER (PARTITION BY partner_code ORDER BY run_date) IS NULL)) THEN (NULL) ELSE (payment_coverage_ratio - lag(payment_coverage_ratio) OVER (PARTITION BY partner_code ORDER BY run_date)) END AS pcr_delta, CASE  WHEN ((lag(gap_amount) OVER (PARTITION BY partner_code ORDER BY run_date) IS NULL)) THEN (NULL) ELSE (gap_amount - lag(gap_amount) OVER (PARTITION BY partner_code ORDER BY run_date)) END AS gap_delta FROM v_partner_run_state;;
"
"-- ==========================================
-- VIEW: v_partner_verdict
-- ==========================================
CREATE VIEW v_partner_verdict AS SELECT partner_code, ((CAST(paid_runs AS DOUBLE) / total_runs) * 40) AS score_cashflow_continuity, (least((avg_allocated_amount / nullif(avg_invoice_amount, 0)), 1.5) * 30) AS score_cashflow_strength, CASE  WHEN ((avg_due_amount <= (avg_invoice_amount * 0.05))) THEN (20) WHEN ((avg_due_amount <= (avg_invoice_amount * 0.10))) THEN (10) ELSE 0 END AS score_low_overdue, (severely_stressed_runs * 15) AS penalty_severe_stress, (((((CAST(paid_runs AS DOUBLE) / total_runs) * 40) + (least((avg_allocated_amount / nullif(avg_invoice_amount, 0)), 1.5) * 30)) + CASE  WHEN ((avg_due_amount <= (avg_invoice_amount * 0.05))) THEN (20) WHEN ((avg_due_amount <= (avg_invoice_amount * 0.10))) THEN (10) ELSE 0 END) - (severely_stressed_runs * 15)) AS net_risk_score, CASE  WHEN (((((((CAST(paid_runs AS DOUBLE) / total_runs) * 40) + (least((avg_allocated_amount / nullif(avg_invoice_amount, 0)), 1.5) * 30)) + CASE  WHEN ((avg_due_amount <= (avg_invoice_amount * 0.05))) THEN (20) WHEN ((avg_due_amount <= (avg_invoice_amount * 0.10))) THEN (10) ELSE 0 END) - (severely_stressed_runs * 15)) >= 60)) THEN ('LOW_RISK') WHEN ((((((CAST(paid_runs AS DOUBLE) / total_runs) * 40) + (least((avg_allocated_amount / nullif(avg_invoice_amount, 0)), 1.5) * 30)) - (severely_stressed_runs * 15)) >= 30)) THEN ('MEDIUM_RISK') ELSE 'HIGH_RISK' END AS risk_bucket FROM v_partner_behavior_agg;;
"
"-- ==========================================
-- VIEW: v_payments_latest
-- ==========================================
CREATE VIEW v_payments_latest AS SELECT * FROM v_payments_normalized WHERE (run_date = (SELECT max(run_date) FROM v_payments_normalized));;
"
"-- ==========================================
-- VIEW: v_payments_normalized
-- ==========================================
CREATE VIEW v_payments_normalized AS SELECT ""Run Date"" AS run_date, ""Partner Code"" AS partner_code, ""Partner Name"" AS partner_name, ""Invoice Number"" AS invoice_number, ""Paymt Ref"" AS payment_reference, ""Invoice Date"" AS invoice_date, ""Invoice Amount"" AS invoice_amount, ""Due Date"" AS due_date, ""Due Amount"" AS due_amount, ""Pymnt Dt"" AS payment_date, ""Payment Amount"" AS payment_amount, ""Allocated Amt"" AS allocated_amount FROM payment_allocations;;
"
