export interface ExecutionStressMetricsRow {
    label: string;
    severityRank: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
}

export type ExecutionMonotonicityMetric = 'netR' | 'expectancyR' | 'profitFactor' | 'maxDrawdownR';

export interface ExecutionMonotonicityViolation {
    metric: ExecutionMonotonicityMetric;
    lessSevereLabel: string;
    moreSevereLabel: string;
    lessSevereValue: number;
    moreSevereValue: number;
    delta: number;
}

export interface ExecutionMonotonicityResult {
    ok: boolean;
    violations: ExecutionMonotonicityViolation[];
}

export interface ExecutionMonotonicityOptions {
    tolerance?: number;
}

function isFiniteNumber(value: unknown): value is number {
    return typeof value === 'number' && Number.isFinite(value);
}

export function findExecutionMonotonicityViolations(
    rows: ExecutionStressMetricsRow[],
    options: ExecutionMonotonicityOptions = {},
): ExecutionMonotonicityViolation[] {
    const tolerance = Math.max(0, Number(options.tolerance ?? 1e-9));
    const sorted = rows
        .filter(
            (row) =>
                isFiniteNumber(row.severityRank) &&
                isFiniteNumber(row.netR) &&
                isFiniteNumber(row.expectancyR) &&
                isFiniteNumber(row.maxDrawdownR),
        )
        .slice()
        .sort((a, b) => a.severityRank - b.severityRank);

    const violations: ExecutionMonotonicityViolation[] = [];
    for (let i = 0; i < sorted.length; i += 1) {
        for (let j = i + 1; j < sorted.length; j += 1) {
            const lessSevere = sorted[i]!;
            const moreSevere = sorted[j]!;

            if (moreSevere.netR > lessSevere.netR + tolerance) {
                violations.push({
                    metric: 'netR',
                    lessSevereLabel: lessSevere.label,
                    moreSevereLabel: moreSevere.label,
                    lessSevereValue: lessSevere.netR,
                    moreSevereValue: moreSevere.netR,
                    delta: moreSevere.netR - lessSevere.netR,
                });
            }

            if (moreSevere.expectancyR > lessSevere.expectancyR + tolerance) {
                violations.push({
                    metric: 'expectancyR',
                    lessSevereLabel: lessSevere.label,
                    moreSevereLabel: moreSevere.label,
                    lessSevereValue: lessSevere.expectancyR,
                    moreSevereValue: moreSevere.expectancyR,
                    delta: moreSevere.expectancyR - lessSevere.expectancyR,
                });
            }

            if (isFiniteNumber(lessSevere.profitFactor) && isFiniteNumber(moreSevere.profitFactor)) {
                if (moreSevere.profitFactor > lessSevere.profitFactor + tolerance) {
                    violations.push({
                        metric: 'profitFactor',
                        lessSevereLabel: lessSevere.label,
                        moreSevereLabel: moreSevere.label,
                        lessSevereValue: lessSevere.profitFactor,
                        moreSevereValue: moreSevere.profitFactor,
                        delta: moreSevere.profitFactor - lessSevere.profitFactor,
                    });
                }
            }

            if (moreSevere.maxDrawdownR < lessSevere.maxDrawdownR - tolerance) {
                violations.push({
                    metric: 'maxDrawdownR',
                    lessSevereLabel: lessSevere.label,
                    moreSevereLabel: moreSevere.label,
                    lessSevereValue: lessSevere.maxDrawdownR,
                    moreSevereValue: moreSevere.maxDrawdownR,
                    delta: lessSevere.maxDrawdownR - moreSevere.maxDrawdownR,
                });
            }
        }
    }
    return violations;
}

export function evaluateExecutionMonotonicity(
    rows: ExecutionStressMetricsRow[],
    options: ExecutionMonotonicityOptions = {},
): ExecutionMonotonicityResult {
    const violations = findExecutionMonotonicityViolations(rows, options);
    return {
        ok: violations.length === 0,
        violations,
    };
}
