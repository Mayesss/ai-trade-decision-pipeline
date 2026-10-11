# L1 scout: Hyperliquid user vaults (feasibility, behaviour only)

2026-10-11. Light L1 of `docs/edge-discovery-2026-10-11.md`. This scout measures
the opportunity set and the data. It does not measure returns: no P&L, NAV change,
APR, skill score or t-stat was computed, printed or saved for any vault or account.
Full machine output: `data/scout_l1/tables.md` (gitignored). Scripts:
`scouts/l1_*.py`.

## Summary

1. **Universe.** The list has 9,463 vaults created before 2026-07-01 (9,455 user,
   8 HLP). About 7,700 were created before the archive, but at any date most are
   dead. The ledger gives a working discovery-period closure proxy: the first
   `vaultDistribution` on the vault's own ledger. 71% of pre-archive vaults were
   distributed before 2025-07-28, and none of those 218 sampled vaults has a fill
   in the archive.
2. **Selectable at 001's dates** (≥ 20 trips in 91 d, equity ≥ $10k at a snapshot
   ≤ 7 d before S): **158 / 131 / 114 / 121**. That drops to 51 / 49 / 44 / 41 at
   ≥ $100k and 9 / 7 / 8 / 7 at ≥ $1M. Under 001's full T1 filters: 99 / 78 / 65 / 72.
   Selectable vaults trade like 001's *day* bucket (median hold 10–14 h), not like
   its slow bucket (44–48 h).
3. **Capacity under 10% of equity:** a $10k deposit fits 41–51 selectable vaults
   per window, $50k fits 12–17, and $250k fits 2–5.
4. **Pre-archive history is coarse.** `vaultDetails.portfolio.allTime` is a
   resampled grid of one point every 7 or 14 days: about 7 points per 91-day
   lookback and 4 per 56-day hold. `day`, `week` and `month` hold nothing before
   2026-07-01. The grid keeps producing points long after a vault has distributed,
   so it cannot show whether a vault was alive. 001's per-trip score cannot come
   from this history. `userFillsByTime` does serve pre-archive vault fills, but the
   API's retention of recent fills cuts off active vaults.
5. **The list looks complete.** Sampled leaders created 1,064 vaults (ledger
   `vaultCreate`), and none is missing from the list. Of 300 non-list archive
   addresses, none has role `vault` (the 10 positive controls all do). The list
   includes long-dead vaults: 85% of pre-archive vaults show no activity in the
   archive year.

---

## A. Universe over time

**A1. Vaults created per quarter** (list `createTimeMillis`; vaults created on or after 2026-07-01 were dropped at load)

| quarter created | user vaults | HLP parent/children | cumulative |
|---|---|---|---|
| 2023Q1 | 2 | 0 | 2 |
| 2023Q2 | 51 | 3 | 56 |
| 2023Q3 | 84 | 1 | 141 |
| 2023Q4 | 636 | 0 | 777 |
| 2024Q1 | 1,629 | 0 | 2,406 |
| 2024Q2 | 890 | 0 | 3,296 |
| 2024Q3 | 847 | 0 | 4,143 |
| 2024Q4 | 1,713 | 0 | 5,856 |
| 2025Q1 | 1,203 | 0 | 7,059 |
| 2025Q2 | 624 | 1 | 7,684 |
| 2025Q3 | 678 | 0 | 8,362 |
| 2025Q4 | 488 | 3 | 8,853 |
| 2026Q1 | 512 | 0 | 9,365 |
| 2026Q2 | 98 | 0 | 9,463 |

`relationship` in the pre-cutoff list: 9,455 `normal` (the user vaults), 7
`child` and 1 `parent` (HLP, 0xdfc2…f303). HLP is excluded from every count in
B–F.

**A2. Archive closure proxies** (user vaults created before the archive start, 2025-07-28; archive 2025-07-28 → 2026-06-30)

| created | vaults | any archive fill | share | any snapshot row | last fill before 2026-04-01 |
|---|---|---|---|---|---|
| 2023Q1 | 2 | 0 | 0.0% | 0 | 0 |
| 2023Q2 | 51 | 2 | 3.9% | 2 | 2 |
| 2023Q3 | 84 | 3 | 3.6% | 3 | 1 |
| 2023Q4 | 636 | 26 | 4.1% | 28 | 19 |
| 2024Q1 | 1,629 | 66 | 4.1% | 64 | 45 |
| 2024Q2 | 890 | 44 | 4.9% | 43 | 25 |
| 2024Q3 | 847 | 70 | 8.3% | 69 | 48 |
| 2024Q4 | 1,713 | 195 | 11.4% | 185 | 136 |
| 2025Q1 | 1,203 | 260 | 21.6% | 246 | 181 |
| 2025Q2 | 624 | 327 | 52.4% | 315 | 232 |
| 2025Q3 | 205 | 162 | 79.0% | 158 | 125 |

"Last fill before 2026-04-01" means the vault traded in the archive year, then had
no fill in the last 91 days before the cutoff. The archive cannot tell closed from
idle. Quarter of the last archive fill, over all user vaults that traded:
2025Q3 386, 2025Q4 739, 2026Q1 475, 2026Q2 933. Snapshots are no use as an
"equity drops to 0" proxy: they hold only accounts with at least one open
position, so a closed or flat vault simply disappears.

**A3. Ledger event types** (`userNonFundingLedgerUpdates`, endTime 2026-06-30T23:59:59.999Z; counts only, no amounts kept). This covers the 308 sampled user vaults from D (from their creation date), their 291 leaders (from 2023-04-01), and one 3-day HLP window (2025-01-06..08).

| ledger | event type | events | accounts with ≥ 1 |
|---|---|---|---|
| vault (308) | vaultDeposit | 7,474 | 169 |
| | vaultWithdraw | 6,484 | 140 |
| | vaultCreate | 308 | 308 |
| | vaultDistribution | 249 | 239 |
| | internalTransfer | 38 | 16 |
| | liquidation | 19 | 15 |
| leader (291) | accountClassTransfer | 33,100 | 247 |
| | spotTransfer | 14,023 | 182 |
| | withdraw / deposit | 6,353 / 6,300 | 263 / 286 |
| | send | 6,093 | 144 |
| | vaultLeaderCommission | 5,999 | 109 |
| | vaultDeposit / vaultWithdraw | 5,155 / 3,790 | 230 / 213 |
| | subAccountTransfer | 4,097 | 41 |
| | spotGenesis | 2,848 | 186 |
| | vaultDistribution | 1,478 | 241 |
| | internalTransfer | 1,408 | 126 |
| | rewardsClaim | 1,225 | 59 |
| | vaultCreate | 1,064 | 289 |
| | cStakingTransfer / borrowLend / liquidation / activateDexAbstraction | 275 / 104 / 40 / 15 | 68 / 20 / 28 / 11 |
| HLP 3-day window | vaultWithdraw / vaultDeposit | 1,365 / 986 | 1 |

Depositor flows **do** appear on the vault's own ledger (`vaultDeposit` and
`vaultWithdraw`: 13,958 events on 308 vaults). The leader's commission appears on
the leader's ledger (`vaultLeaderCommission`). The API page cap is 2,000 rows (HLP
probe), and the script pages on time with de-duplication.

**A4. Ledger closure proxy:** the first `vaultDistribution` on the vault's own ledger (308 sampled user vaults created before 2025-07-01; the share is weighted by population over the creation-quarter strata)

| first vaultDistribution | sampled vaults | weighted share |
|---|---|---|
| before 2025-07-28 (pre-archive) | 218 | 71.1% |
| 2025-07-28 .. 2026-06-30 (archive year) | 21 | 6.9% |
| none before 2026-07-01 | 69 | 22.0% |

Validation against the archive:
- The 218 vaults distributed before the archive have **0** archive fills.
- 13 of the 21 distributed in the archive year have fills. For 11 of those 13, the last fill is within 0.2 days before the distribution. The other two show −10 to −189 days and **+50 days** (one vault traded after a distribution).
- 21 of the 69 never-distributed vaults traded in the archive.
- Distributions run about 1.04 per vault.

So `vaultDistribution` behaves like a closure marker. It is not proven to be one,
and partial distributions may exist.

## B. Selectable vaults

**B1. At 001's selection dates** (`features_<S>.parquet` covers vault addresses: 1,579 / 1,530 / 1,202 / 1,247 listed vaults have rows; lookback [S−91 d, S))

| S | user vaults existing | with fills in lookback | ≥ 20 trips | ≥ 20 trips & eq ≥ $10k (any lookback snapshot) | ≥ 20 trips & eq ≥ $10k (snapshot ≤ 7 d) | … ≥ $100k | … ≥ $1M | pass all 001 T1 filters | pass 001 slow filters |
|---|---|---|---|---|---|---|---|---|---|
| 2025-10-27 | 8,546 | 1,575 | 766 | 256 | **158** | 51 | 9 | 99 | 42 |
| 2025-12-22 | 8,810 | 1,524 | 661 | 213 | **131** | 49 | 7 | 78 | 28 |
| 2026-02-16 | 9,065 | 1,195 | 514 | 157 | **114** | 44 | 8 | 65 | 29 |
| 2026-04-13 | 9,436 | 1,241 | 550 | 156 | **121** | 41 | 7 | 72 | 36 |

"Existing" counts creation only; the list cannot remove closures. Equity is the
account value at the latest snapshot in the lookback. A vault that is flat at S
has no reading.

**B2. Quarterly dates before the archive** (list creation times, plus a closure
estimate from the A4 ledger sample). Per-quarter strata are small (8–66 sampled),
so the early rows are noisy.

| date | user vaults created before | est. share not yet distributed | est. count |
|---|---|---|---|
| 2023-07-01 | 53 | 28% | 15 |
| 2023-10-01 | 137 | 52% | 71 |
| 2024-01-01 | 773 | 40% | 310 |
| 2024-04-01 | 2,402 | 41% | 980 |
| 2024-07-01 | 3,292 | 33% | 1,086 |
| 2024-10-01 | 4,139 | 30% | 1,254 |
| 2025-01-01 | 5,852 | 29% | 1,670 |
| 2025-04-01 | 7,055 | 29% | 2,039 |
| 2025-07-01 | 7,679 | 30% | 2,298 |

The trip and equity filters cannot be applied before the archive without fills
(see D) or `accountValueHistory` values (not read here). A reference point from
inside the archive: at 2025-10-27, D3 estimates about 1,856 undistributed vaults,
1,575 had fills in the lookback, and 158 were selectable.

**B3. Behaviour:** selectable vaults vs 001's non-vault T1 buckets (feature medians)

| S | group | n | median of median hold (h) | median fills / active day |
|---|---|---|---|---|
| 2025-10-27 | selectable vaults | 158 | 10.4 | 78 |
| | 001 slow / day / scalper | 533 / 2,049 / 520 | 46.8 / 5.6 / 0.55 | 34 / 61 / 162 |
| 2025-12-22 | selectable vaults | 131 | 11.8 | 67 |
| | 001 slow / day / scalper | 428 / 1,818 / 524 | 44.4 / 5.1 / 0.57 | 35 / 60 / 159 |
| 2026-02-16 | selectable vaults | 114 | 10.3 | 76 |
| | 001 slow / day / scalper | 345 / 1,506 / 445 | 44.7 / 4.7 / 0.57 | 34 / 60 / 183 |
| 2026-04-13 | selectable vaults | 121 | 14.3 | 52 |
| | 001 slow / day / scalper | 371 / 1,603 / 342 | 48.0 / 5.2 / 0.57 | 40 / 56 / 138 |

Selectable vaults by 001 hold bucket (scalper / day / slow): 22 / 87 / 49,
18 / 79 / 34, 13 / 63 / 38, 15 / 60 / 46.

## C. Archive coverage

**C1.** User vaults that existed at S (by creation)

| S | existing | with fills in lookback | with a snapshot row in lookback | in the snapshot just before S | equity share (that snapshot) of vaults with lookback fills | equity share of selectable vaults |
|---|---|---|---|---|---|---|
| 2025-10-27 | 8,546 | 18.4% | 18.0% | 620 | 99.7% | 77.2% |
| 2025-12-22 | 8,810 | 17.3% | 15.6% | 598 | 99.7% | 61.6% |
| 2026-02-16 | 9,065 | 13.2% | 11.5% | 555 | 98.7% | 53.7% |
| 2026-04-13 | 9,436 | 13.2% | 13.5% | 659 | 98.8% | 49.0% |

Across the whole archive year, 2,540 listed vault addresses have fills (of
724,430 archive addresses), and 2,346 have snapshot rows. The low count shares
are dead vaults (A4). Equity is known only for vaults with an open position.

**C2. Who does the trading:** the vault address or the leader's own address (archive-year notional)

The 279 vaults selectable at any S were measured. The vault's share of
(vault + leader) notional was p10 0.26, p25 0.73, median 0.99, p75 1.00. For 47
of them the leader's own address traded more than the vault, and for 83 the
leader address has no archive fill at all. 108 of the 279 have a leader who runs
more than one listed vault. Overall, 6,831 leaders run the listed vaults, 1,113
of them run more than one, and the maximum is 178.

| vault (8 largest by archive notional) | vault active days | leader active days | days both active | vault notional share |
|---|---|---|---|---|
| 0xca230e81… | 327 | 2 | 2 | 1.00 |
| 0xf967239d… | 135 | 2 | 2 | 1.00 |
| 0x06749041… | 131 | 3 | 3 | 1.00 |
| 0x07fd993f… | 338 | 0 | 0 | 1.00 |
| 0x4cb5f4d1… | 113 | 189 | 51 | 0.48 |
| 0x1e37a337… | 310 | 1 | 1 | 1.00 |
| 0x32af4c34… | 163 | 0 | 0 | 1.00 |
| 0x780825f3… | 149 | 1 | 0 | 1.00 |

## D. History density before the archive (`vaultDetails`, 313 vaults)

The sample is stratified by creation quarter, seed 11, with proportional
allocation (minimum 8 per quarter). It covers 308 user vaults created before
2025-07-01 plus the 5 HLP accounts created by then. All 313 calls returned a
history. Only timestamps before 2026-07-01 were kept.

**D1.** Series and points per vault

| series | vaults with ≥ 1 point < 2026-07-01 | median points | max |
|---|---|---|---|
| day / week / month | 0 | 0 | 0 |
| perpDay / perpWeek / perpMonth | 0 | 0 | 0 |
| allTime | 313 | 66 | 97 |
| perpAllTime | 313 | 57 | 86 (starts 2024-05 for most) |

**D2.** `allTime` by creation stratum

| stratum | sampled | median points | median spacing (d) | median first point − creation (d) | last pre-cutoff point |
|---|---|---|---|---|---|
| 2023Q1 | 2 | 91.5 | 14 | 4.1 | 2026-06-17/24 |
| 2023Q2 | 8 | 83 | 14 | 1.1 | " |
| 2023Q3 | 8 | 76 | 14 | 3.1 | " |
| 2023Q4 | 24 | 69 | 14 | 3.3 | " |
| 2024Q1 | 63 | 64 | 14 | 3.6 | " |
| 2024Q2 | 34 | 57.5 | 14 | 3.4 | " |
| 2024Q3 | 33 | 51 | 14 | 2.6 | " |
| 2024Q4 | 66 | 80.5 | 7 | 3.2 | " |
| 2025Q1 | 46 | 75.5 | 7 | 3.1 | " |
| 2025Q2 | 24 | 56.5 | 7 | 3.2 | " |
| HLP | 5 | 92 | 14 | 6.0 | " |

- **The gaps form a grid.** Of 20,635 gaps, 10,936 are 14 d, 9,461 are 7 d, and 238 are 0 d (a point at creation followed by the first grid point). Within one vault, the spacing switches between 7 d and 14 d. Timestamps sit at 19–23 h UTC.
- **Closed vaults still return a history.** All 239 vaults with a `vaultDistribution` (218 of them before 2025-07-28) still return `allTime` points every 7–14 days up to June 2026. Their last pre-cutoff point is a median of 631 days after the distribution. Point density therefore does not show that a vault was alive.

**D3. Can a 91 d lookback / 56 d hold be computed point-in-time from `allTime` alone?** (19 windows, S stepping 56 d from 2023-07-31)

| S | sampled user vaults created before S−91 d | median points in lookback | median points in hold | ≥ 6 lookback & ≥ 4 hold | not distributed before S | est. population (both) |
|---|---|---|---|---|---|---|
| 2023-07-31 | 3 | 7 | 4 | 3 | 2 | 2 |
| 2023-09-25 | 10 | 7 | 4 | 10 | 3 | 8 |
| 2023-11-20 | 13 | 7 | 4 | 13 | 5 | 29 |
| 2024-01-15 | 20 | 7 | 4 | 20 | 9 | 71 |
| 2024-03-11 | 33 | 7 | 4 | 33 | 10 | 98 |
| 2024-05-06 | 67 | 7 | 4 | 67 | 21 | 386 |
| 2024-07-01 | 105 | 7 | 4 | 105 | 35 | 772 |
| 2024-08-26 | 126 | 7 | 4 | 126 | 38 | 868 |
| 2024-10-21 | 152 | 7 | 4 | 152 | 44 | 1,023 |
| 2024-12-16 | 165 | 7 | 4 | 165 | 43 | 995 |
| 2025-02-10 | 188 | 7 | 4 | 188 | 46 | 1,073 |
| 2025-04-07 | 244 | 7 | 4 | 244 | 60 | 1,437 |
| 2025-06-02 | 277 | 7 | 4 | 277 | 68 | 1,647 |
| 2025-07-28 | 288 | 7 | 4 | 288 | 75 | 1,830 |
| 2025-09-22 … 2026-05-04 | 303–308 | 7 | 4 | all | 70–76 | 1,701–1,856 (vaults created after 2025-06 not sampled) |

Facts for the orchestrator:
- A history-only score would rest on about 7 NAV points per lookback and about 4 per hold.
- 001's per-trip score (round trips, RoN) needs fills. `allTime` has none.

**D4. Pre-archive fills from the API** (`userFillsByTime`, counts and times only)
- **Dormant vaults:** in 24 seeded sampled vaults created before 2024-06, two pre-archive windows returned fills for 4 and 0 vaults (max 9 fills). Most old vaults are dormant (A4), so this sample is uninformative.
- **Positive control:** 12 vaults active at the archive start (20–1,500 archive fills in 2025-07-28..08-10, created before 2025-03).
  - For the archive window, the API count equals the archive count for 10 of 12. One returned 470 against 594 in the archive (possibly TWAP slices, which this endpoint omits). One returned **0 against 1,270**, which fits the API's retention of each account's most recent fills.
  - For 2025-05-01..07-27, 11 of 12 returned fills (98–2,000+). Four hit the 2,000-row page cap, and paging was not run.

So pre-archive fills exist for less active vaults and are cut off for very active
ones. How complete they are by vault was not measured.

## E. Terms

**Documentation** (fetched 2026-10-11 from hyperliquid.gitbook.io: hypercore/vaults/for-vault-leaders, for-vault-depositors, protocol-vaults, vaults; text as served today):

| term | rule |
|---|---|
| Leader profit share | "Vault leaders receive a 10% profit share for managing the vault." |
| Leader minimum ownership | "you must maintain ≥5% of the vault at all times"; the leader "cannot withdraw … if it would cause your share to fall below 5%". |
| Creation | A 10k USDC gas fee (current docs) and a leader deposit of at least 100 USDC. The name cannot be changed. User vaults trade only validator-operated perps (no spot, no HIP-3). |
| Depositor lock-up | User vaults 1 day; HLP 4 days after the most recent deposit. |
| Withdrawal | If margin is sufficient, open positions are untouched. Otherwise orders are cancelled in increasing order of margin used, then "20% of positions are automatically closed", repeated until the withdrawal is covered. Leaders may set "always proportionally close positions on withdrawals". Slippage is borne by the withdrawer. |
| Closure | "All positions must be closed before the vault can close." "All depositors will receive their share of the vault when it is closed." |
| Deposit caps | None documented. `allowDeposits` is a leader-settable flag, with no doc text. |
| Status | The docs call HyperCore vaults "legacy", superseded by HyperEVM vaults (introduced 2023). |

**From vaultDetails** (308 sampled user vaults):

| field | distribution |
|---|---|
| `leaderCommission` | 0.10: 307; 0.00: 1 |
| `allowDeposits` (**today's value**) | true 271, false 37 |
| `alwaysCloseOnWithdraw` (**today's value**) | false 289, true 19 |
| `relationship` (all 313) | normal 308, child 4, parent 1 |

The historical values of `allowDeposits` and of the creation fee cannot be
recovered from these endpoints.

## F. Capacity: selectable vaults with equity at S ≥ 10 × deposit

| S | selectable (eq ≥ $10k) | $10k (eq ≥ $100k) | $50k (eq ≥ $500k) | $250k (eq ≥ $2.5M) |
|---|---|---|---|---|
| 2025-10-27 | 158 | **51** | **17** | **5** |
| 2025-12-22 | 131 | 49 | 14 | 5 |
| 2026-02-16 | 114 | 44 | 12 | 2 |
| 2026-04-13 | 121 | 41 | 13 | 2 |

Equity is the vault's total account value (leader plus depositors) at the
latest snapshot ≤ 7 d before S.

## G. Survivorship

| test | result |
|---|---|
| Does the list include long-dead vaults? | Yes. 6,696 of 7,884 user vaults created before 2025-07-28 (84.9%) have no fill and no snapshot row in the archive year. 92 of the 308 sampled vaults were distributed before 2024-07-01 and are still listed. |
| Vaults created by sampled leaders but missing from the list | **0 of 1,064** distinct `vaultCreate` vaults across 291 leaders (full ledgers 2023-04 → 2026-06-30). |
| `userRole` (today) of archive addresses not in the list | Top 150 by archive notional: subAccount 88, user 62, **vault 0**. Random 150 (seed 11): user 146, subAccount 4, **vault 0**. Control (10 listed vaults): vault 10. |

Limits:
- The leader test cannot see a vault whose leader has no listed vault.
- The random role sample is 150 of about 722k non-list addresses, so it would only catch a large missing population.
- A full check would be `userRole` on all 722k archive addresses, about 2 days at half the rate limit. It was not run.

## H. Ingredients for statistical power

The rules match 001's schedule: 91 d lookback, 56 d hold, step 56 d, hold ends by the stated date, and holds do not overlap.

| design | windows | S range | selectable per window |
|---|---|---|---|
| Pre-archive history only (2023-05-01 → 2025-07-28) | **13** | 2023-07-31 … 2025-06-02 | Undistributed vaults, estimated (D3): 2 → 1,647 (under 100 before 2024-03, about 1,000 from 2024-10). Trip and equity filters cannot be applied without fills or NAV values. |
| Archive year only (001 schedule) | **4** | 2025-10-27 … 2026-04-13 | 158 / 131 / 114 / 121 (eq ≥ $10k); 51 / 49 / 44 / 41 (≥ $100k) |
| History across both (2023-05-01 → 2026-07-01) | **19** | 2023-07-31 … 2026-05-04 | as above, combined |

All windows are on the same venue and share selection (§2 of the programme doc).
The pre-archive span overlaps 001 T1 only in the choice of light, not in data.

---

## Reproduce

```bash
cd research/copytrade
.venv/bin/python -I scouts/l1_archive.py          # vault+leader daily activity, pruned archive < 2026-07-01 (~40 s)
.venv/bin/python -I scouts/l1_candidates.py       # G: all archive addresses, userRole candidates (~25 s)
.venv/bin/python -I scouts/l1_api.py sample       # stratified sample, seed 11 (offline)
.venv/bin/python -I scouts/l1_api.py details      # 313 x vaultDetails, projected at load (~11 min)
.venv/bin/python -I scouts/l1_api.py ledger       # 600 x userNonFundingLedgerUpdates (~25 min)
.venv/bin/python -I scouts/l1_api.py roles        # 310 x userRole (~10 min)
.venv/bin/python -I scouts/l1_fills_probe.py      # D4 dormant-sample probe
.venv/bin/python -I scouts/l1_fills_probe.py control   # D4 positive control
.venv/bin/python -I scouts/l1_vaults_scout.py > data/scout_l1/tables.md   # all tables A-H
```

All outputs go under `data/scout_l1/` (gitignored). `vaultDetails`, `userRole`
and the docs are today's server state, so a re-run can differ slightly. The
`details` run stalled once at 120 of 313 and was resumed on the same sample.

## What could not be determined

- **Exact closure dates.** `isClosed` is today's state and was not read. `vaultDistribution` is a validated proxy (A4), not a definition, and one vault traded 50 days after one.
- **Equity at S before the archive.** The only source is `accountValueHistory` values, which were not read under rule 3. Inside the archive, snapshots cover only vaults with an open position.
- **Trip-based selection before the archive.** API fills exist but are cut off by retention for active vaults (D4). Per-vault completeness was not measured, because that needs paging and the archive cross-check (startPosition chain) on many vaults.
- **The leader's ownership share over time, and depositor counts.** `leaderFraction` and `followers` are current state and were discarded. Ledger `vaultDeposit` and `vaultWithdraw` counts exist per vault, but users were not extracted.
- **Historical `allowDeposits` and creation fee.** Only today's values are visible.
- **Whether `allTime` points after a distribution are flat or carry anything.** That would need the values, which were not read.
- **Full list completeness.** See G limits.

## Holdout hygiene

Endpoints called (all public, no key):

| endpoint | calls | end bound |
|---|---|---|
| `vaultDetails` | 313 + 1 structure probe | No end parameter. Each response was projected in memory before any print or save: `allTime`/`perp*`/`day`/`week`/`month` → `accountValueHistory` **timestamps < 1782864000000 only**; `leaderCommission`; today's `allowDeposits` and `alwaysCloseOnWithdraw`; relationship type. `apr`, `followers`, `leaderFraction`, `maxDistributable`, `maxWithdrawable`, `isClosed`, `description`, `pnlHistory` and `vlm` were discarded unread. Raw responses were never cached. The probe printed key names, point counts < cutoff and first dates only. |
| `userNonFundingLedgerUpdates` | 600 + 2 structure probes + 1 page-cap probe | `endTime` = 1782863999999 (HLP window: 2025-01-08 end). Every row is re-checked to be < cutoff (the script stops otherwise; it never fired). Kept: time, type, vault address, presence of `user`. Amounts were discarded. |
| `userFillsByTime` | 72 | `endTime` ≤ 2025-08-10. Kept: count and first/last time; `closedPnl` discarded. |
| `userRole` | 310 | No time dimension (today's role). |
| `allMids` | 1 | Reachability test: HTTP status only, body discarded. |
| Docs (gitbook) | 5 pages | Today's text. |

Local reads:
- **Vault list:** only address, leader, name, createTimeMillis and relationship. Vaults with createTimeMillis ≥ cutoff were dropped at load.
- **Archive:** `data/pruned` dates < 2026-07-01; `realized_pnl` was never selected.
- **Snapshots:** `snapshot_ms < 1782864000000` in every analysis query.
- **Features:** `features_<S>` (discovery).

No value, return or P&L dated on or after 2026-07-01 was printed or saved.
Three incidental exposures, all counts and no values, nothing saved:

1. An early schema check counted, over the whole snapshot file, the rows with
   `snapshot_ms ≥ 2026-07-01` (**8,651,725**). It also printed all-period totals:
   27,219,616 rows, 384 distinct snapshots and 703,272 distinct users. So the
   snapshot file holds 98 holdout snapshots. That was already implied by the
   inventory's "286 discovery snapshots".
2. The printed pre-cutoff list size (9,463), set against the documented 9,477,
   implies that 14 listed vaults were created on or after 2026-07-01.
3. `allowDeposits`, `alwaysCloseOnWithdraw` and `userRole` are today's values,
   as the brief allows, and are flagged where reported.
