# FleetOps tracking benchmark

Machine: Windows-11-10.0.26200-SP0, Python 3.14.6
Seeds: [1, 2, 3, 4, 5]. Synthetic data only.

## 1. Throughput (single tracker process, in-memory)

56,969 pings from 300 buses (5% dup, 5% reorder, 20% of trips with a dropout)

| pings/sec | p50 per ping | p99 per ping |
|---|---|---|
| 82,846 | 12.1 µs | 24.8 µs |

## 2. Parent alert quality (target: alert ~5 min before arrival)

Scored on the LAST message each parent got before the bus arrived. `lead` = real arrival − alert time. `late` = under 2 min warning. `early` = last message was an alert sent over 10 min early (a 'delayed' update correctly cancels it). ETA MAE scored on predictions made 4–6 min out. `false breakdowns` = breakdown incidents for buses that never broke down.

| scenario | method | scored stops | median lead | late % | early % | ETA MAE (5 min out) | ETA bias | false breakdowns |
|---|---|---|---|---|---|---|---|---|
| clean | naive | 829 | 433 s | 0.0% | 0.4% | 272 s | +198 s | 0 |
| clean | moving, no warmup | 829 | 306 s | 0.0% | 0.0% | 57 s | +35 s | 0 |
| clean | moving + warmup | 829 | 305 s | 0.0% | 0.0% | 57 s | +35 s | 0 |
| clean | + breakdown updates | 829 | 305 s | 0.0% | 0.0% | 57 s | +35 s | 0 |
| traffic | naive | 973 | 533 s | 0.0% | 30.4% | 208 s | +136 s | 0 |
| traffic | moving, no warmup | 973 | 376 s | 0.2% | 2.6% | 104 s | +34 s | 0 |
| traffic | moving + warmup | 973 | 351 s | 0.2% | 2.3% | 104 s | +34 s | 0 |
| traffic | + breakdown updates | 973 | 351 s | 0.2% | 2.3% | 104 s | +34 s | 0 |
| breakdowns | naive | 908 | 531 s | 0.0% | 40.2% | 269 s | +192 s | 0 |
| breakdowns | moving, no warmup | 908 | 364 s | 0.0% | 25.8% | 112 s | +13 s | 0 |
| breakdowns | moving + warmup | 908 | 347 s | 0.1% | 25.8% | 112 s | +13 s | 0 |
| breakdowns | + breakdown updates | 908 | 299 s | 10.2% | 0.3% | 103 s | +41 s | 0 |

ETA bias = mean(predicted - actual arrival). Positive = on average the ETA says the bus is later than it really is. Note an alert fires on the FIRST prediction under 5 min, so a noisy estimator can be late on average yet still alert early.

## 3. Fault handling (10% dup, 10% reorder, 30% of trips with a GPS dropout)

| pings | accepted | duplicates dropped | out-of-order dropped | duplicate alerts | dropouts injected | GPS_LOST opened | closed on recovery | buses finished |
|---|---|---|---|---|---|---|---|---|
| 28,757 | 24,210 | 2,154 | 2,393 | 0 | 53 | 53 | 53 | 150/150 |
