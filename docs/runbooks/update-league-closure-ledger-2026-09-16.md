# September Update League closure ledger

State: isolated real-file repair PASSED in35387584221: both checkpoints, fresh unmodified engine write/checkpoint/reopen, and block346 retirement verified. Production repair is prepared but not yet executed. This ledger is for league updates, not SuperTable SOTA work.

## Current bounded-recovery receipts - 2026-09-18

- SUCCESS35387584221, publicmain058db42e9: isolated replay9.854s,
  both CHECKPOINTs returned, zero newly allocated metadata blocks, fresh
  stock write/checkpoint/reopen/drop/checkpoint passed. Block346 no longer
  registered. Entire remote proof48.2s including startup; machine destroyed.
  Artifact10563988695 is65010bytes, no database payload.
- Live read-only catalog verification confirms all five exact quarantined
  names plus five replacements. Production WAL484MiB (35388068533), so
  production handoff retains only this WAL (explicit1GiB ceiling), not16GB
  database. Existing16GiB/8CPU capacity retained. Remote work<=150s with
  inspect5/preserve25/replay65/remove5/verify50 phase caps. The ordinary
  refresh120s hard limit is unchanged. No unconditional service restart
  after an ambiguous result; exact original config restored only after
  durable stock proof. No fixtures or isolated resume evidence on production.
- Reviewed handoff fixed two predeployment response bugs with red/green
  regression tests: supervisor completion event propagation and raw Fly
  API exit_signal handling. No production mutation from these tests.

- Trial35387352711 stopped before engine open with34s remaining, so the
 35s startup guard was the artificial blocker; no DB mutation occurred.
  User then explicitly approved increasing the limit and test writes.
  Use ONE60s total trial including startup;30s for checkpoint+fresh stock
  verification, with all other stage, memory, checksum and metadata caps
  unchanged. This replaces the previously approved40s total, not the120s
  refresh cap. Do not repeat fractional deadline tuning or add new machinery.

- User explicitly approved a longer checkpoint allowance and test writes,
  retaining tight/light work. Set verify20s within unchanged40s total; bind
  only the fresh35386902355 main/WAL fingerprint and reject stale inputs.
  Both changed preflight regressions failed before implementation. No
  checksum, WAL preservation, target-table or production-access gate relaxed.
- Completion scope includes every affected failed worker/import and league
  recovery, correct visible2026week1, retained history/aliases/merges and
  ranks. A successful pilot or server health response is not completion.

- Resume35386706937 on ecc328a27 reached the real engine. Replay9.894s;
  retained-value reconciliation passed, zero newDROP/fixture rows. First
  checkpoint advanced through41tables (last observedtransaction_player_career)
  before the15s verify cap killed it; no checksum exception appeared in the
  captured trace, but no checkpoint return or stock-engine proof exists.
  PeakRSS1870316KiB; cumulative reads874651648B,writes250314752B at28.143s.
  This proves forward checkpoint work, NOT durable success or corruption fixed.
  Machine286265dc44e418 destroyed19:35:24Z. Do not repeat with old binding.
- Read-only reconciliation35386902355 took0.834s. Main15555375104B,
  inode14,mtime_ns1789760116677575290; WAL45522341B,inode64,
  mtime_ns1789760101741571802,SHA256
  c45382cd53958c6b66691371ff1c2de195427f1a3c06b29c5ae1286de8f11176.
  Header and bad-block hashes unchanged; activecheckpoint12126. No companion
  WAL checkpoint/recovery file. Inspector d895d17b006548 destroyed19:37:04Z.
  Current adapter intentionally retains the older binding and will refuse
  another resume until this evidence is explicitly incorporated.
  Narrow next proposal:20s checkpoint/proof allowance within unchanged40s
  overall pilot, rather than repeating a proven15s cutoff. This has NOT been
  implemented or authorized; no production write or restart was performed.

- On02f3c5063, synthetic35386383813 passed all6cases (62adapter tests6.22s).
  Real trial35386448920 successfully placed shared4/3072MiB machine
  48ee5d1cee7e48. It stopped BEFORE engine open: startup consumed8.4s and
  the internal38s deadline left just under the required30s. No database
  mutation or changed baseline; machine destroyed19:32:17Z. Capacity is
  therefore no longer the observed blocker for this profile.
  Remove the duplicate2s reservation: use the whole40s deadline already
  enforced by the unchanged outer timeout. No stage limit or safety gate
  changes. This is not permission to lengthen the pilot or touch production.

- Fresh publication failures: Yahoo full import35382170308 (`stk`), Sleeper
  quick import35379651781 (`ifl_dynasty`) and refresh35383306930 all report
  the same checkpoint checksum failure at90714112. The first two live bundle
  status lookups return202/VALIDATED, notCOMMITTED. Their bundles were only
  3.3MB/2.8s and0.2MB/1.1s respectively; provider work is not this blocker.
  The Yahoo run lists no retained Actions artifacts. Do not claim its payload
  is preserved or mark either import published from a health response.
- One smaller isolated placement is prepared, not yet run: shared4/3072MiB,
  engine2560MB, preserving4CPU volume throughput. Previous measured peakRSS
  was2024476KiB, so this reduces requested host RAM with measured headroom.
  It changes no helper behavior, table scope, deadlines or production config.
  The real-connection memory regression failed against3072MB as expected.
  Run existing tests before this single trial; no capacity retry ladder.

- Blocked audit after the repeated capacity condition across the status turn
  and two goal continuations: the recovery host refused all three tested
  placements, there is no live recovery job, and the inspected inventory has
  no abandoned pilot to remove. Observer/native/synthetic work is complete
  for the next bounded trial. Real-file proof cannot advance until this
  existing volume's host has suitable capacity. Do not loop unchanged launches
  or substitute production, a copied volume, or extended deadlines.
- Fresh production catalog-only query returned exactly the five quarantined
  names AND all five canonical replacements. This proves cleanup is still
  incomplete, not preservation of every value. One /ready503 was transient;
  immediate recheck returned200/serving/accepting with no active writers,
  without a restart. No production mutation was performed.
- Handoff remains unexecuted: local close_pool cannot close borrowed handles
  and HTTP maintenance routes reopen the pool. Live runtime configuration also
  differs from local fly.toml. A controlled server-process shutdown and exact
  deployed-config binding are prerequisites AFTER successful isolated proof;
  no hot helper or local-config deployment is an approved substitute.

- Diagnostic35381870547 on619009c6c: even the previously working
  shared4/3584 placement was refused by the volume host at18:43:58Z, before
  engine open. No third unchanged launch is authorized by this evidence.
  Its3321-byte artifact was inspected: only production1781e011b69068 exists
  (started,shared8/16384,volumevol_rkg7mmd17llez224); no stranded pilot is
  consuming capacity. Recovery volumevol_4919j2m0wzg0xw5r is unattached,
  created,in iad/zone529a. No stale pilot can be removed to clear the blocker.
  All three placement refusals leave the exact35378960124 baseline unchanged.
- Current blocker: no suitable VM can be placed on the existing recovery
  volume's host. New table-progress observation is verified and ready, but
  no real checkpoint trace from it exists yet. Do not claim the suspected
  compaction work is the real-file bottleneck. Resume the one bounded probe
  only after capacity changes; no copying/moving volume or larger time caps.
  Production /ready still serves/accepts with zero active queries/writes.

- 38ca16ee9 Linux proof35381566119: all6cases passed,62adapter tests6.18s,
  real_scope8.673s. Native WriteTable observer saw103stock callbacks. Its
  negative mutant was correctly rejected; helper-free write/reopen, individual
  drops, interruption/replay and repeat proofs still pass. Review corrections:
  direct table-name accessor replaces fullDDL serialization; no-op recovery
  does not incorrectly require new checkpoint callbacks.
- Actual performance1/4096 placement35381668335 refused at18:41:55Z with
  insufficient resources on existing volume; no VM/DB open or baseline change.
  No more unchanged performance launches. Next is one diagnostic on the known
  shared4/3584 profile with newly verified table progress; unchanged40/15s
  limits. Its purpose is locating the checkpoint work, not asserting speed.

- Public main feb16731e: synthetic Linux run35379364978 passed all six cases,
  including61adapter tests5.95s and real_scope6.712s. These are synthetic,
  not proof of real-file durability.
- Capacity trial35379439057 refused performance2/4096MiB on the existing
  recovery volume at18:19:01Z: insufficient host resources. No VM was created,
  no engine opened and no baseline changed. Do not repeat this launch unchanged.
- Checkpoint source review: max_vacuum_tasks=0 disables merging tasks but not
  InitializeVacuumState's empty-row-group removal and subsequent metadata
  rewriting. A tiny local1.5.1 characterization preserved122880rows and
  score sum22661468160; concurrent checkpoint retained2rowgroups, but fresh
  normal reopen/write/checkpoint reduced them to1. Total2.515s. This only
  defers the work; NOT adopted as a fix and NOT production-engine evidence.
  Primary sources: DuckDB v1.5.4 src/storage/table/row_group_collection.cpp
  InitializeVacuumState/Checkpoint and src/transaction/duck_transaction_manager.cpp.
- Next diagnostic: sample the stock WriteTable callback's current table,
  elapsed time and completed-table count every2s. Bounded512-byte samples;
  no new queries, native I/O, values or altered checkpoint behavior. The
  timeout-persistence regression failed before forwarding this event and
  passes after. Native observer proof on1.5.4 remains pending; no real pilot
  until it and the existing preservation/interruption suite pass.
- Local scoped suite79passed,3Linuxskipped,2legacy-metadata-donor-excluded
  in23.28s. Startup shell proofs2passed1.55s; YAML/compile/diff checks passed.
  Ruff reports the same3pre-existing findings on HEAD and working files;
  no unrelated lint edits. Candidate capacity is nowperformance1/4096MiB:
  fewer dedicated CPUs than the refused placement, with48MiB/s documented
  volume bandwidth. No claimed speed result;3GiB engine and all time caps
  unchanged. Requires synthetic proof before one isolated attempt.

- Real resume35378792853 on3e7e573df: startup4.46s (previous7.60s).
  WAL replay9.747s, preservation reconciliation passed, new_drops0 and
  new_fixture_rows0. First checkpoint exceeded15s; no stock proof. Observed
  read throughput during replay~32MiB/s; peakRSS2024476KiB. No full DB copy.
  Machine080d6e0b226138 destroyed18:13:09Z. Actual outcome UNKNOWN: a delayed
  wait after SIGKILL incorrectly escaped as FAILED; regression reproduced it,
  and stop/reap now retains UNKNOWN plus termination_unconfirmed.
- No-engine inspection35378960124:3258-byte complete trace recovered;
  fingerprinting0.825s. Header/bad-block SHA unchanged. Mainmtime
  1789755176638185818; WAL45522288bytes,mtime1789755164142675724,SHA
  81f5df9726e7a1e4009e3de53e8ee9d13e6c9369ae52618b6d75e3666f991af3.
  Inspector683e3d6c447de8 destroyed18:14:18Z. New exact baseline bound in
  adapter/test. Local78passed,3Linuxskipped,2legacyexcluded in16.61s.
- Next isolated capacity trial:performance2/4GiB, same volume and3GiB engine
  ceiling. Fly documents64MiB/s vs shared4's32MiB/s; measured I/O supports
  testing this, NOT a claim it will finish. User authorized temporary capacity.
  No retries/fallback tier, deadline increase, production resize or copy.
  Production remains serving/accepting, no active queries/writes.

- Public main 2b676c1a3: bounded trace/inspection changes published. Linux
  matrix35378176034: adapter58tests5.71s; real_scope6.416s, five individual
  durable steps1.067s. Shared test's compiler hit its5s limit on attempt1;
  only that failed job reran, passing the behavioral proof1.313s on attempt2.
- Real resume35378327494 refused BEFORE opening DuckDB: startup left29.6s,
  below the unchanged30s minimum. Machine2870549b440618 destroyed18:07:52Z;
  no replay, drops or WAL changes. New startup-only reduction overlaps local
  compiler with independent inventory reads and removes a duplicate list.
  All results still checked before mutation. Two real-shell regression cases
  failed before change, then passed1.51s; compiler failure propagates. No
  timeout/identity/capacity change. Real-file checkpoint remains unverified.

- Trace changes: independent review closed all findings. Fresh local targeted
  suite:75passed,3Linuxskipped,2legacy-metadata-donor-deselected in14.32s.
  A trace's zero-exit end remains PROVISIONAL; authoritative completion needs
  the separate stock-verified completed.json. Slow/disk-failed logging cannot
  extend replay's deadline or report a potentially mutable attempt as FAILED.
  These changes are locally verified, not yet Linux/live verified or pushed.
- Lightweight priority: retain useful evidence from one bounded attempt rather
  than repeat blind replay/checkpoint attempts. Four explicit checkpoints are
  present in the proof path; do not remove any without fresh durability proof.
  Earlier failures occurred during the FIRST checkpoint, so deleting later
  verification checkpoints would not resolve that observed blocker. Production
  /ready checked serving/accepting with zero active queries/writes.

- Real resume `35375944871` on `d7a270eb7`: shared4/3.5GiB DID start; no
  host-capacity refusal. The Machines API command exhausted its31s remaining
  deadline and returned no child output. Actual outcome UNKNOWN. Temporary
  VM `d895d17b006648` destroyed17:43:40Z; production unchanged.
- No-engine reconciliation `35376088686` completed0.854s. Header still12126,
  exact bad-block fingerprint unchanged. Main size15555375104,inode14,
  mtime1789753408168174644. WAL45522235bytes,inode64,
  mtime1789753395532165342,SHA
  `37130c73a403e3a951bd7ea978d226456f77b2ffbfa68272f39c500f7a60f5d3`.
  No alternate sidecars. Inspection VM `e8204deb337728` destroyed17:44:35Z.
  Resume accepts only this observed baseline; no committed WAL was discarded.
- Closing the missing-output gap before another mutable run: create-only,
  <=64KiB structured per-attempt trace, asynchronous fsync isolated from the
  phase watchdog, exact-ID read-only trace inspection without DuckDB open.
  Disk-failure and slow-fsync regressions were red before fixes; failure after
  a potentially mutable child starts is UNKNOWN even if phase logs lag. No
  deadline increase, full-file copy, engine change or new production write.

- Public main `23348a5cd`, Linux matrix `35375537024`: all six groups PASS.
  Actual fault-injected synthetic fixture3,944,448bytes: first single-object
  commit/checkpoint/stock-process verification finished0.384s; five independent
  steps finished1.587s,18 fresh metadata blocks,zero repeated drops. Every step
  reopened in a separate helper-free process and performed an ordinary write,
  checkpoint and reopen before advancing. Full real_scope pilot6.708s; adapter
  53tests passed6.08s. Prior steps survive a later refused transaction (local
  regression). Read-only independent review found no must-fix defect.
- This is SYNTHETIC proof, not live repair. Actual real-volume CLI was not
  rerun or broadened: its existing all-five WAL commit still needs durable
  checkpoint/stock proof. No production changes, new volume copies, resized
  machines or new Fly allocations. Production /ready remains serving/accepting
  with zero active writes. Next real operation must reconcile and persist
  existing commits, not issue duplicate drops or recreate removed tables.

- User amendment: allow one approved quarantined object at a time; do not
  require all five removals to succeed together. Temporary capacity increases
  are authorized, with cleanup afterward. Existing actual isolated WAL already
  contains all five committed removals: never undo or repeat those drops to
  manufacture a single-object trial. Durable checkpoint remains unverified.
- Single-object primitive now requires the exact target plus explicitly known
  prior removals. Unexpected partial absence still refuses mutation. Four red
  regressions preceded implementation; local50passed/3Linuxskipped10.61s.
  The small real_scope synthetic pilot now tests individual commits with a
  checkpoint, fresh helper-free reopen/write/checkpoint, and no-op repeat after
  EACH object. Pinned Linux evidence pending; production and real-volume resume
  remain unchanged. No claim that table-by-table DROP reduces WAL replay or
  database-checkpoint cost. Latest /ready is serving, zero active writes.

- Latest revision `125ef1649`: direct Machines-API inspection `35373965485`
  passed, including deliberately failing remote exit7. Startup+inspection
  completed about8s; fingerprint query0.932s, exact current main/WAL unchanged.
  VM `78452e4b6627e8` removed17:23:08Z. Full Linux matrix `35373994603`
  passed all six groups. Real resume `35374104719` was refused by Fly BEFORE
  machine/database startup: even shared4/4GiB now lacks host capacity. No
  unchanged retry, no file mutation, no extra capacity left allocated.
- Its 3,329-byte machine-inventory artifact contains only production machine
  `1781e011b69068`, started, shared8/16GiB, production volume. No disposable
  pilot remains. Production sizing has not been changed by this work.
  Latest /ready serving/accepting, zero active writes/publications; OPS startup
  timestamp changed externally17:25:03Z (not this work). Actual isolated
  durable checkpoint/stock proof, exclusive production handoff and import/
  refresh canaries remain UNVERIFIED. Goal stays active; this is progress,
  not a completion claim. Next viable real attempt requires available volume-
  host capacity and the >=30s remaining-window guard, retaining all caps.

- Four-thread matrix `35373115939` on `f3e65e0d4` passed all six groups.
  Actual `35373221048` again exhausted the overall startup-inclusive window
  during first checkpoint. Last process counters:862,777,344 read bytes,
  237,989,888 written bytes, CPU23.48s. No durable checkpoint marker; UNKNOWN.
  VM `683e3d6c449348` removed17:16:10Z. This does not establish CP time>15s.
- No-engine `35373483853` took0.928s: header iteration12126 and known bad
  block unchanged; main size15555375104,inode14,mtime1789751759807751374;
  WAL45522182bytes,inode64,mtime1789751748307783731,SHA
  `6a14b987e064f8854b3027971171d98d670c8e3fd8486645ab5425567bdd08ef`.
  No alternate sidecars. VM `7812613c007368` removed17:18:12Z. New baseline
  is exact; earlier receipts/WAL copies retained, no blind retry.
- Startup-starvation guard now requires >=30s remain before starting a recovery
  child; deadlines are not extended. Child checkpoint markers are forwarded
  immediately instead of lost if the outer deadline kills the supervisor.
  Machines-API transport avoids SSH setup; explicit JSON exit-code AND terminal
  event checks prevent flyctl's zero CLI exit from hiding a remote failure.
  Three red regressions preceded implementation; local46passed/3Linuxskipped.
  API transport is first tested read-only with a deliberate remote exit7;
  real execution must wait for that receipt. Production still unchanged.

- `35372449206` on `5bdd4444d`: all six Linux groups passed, including all45
  adapter tests8.65s and the no-compaction regression. Actual resume
  `35372578623`: replay9.786s, original witnesses preserved, zero new drops or
  inserts. CHECKPOINT1 was cut off after about10.45s by the OVERALL deadline
  (startup consumed more time); outcome UNKNOWN, not evidence that CP needs
  over15s. VM `e8204deb339178` destroyed17:10:07Z. No production changes.
- No-engine reconciliation `35372875340` took0.953s. Header iteration12126,
  block346 fingerprint, inode14 and size15555375104 unchanged; main mtime
  1789751396939092785. WAL size45522129, mtime1789751386755047295,inode64,
  SHA `016b3debb96b9479e39dacd99aee29f6ad58bbd95dfc498a45ccf1cf5e1d61a7`.
  No alternate sidecars; VM `080d6e0b224708` removed17:12:08Z. Only this new
  exact resume baseline is accepted. All prior WAL/evidence retained.
  Next bounded optimization uses four already-available DuckDB worker threads
  (previously one) with unchanged3072MB/no-spill and zero optional vacuum.
  Synthetic native interruption tests also use four threads before real use.

- Capacity attempts `35371924340` (performance8/16GiB) and `35372065094`
  (performance2/4GiB) were refused by the existing volume host before a VM or
  database process started. No data change or extra capacity remains. Resume
  baseline from `35371660210` is unchanged; no unchanged capacity retry.
- Tiny behavioral test reproduced optional stock-checkpoint compaction:
  245,760 input rows with committed deletes retained in WAL become 81,920
  rows with the same values, but default checkpoint merges two row groups
  into one. Recovery config now sets stock `max_vacuum_tasks=0`: durable
  replay/checkpoint retains both groups and identical surviving values,
  avoiding unrelated optional rewrites. This test failed before the setting.
  This is a candidate explanation for real CP cost, not yet an actual-file
  performance proof. Same deadlines, memory and allocation cap; shared4/4GiB
  remains available. Pinned Linux regression and then real pilot still needed.

- Actual resume `35371482774`: retained current WAL, replay 9.648s, five DROP
  records replayed, all five names absent, original value witnesses matched;
  zero new inserts/drops. First CHECKPOINT hit the 15s verify cap: UNKNOWN,
  adapter 29.114s. Process I/O reached 929,771,520 read and 218,066,944 written
  bytes including WAL/evidence and ordinary row-group work, not a full DB copy.
  No checkpoint return marker. VM `48ee5d1cee6908` removed at 16:58:16Z.
- No-engine reconciliation `35371660210` completed in 0.967s. Same main size,
  inode, header iteration 12126 and bad-block SHA. Main mtime now
  1789750685202052088; WAL size 45522076, mtime 1789750670218175673, inode64,
  SHA `6be240c48f4ad466183c07ffb5e8f3acdbefc1317efa330ec398a3a827e3aa91`.
  No alternate WAL sidecars. VM `286265dc442108` removed16:59:39Z. This is the
  ONLY new accepted resume state; both earlier WAL/evidence copies remain.
  Next resume uses user-authorized temporary performance-8 CPU/16GiB for
  128MiB/s volume bandwidth; existing DuckDB memory,128block and time caps
  remain unchanged. Cleanup destroys temporary capacity; production unchanged.

- Public main `d93faa186`, matrix `35371382486`: all six Linux groups passed.
  The shared-reference MarkBlocksAsModified proof fails the deliberately broken
  mask variant with exposed allocatable subslots, then passes the real mask on
  two checkpoints with incoming mask `0xfffffffffffffffd`. Retained values
  and the protected block digest stay unchanged; releasing the last reference
  retires the block normally. Stock-helper-free reopen/write/checkpoint passes.
  Shared group elapsed 2.804s, fixture 1,323,008 bytes; no Fly access in matrix.
  Actual isolated resume dispatched as `35371482774` on that exact SHA.

- Guarded resume implementation: only original receipt `35368369603_1` plus
  the exact post-commit main/WAL identity from `35369876519` is accepted.
  Current WAL is retained separately before replay; original before-witnesses
  are reused. After replay, all five old objects MUST already be absent and
  the sampled values unchanged. No reseeding or new DROP is issued. A stock
  write probe now carries an exact receipt owner and safely reconciles its
  own interrupted write, while refusing another owner's object. Five new
  regressions were red before implementation; local adapter result 41 passed,
  three Linux-only skipped. Storage pilot tests: 19 passed, two old donor
  cases deselected. Linux shared-block Mark-origin proof and real resume
  remain pending; no production mutation is authorized by these local results.

- Actual isolated run `35368369603` on `5b7a8d2ed`: export twenty genuine rows
  in about 0.66s; guarded retained-WAL replay 9.739s; fixture type/value and
  preservation checks passed. Five-object removal COMMIT returned in about
  8ms. Checkpoint processing exited 97 at the 128-new-block allocation ceiling,
  adapter elapsed 16.509s. No completed checkpoint marker or stock proof;
  outcome UNKNOWN, never called durable success. Fixture 5927 bytes, SHA256
  `373b43909b047303815798e581b1d3c861562fbcbd3a3f2cf064de09949df4f6`.
  Machine `7812613c007168` was destroyed at 16:26:06Z.
- No-engine reconciliation `35368541439` took 0.034s: header/active iteration
  12126 and known bad-block SHA remain unchanged, main size 15555375104 and
  inode 14 unchanged, main mtime now 1789748756148253909. WAL is now 45522023
  bytes, mtime 1789748753472137934, inode 64. Preserve CURRENT main/WAL plus
  original evidence under `recovery_five_35368369603_1`; do not restore only
  the old WAL or blindly reseed/drop. Machine `d8d0795b99d468` removed.
  This inspection did not yet include `.wal.recovery`; a later bounded check
  includes all exact sidecars and hashes, with no engine open.
- Root cause reproduced in a 3.94MB synthetic catalog, `35368965152` on
  `c95cc042e`: global `PeekNextBlockId=-1` allocated a full 256KiB block for
  each 4KiB handle, exhausting the cap even for a healthy dense catalog.
  `204d30403` restores stock packing and reserves only the exact damaged
  block's free subslots, using the engine's in-memory FreeBlocksFromInteger
  call; checksums/live references remain untouched. Independent source review
  confirms normal all-unreferenced retirement precedes that mask call.
- `35369387456` proves internal Read interception, five-block dense checkpoints,
  stock reopen/write, crash-after-commit/flush and repeat recovery for the
  dense fixture. Matrix overall remains FAILED: old tests demanded fresh
  allocation even when stock packing reused healthy blocks, and an unrelated
  DROP test incorrectly demanded interception for an already retired block.
  `901c45b43` corrects those test preconditions and makes the cap test force
  real physical allocations with a bounded dense catalog. Local result:
  55 passed, three Linux-only skipped, two old donor tests deselected, 9.57s.
  Full follow-up matrix `35369788137` passed all six jobs; no second real-file
  mutation has been attempted. The real file still requires reconciliation
  using its post-commit WAL, not a fresh removal attempt.
- No-engine sidecar reconciliation `35369876519` on `901c45b43` completed in
  0.917s. Main and WAL inode/size/mtime match `35368541439`; neither
  `.wal.checkpoint` nor `.wal.recovery` exists. Current 45522023-byte WAL SHA256:
  `b330657077bb40250e0e1977309d917810f6c9856eb9bc4cef9dc1d9b0dc1213`.
  Machine `2870549b442968` was destroyed at 16:41:03Z. This is the exact current
  recovery baseline, with original WAL/evidence also retained separately.
  Production `/ready` remained serving/accepting with no active query/write
  activity, league fingerprint `sha256:935b1b973ff578d5`. Its startup timestamp
  changed externally to 16:35:16Z; this work made no production changes.
- Still required: prove mask interception through MarkBlocksAsModified with a
  retained live reference, complete exact post-commit reconciliation/resume
  without duplicate inserts/drops, durable stock-engine verification on the
  real isolated file, then controlled exclusive production procedure and
  actual import/refresh canaries. Fresh-allocation cap is not a claim that all
  ordinary engine metadata writes total <=32MiB; WAL/normal writes are separate.

- `ffc2f9026` adds an isolated fixture of genuine current aggregate values,
  capped at four rows per named replacement / twenty total / 64 KiB. Its two
  read-only Fly requests took 1.344s for 5447 bytes locally. Export runs under a
  5s external ceiling inside the same 40s pilot; the child's preservation cap
  is reduced to 5s so total preservation cannot exceed the approved 10s.
  Fixture rows enter only empty selected-league scopes on the exact isolated
  volume, in the SAME transaction as the five drops. Existing rows, other
  leagues, facts and configuration cannot be overwritten. Failure rolls back
  fixture inserts and drops together. Exact inputs and value hashes are kept
  in create-only receipts. This is test setup, not a production data restore.
- Seven fixture regressions failed before implementation and passed afterward.
  Live `/query` rejects parenthesis-leading SQL; a failing test now covers that
  boundary. A second red test limits export to two network requests; the first
  ten-request draft exceeded the 5s ceiling and was not used on a pilot.
  Local combined tests: 52 passed, three Linux-only skipped, two old donor tests
  deselected, 8.88s. Linux matrix `35368007952` passed all six jobs including
  37 adapter tests in 3.67s. No removal dispatch with this fixture was made.
- Independent review identified possible silent value coercion before the
  preservation baseline. `5b7a8d2ed` rejects mismatched DuckDB source/target
  types AND compares actual inserted JSON values with exported values before
  baseline capture. Both rounding/type regressions failed before the fix.
  Local combined result: 54 passed, three Linux-only skipped, two donor tests
  deselected, 9.62s. Follow-up Linux matrix pending before physical dispatch.

- Latest physical pilot revision: `1269396af`; preceding Linux matrix
  `35365345651` passed. The user explicitly authorized temporary capacity
  increases followed by reduction. Machine cleanup removes the temporary
  capacity; production sizing is unchanged. Parent-side /proc counters report
  CPU, peak RSS, reads and writes every 2s.
- `35365427189` (performance CPU / 4 GiB) was refused by the existing volume's
  host before creating a machine or opening the database. `35365632447`
  (`35c9d26ba`, shared 2 CPU / 4 GiB, 3072MB cache) launched successfully but
  reached the 10s replay ceiling. At 9.59s of replay it had consumed 2.57s
  cumulative process CPU and 990292 KiB peak RSS; disk reads rose about
  32-35 MB per two seconds. It never reached preservation queries or DROP.
  Machine `286265dc442008` was destroyed at 15:59:04Z.
- Fly's official volume limits explain that measured rate: shared 1/2 CPU
  receives 16 MiB/s, shared 4 CPU receives 32 MiB/s
  (https://fly.io/docs/volumes/overview/). `1269396af` changes only the disposable
  pilot to shared 4 CPU, retaining 4 GiB RAM, 3072MB/no-spill cache, and every
  time/allocation guard. Run `35366265545` confirms the measured I/O hypothesis:
  WAL replay completed in 9.734s, zero experimental metadata blocks allocated.
  Preservation then failed because `homepage_manager_rankings` had no rows for
  `nyu_ffl`; no removal began. Total adapter time 14.020s. Machine
  `d8d0795b99d768` was destroyed at 16:05:09Z; temporary capacity is removed.
  No volume fork/copy or production resize is involved.
- Prior isolated run `35170289421` explains this fixture mismatch: it created
  EMPTY canonical shells when quarantining the five original objects. The
  later full canonical reaggregation was production-only, not applied to this
  retained isolated volume. Empty shells must not be presented as proof of
  healthy aggregate-value preservation. A bounded real-value witness fixture
  is still needed before actual removal; no emptiness bypass was added.
- No-engine reconciliation `35366670108` took 0.024s after successful replay.
  Original main/WAL inode, size and mtime, main-header SHA and damaged-block SHA
  are unchanged. No checkpoint WAL exists. Machine `2870549b442d68` was
  destroyed at 16:08:56Z. The volume and preserved WAL receipts remain retained.
- Production `/ready` at 16:08Z: serving/accepting queries, zero active queries,
  OPS writes and delta publications, league fingerprint unchanged
  `sha256:935b1b973ff578d5`. No production machine/configuration change was made.
- Actual isolated attempts `35363995259` (576MB cache) and `35364562795` (768MB)
  reached guarded writable WAL replay but failed at 549.2 and 732.1 MiB managed
  memory, respectively, BEFORE the preservation-query/removal stages. Replay
  lasted approximately 5.26s and 6.59s. `35364801571` (2 GiB VM / 1536MB cache)
  reached the externally enforced 10s replay limit. Each result is UNKNOWN,
  never treated as a rollback or successful removal. Each machine was destroyed.
- Each writable attempt preserved the original 45516621-byte WAL create-only
  under its isolated-volume receipt directory. SHA256:
  `a4f7a2a20afdf2dc1cc218509c1f4052bf6f4df37924768fef518e8c53dace1f`.
  Subsequent preflight matched original main size/mtime/header/block and WAL
  size/mtime/digest before permitting replay. No DROP statement has run yet.
- `35364214730` could not launch its inspection VM because the volume host
  lacked capacity; it did not access the file. A later changed-memory recovery
  pilot launched successfully. No unchanged capacity-failure launch loop was run.
- Independent review held dispatch for parent-death termination and the correct
  v1.5.4 `.wal.checkpoint` suffix. Fixes `b5973ae10` / `0d164351c` also enforce
  outgoing and final phase budgets and disable spill on every stock open.
  Red Linux tests `35363579487` failed as intended; `35363693850` and
  `35363916275` passed. Linux actually kills a supervisor to verify child exit.
- The previously failing ESPN upload is `i_95_gridlock_league_2k27`, run
  `35287522419` attempt 5. Its upload error identifies the exact known damaged
  block 90714112. No import canary has been retried before physical recovery.

- Adapter orchestration is now on public main `d956ca93e`, mirrored locally.
  `35363125981` passes the six-case Linux matrix: 25 adapter tests in 3.02s;
  actual adapter removal function plus native five-name recovery in 3.460s.
  The workflow exposes an isolated-only `engine_recover` action, with actual
  attempt results above. It binds the runtime mount/file/build, preserves the
  45 MB WAL create-only, supervises cumulative phase budgets externally, and
  invokes stock verification in a new helper-free process. Unexpected phase
  transitions and early success are rejected; replay/commit timeout is UNKNOWN.
- `35362176139` (no engine open, 0.025s) reconciled post-timeout state: original
  main-file size/mtime, WAL size/mtime, main-header SHA and bad-block SHA all
  match their earlier baseline. Machine 683e3d6c449448 was removed.
- `35362459102` (`f4340320b`) removed spill churn without increasing VM size or
  time ceilings. Read-only replay with 576MB cache and no temp directory fails
  cleanly in 6.666s: cannot pin another 256 KiB at 549.1/549.3 MiB used. It
  confirms source/WAL file metadata unchanged. Process writes stayed at 8192
  bytes rather than the earlier 110 MB. Machine 286265dc442408 was removed.
  The next proof uses guarded writable replay, which can flush committed row
  groups normally; it does not skip, hide, truncate or delete the source WAL.
- Preservation witnesses now order years before display names and include
  saved manager-name overrides/franchise merges. Twenty-four local adapter
  tests pass, one Linux-only test skipped; Linux executes all 25 successfully.

- Public main `595aec4f3`, run `35360987274`: all six synthetic matrix jobs pass;
  adapter 16 tests in 1.11s; five-object corruption/recovery fixture in 10.901s.
  The stock engine proves the damaged metadata block is either unregistered or
  checksum-valid after repair, not merely that DROP no longer lists the object.
  Prior committed WAL data survives. Crash-after-commit, crash-after-flush,
  repeated recovery, unrelated-object rejection, and bounded allocation pass.
- Run `35360800968` (`57cdd8991`) closed four review findings: direct file/mount
  binding, WAL pathname replacement detection, retained commit-timeout logs,
  and bounded stdout/stderr capture. The four tests were red in `35360471890`.
- Real-volume engine identity `35358633104`: DuckDB 1.5.4, revision 08e34c447b,
  extension SHA256 9135828981e3d0bdc346c10f663e353eb12de486d352edd4af89651a926967a9,
  Python 3.11.16, x86_64, glibc 2.41. No database was opened for that check.
- Real read-only inventory `35359997115` stayed within its 10s external limit
  but timed out before returning the catalog. At 9.347s it recorded 537344 KiB
  peak RSS, 1.848s CPU, 173740032 process read bytes, and 110309376 process write
  bytes. These are process IO, NOT proof the source DB changed. Before-state
  retained the original 15555375104-byte file and 45516621-byte WAL. This run's
  post-kill file metadata has not yet been rechecked. No DROP was issued.
- All disposable pilot machines were removed; the recovery volume and WAL are
  retained. No unchanged WAL replay attempt has been resubmitted.
- Production health `35359880765` at 15:01:52Z: serving, accepting queries,
  zero active queries/OPS writes/delta publishes, both checks passing. League
  fingerprint sha256:935b1b973ff578d5. Startup timestamp and OPS hash changed
  externally during the turn; cause unknown. This work did not restart or
  mutate production. Revalidate the deployed image before any offline handoff.
- The real-file orchestration and actual isolated removal remain unfinished.
  A process shutdown is required for exclusive production ownership with the
  existing controls; there is no proven zero-downtime/hot-helper path.

## Real-file adapter continuation - 2026-09-18

The preceding goal-statement turn made no implementation progress. This resumed
turn verified public main at e079bb67d and added a real-catalog five-object
regression. Run 35357407714 (a50ae723a) correctly failed: all five exact
`___leagues.public` identities reached CommitDrop, but zero matched the old
synthetic-only guard. Main 023e6b5a1 adds a separately compiled five-name guard
and refuses unexpected armed removals with exit 99. Real-file use is NOT yet
enabled. The follow-up run 35357592441 hit the 5s compiler deadline on real_scope;
the other four scenarios passed. Optimization was removed from this tiny helper
instead of increasing the deadline. Updated Linux proof remains pending.

New adapter primitives have behavioral tests for actual runtime/mount identity,
bounded create-only WAL preservation, exact quarantined/replacement object sets,
sampled score/alias/aggregate value preservation, externally terminated stages,
and UNKNOWN outcome on removal timeout. Twenty-six local tests passed in 5.04s
(one existing hard-link test excluded on this filesystem). This is not an
end-to-end real-file adapter or a production recovery receipt.

A no-database-open engine_identity probe now fingerprints the actual extension
binary/revision/ABI for binding. It does not retry the earlier database-open/OOM
operation. Runtime capacity and deployed fingerprint are still to be measured.

Read-only maintenance audit found that existing close_pool drains queued
connections but cannot close borrowed connections, and its routes reopen the
pool automatically. The in-process merge lock and fleet repair-lane lock do not
fence all external writers. Production therefore needs a controlled full server
process shutdown and verified exclusive ownership, not helper injection. No
production shutdown or mutation has been performed. Startup WAL handling differs
between the two local trees; deployed-image verification is mandatory. No claim
of zero downtime is supported.

## Approved isolated engine-removal investigation - 2026-09-18

User explicitly approved isolated database-engine recovery/removal pilots,
with small tests before large writes. No production engine modification or
table removal has been performed. The experimental helper accepts only its
own synthetic temporary files, at most 8 MiB, and has no real-volume mode.

Hypothesis: bypass only row-group storage reclamation for one explicitly
armed DROP, leaving ordinary catalog transactions/checksums intact. This is
not a production repair design. Independent source review identified metadata
allocation, shared blocks, indexes, rollback and pre-checkpoint WAL replay as
required safety tests before considering a real-volume experiment.

The exact official Linux DuckDB 1.5.4 wheel exports the proposed function;
bounded 2.922s inspection downloaded 21,451,698 bytes into memory, checked the
published SHA256, and performed no installation or production access.
Initial synthetic Actions run `35349153174`, main `595cd614e`, stopped after
about three seconds because fortified C compilation rejects an ignored write
result. The helper now checks that return value; warnings remain errors.

A 2,633,728-byte generated fixture on local DuckDB 1.5.1 reproduces the right
failure boundary in 4.578s: healthy facts/history/aliases are readable, target
metadata reports checksum corruption, and ordinary DROP fails at COMMIT.
This is deliberately injected corruption in a disposable test fixture, not
a mutation or diagnosis of production data. Exact-version Linux execution
subsequently reproduced the same boundary; no production repair claim is made.

Exact Linux 1.5.4 run `35350290771` (`038c8347b`) passed in 1.844s:
the scoped DROP plus fresh-metadata allocation checkpointed, reopened in a
stock-engine process, preserved facts/history/aliases, and accepted another
ordinary write/checkpoint/reopen. Skipping row-group reclamation alone had
failed at checkpoint in `35349807881`, establishing the metadata-reuse gap.

The strengthened matrix `35352321840` (`0ceb281a0`) passed: normal 3.543s,
indexed 3.493s, shared-metadata refusal 2.543s. It covers exact catalog/schema
targeting, unrelated DROP forwarding, rollback, crash after COMMIT, crash
after metadata flush, replay without repeating DROP, repeated recovery,
stock-engine reopen/write, and unchanged healthy values. Shared catalog
corruption is refused with database and WAL sidecar hashes unchanged.
Earlier scope tests failed because ToSQL includes the catalog; diagnostic
run `35352238964` proved the exact `candidate.public` prefix before correction.

Read-only inventory `35352594800` (`14d3620d3`) targeted only existing
isolated volume `vol_4919j2m0wzg0xw5r`, with a 40s provisioning-plus-probe cap.
It read 274,432 bytes for the known-block check and found a 45,516,621-byte
retained WAL, DuckDB 1.5.4 / Python 3.11.16 / glibc 2.41. Opening read-only
then exited before catalog inspection or the post-read file-metadata check.
Read-only exit-log run `35352855026` proved an OS OOM kill at 409,048 KiB
anonymous RSS on the disposable 512 MB VM. That VM was destroyed; its volume
and WAL were not removed. Production was not involved.

Attempt `35353006359` requested a 1 GB disposable VM to leave OS headroom;
Fly refused startup with insufficient host resources. Attempt `35353155279`
instead requested 512 MB with a 128 MB DuckDB cache and a 16 MB spill ceiling;
Fly also refused that VM before database access. Neither attempted a DROP.
Do not repeat unchanged launches or migrate/copy the whole database to evade
this capacity condition. Wait for capacity on that existing volume's host.

Allocation-limit regression `35353358752` first failed as expected before the
limit existed. The final matrix `35353660883` (`4563c1c05`) passed with the
limit applied at actual MetadataManager::GetNextBlockId allocation, not only
the optional PeekNextBlockId branch. The synthetic helper allows at most 128
new metadata blocks (32 MiB at the tested block size); its one-block test
exits 97 before the next allocation and then passes stock-engine reopening.
This ceiling is not a blanket bound on all database writes or a tested
real-volume recovery budget.

No real-volume removal has run. Required next gates: exact deployed ABI
binding, retained WAL,
sampled healthy values and replacement aggregates, bounded one-object removal,
and fresh stock-engine checkpoint/reopen. Synthetic success is not approval
for production engine modification or proof of production recovery.

Sixteen guard/deadline tests passed in 2.48s; the unchanged hard-link
test was excluded on the D: filesystem. Python compile, Ruff, YAML and diff
checks passed. Both repository copies of the pilot workflow and helper match.
The new engine-probe job has no Fly credentials or volume; the Fly job is
excluded for that action. Installation plus experiment have an OS 40s cap.

## Whole-table replacement evidence audit - 2026-09-18

Independent read-only review distinguishes three different outcomes:

- The five complete aggregate tables were logically regenerated in 122.98s,
  as recorded below. That calculation was not the unresolved physical repair.
- The empty-shell rename/drop transaction is recorded as failing at COMMIT,
  but this continuation did not recover its original failure log. Do not
  describe it as a verified failed upload of five fully rebuilt tables.
- Removal pilot `35305006093` stopped at the retained-WAL guard before opening
  DuckDB. It did not test DROP. Rename-only run `35170289421` succeeded but
  did not prove checkpoint/reopen. The actual checkpoint failure is directly
  present in ESPN `35287522419` attempt 5 at physical location 90714112.

Application `_replace_canonical_table` already renames both tables, drops the
old target, then commits. Official DuckDB v1.5.4 `DuckSchemaEntry::AddEntry`
also implements CREATE OR REPLACE by dropping the existing entry; commit
reclaims the old table through `CommitDropTable`, which traverses row groups.
Thus this spelling does not establish an independent removal mechanism.
A 0.016s in-memory EXPLAIN diagnostic on local DuckDB 1.5.1 found TRUNCATE
and DELETE use identical DELETE/SEQ_SCAN plans. This is local-version evidence,
not an executed production-version recovery test. No forced-drop recovery
operation was established by this audit.

Fly remained serving/accepting with zero active reads, writes or publications
and league fingerprint `sha256:935b1b973ff578d5`. OPS fingerprint changed
externally to `sha256:86a101efc7dddef9`; no cause is inferred. No production
write, Actions dispatch, VM, restart, installation or database copy was made.
Rebuilding the current canonical tables again would not itself remove the
retained damaged objects. Physical repair and reliable updates remain open.

## Snapshot-range and existing-local-donor check - 2026-09-18

Investigated a distinct read-only possibility: accessing only the needed block
from an existing Fly snapshot without restoring a volume. Fly's documented
volume API exposes snapshot create/list and restore into a new volume, not a
snapshot byte-range/file reader. Installed `flyctl volumes snapshots --help`
also exposes only create/list; upstream export request superfly/flyctl#1296
remains open. No restore, volume creation or database copy was initiated.

Bounded metadata-only listings of known local recovery directories found no
full league-database donor. The 7.9 MB KMFFL archive is a logical league export,
not the original physical database. Previously created volume
`vol_vjyqjyyeke02x8ov` came from `vs_LaBDVOpGBKAT9NYpNYMe`, dated September 17
23:12 UTC, after the documented corruption; this is not evidence of an intact
donor. Its original rebuild run `35295482265` was cancelled. No new VM or
repeated damaged-block pilot was started. Direct Fly metadata access remains
unavailable locally because the tool has no Fly access token.

Readiness remains serving/accepting with zero active reads/writes/publications
and unchanged league fingerprint. OPS attachment metadata changed externally
to `sha256:2215b7a1cc1384bc`; its cause was not investigated or inferred.
Physical recovery remains unverified. External posting remains unapproved.

## Repeated storage blocker confirmed - 2026-09-18 12:13 UTC

Fresh read-only checks: Fly serving/accepting, zero active queries/writes and
unchanged league fingerprint; WAL remains 480.7 MiB (metadata query 0.719s).
New alert `35341793840` corresponds to ESPN run `35287522419` attempt 5,
job `105587328027`. Its 11:52:33 UTC upload error is the same checkpoint
checksum mismatch at 90714112, with the same computed/stored checksums.
This is new failure evidence, not a new recoverable condition. No rerun or
production mutation was initiated in this continuation.

The same physical-recovery blocker persists across the three latest goal turns.
The bounded source investigations have not established a safe in-scope repair.
The sanitized upstream question remains unsent: repeated keep-going instructions
do not authorize external posting or override the no-rebuild/no-architecture
constraints. Goal status is blocked, not complete. Resume meaningful recovery
when an approved supported narrow procedure or explicit new authority exists;
do not loop on identical repairs, readiness checks or adjacent code changes.

## Upstream recovery-path check - 2026-09-18 12:12 UTC

Checked official v1.5.5 release notes and PR #23714, including its full four-file
diff. That patch prevents column-drop metadata-index corruption; it does not
provide a recovery operation. The live writer's inspected code has no matching
DROP COLUMN operation; two local staging operations and a keeper-config
migration do not establish the incident's cause. No engine upgrade was applied.
The v1.5.5 row-group drop implementation still traverses stored segments and
the block reader still rejects checksum mismatches. Neither supplies evidence
that upgrading would remove the current damaged reference.

Readiness remained serving/accepting with zero reads/writes/publications and
the same league fingerprint. No production mutation, pilot VM, build, install,
snapshot or restart was performed. No repeated failed pilot was dispatched.
Prepared `duckdb-storage-recovery-question-2026-09-18.md`, a sanitized, explicitly
non-reproduction support question. Asked permission before external posting;
it remains unsent. Recovery is still unverified; this source investigation is
not a repair or a successful refresh.

## Proven-repair claim reconciled - 2026-09-18 12:06 UTC

Re-read the original GitHub logs, not just this ledger. Successful isolated
run `35170289421` reports only the five `__corrupt_recovery_*` renames; it
contains no checkpoint/reopen proof. ESPN run `35287522419`, attempt 4,
explicitly reports checkpoint failure at location 90714112 with computed
checksum 5168518579405463287 versus stored 18392342689821271652.
The scoped reaggregation receipts' `checkpointed:false` is not itself a new
checkpoint failure: `_scoped_recovery_checkpoint_result` deliberately defers
checkpointing when disabled. Those receipts prove neither physical repair nor
successful restart. The prior conclusion that quarantine/reaggregation alone
resolved the storage blocker was too strong.

Inspected official DuckDB v1.5.4 sources: `CommitState::CommitEntryDrop` invokes
`DuckTableEntry::CommitDrop`, then `DataTable::CommitDropTable` traverses row
groups to reclaim blocks. Rename avoids that path but retains the damaged
storage. The proposed move-to-schema route is unavailable: the v1.5.4 parser
has no AlterObjectSchema statement handler. No new safe SQL removal method
was established; no repeated removal pilot was launched.

Fly readiness still reports serving/accepting with zero active queries, OPS
writes or publications and unchanged league fingerprint
`sha256:935b1b973ff578d5`. Last bounded WAL metadata read was 480.7 MiB,
above the existing recovery stop. No production mutation, restart, deployment,
snapshot, new volume, raw-block modification or code fix occurred here.
Remaining blocker: a verified durable repair of the damaged physical reference,
not aggregate calculation speed. Do not retry the logical repair or claim
completion without successful checkpoint/reopen evidence.

## Bounded continuation - 2026-09-18 07:44 UTC

Fly was serving/accepting with zero active queries, OPS writes and publications;
league/OPS fingerprints are unchanged. No production write, restart, deployment,
restore, diagnostic VM or storage modification was attempted. The physical
checkpoint fault and last observed 480.4 MiB WAL still block publication canaries.

Found a distinct worker deadline defect: all three workflows used GNU timeout
with TERM and --foreground. TERM can be ignored and foreground mode excludes
descendants from the timeout. Six real process tests reproduced a child running
after the accelerated deadline (refresh and failure-status steps for each
platform); two timely exit controls passed. The red selection took 22.44s.

The same six commands now send KILL to their local process group. The existing
absolute 105s deadline, reserved finalization time and 8s failure-status budget
remain unchanged; no provider/pipeline/publication code changed. Tests execute
the workflow's actual timeout flags against TERM-resistant Bash parent/child
processes, with only duration accelerated to 0.35s. Fixtures finish naturally
after about 2s if the deadline fails, and every test has a 5s parent timeout.
Git Bash on Windows returns a different native kill status (2304); tests check
nonzero there and 137/-9 on POSIX, plus no child-overrun output and bounded time.
The eight process cases plus 72 existing worker/runtime contracts passed in
4.55s under a 38s command cap. Ruff and diff checks passed. Linux verification
is added to the existing public boundary job as a 38s-capped, no-production-access
test step, not a new worker or deployment.
Independent read-only review found no critical or important merge blocker.
Public main commit `53795315296a606dc56a2f2e06215d5cbd0d7221` passed actual Linux
process verification in run `35320942120`: eight process tests in 2.16s, seven
boundary/migration tests in 0.03s, entire job twelve seconds. Final readiness
remained serving/accepting, zero reads/writes/publications, unchanged hashes.
This is execution of the deadline diagnostic, not a league-refresh canary.

These three workflow files and the boundary workflow do not exist in the dirty
root-app checkout. No replacement duplicate files or new pipeline were created
there; the canonical public executing copy is the only existing target.

Limits: this is a local-runner process-group deadline, not a guarantee that a
native Fly operation is cancelled, or that every setup/finalization step and
queue delay fits the click-to-visible target. A forced kill before a confirmed
receipt is saved can still require remote commit reconciliation. Detached child
process groups are not covered. Storage repair and actual full UI/provider
publication canaries remain unverified; no completion claim is made.

## Bounded continuation - 2026-09-18 07:37 UTC

Fly readiness remains serving/accepting, with zero active queries, OPS writes
and publications; league and OPS fingerprints remain unchanged. This turn used
no production write, deployment, restart, snapshot, restore or diagnostic VM.
The known physical block fault and last verified 480.4 MiB league WAL remain
the production recovery/publication blockers; readiness does not prove repair.

Confirmed and removed process-exit watchdogs from the two existing admin query
writers. Ordinary autocommit DDL/DML can checkpoint without CHECKPOINT appearing
in SQL, so exempting explicit checkpoint SQL alone did not protect those writes.
Query interruption, locking, cleanup, OPS reopening/state restoration and
checkpoint behavior remain intact. The unused RW_HARD_EXIT_SECONDS setting is
removed. No extra query, fetch, pipeline, dependency or schema change was added.

Two real HTTP/tiny DuckDB tests reproduced an armed kill callback immediately
before an INSERT that actually checkpointed (test-only 1-byte threshold, WAL
empty afterward). This proves the unsafe armed window, NOT execution inside
DuckDB's checkpoint and NOT the original corruption's cause. Two positive
tests prove real slow queries still return HTTP 504, roll back the statement
and recover readiness. Initial harness Timer.run incorrectly waited its full
interval and was stopped by the 38s subprocess cap; setting the injected timer
interval to zero produced the intended two failures/two passes in 4.03s.
After the fix, all four passed in 3.71s. Ten existing admin-write/cleanup/lock/
checkpoint checks passed in 7.10s. Root-app mirror tests: four passed in 8.73s.
Ruff and diff checks pass; independent read-only review found no merge blocker.
The final combined selection passed all 14 tests in 9.32s. Public main contains
`e03a4247553af562f954fc14991ab7cf42f6782f`; boundary run `35320345784` passed that
exact revision in eight seconds. Final readiness was serving/accepting with
zero active reads/writes/publications; Northern League returned HTTP 200 in
0.186s. These availability checks do not establish data freshness/correctness.

Only equivalent narrow hunks were applied to the dirty root-app server file,
preserving its other differences; the test is mirrored. Canonical executing
repository is verified public jeleff1000/mfl-league-fetcher, default main.
Server deployment remains manual and was NOT dispatched. This is prevention
source verification, not deployed verification or physical storage recovery.

Open limitations: a native call that ignores interruption can outlive the
external 120s worker cap and retain server locks. Legacy merge, OPS snapshot
and cross-connection process-kill hazards are not closed by this patch.
Full provider/UI canaries, the remaining recovery cohort, weekless draft
routing and physical checkpoint recovery remain unverified/unresolved.

## Bounded continuation - 2026-09-18 07:19 UTC

Fly remains serving/accepting with zero active queries, OPS writes and
publications. Northern League returned HTTP 200 in 0.264s. One metadata-only
WAL check took 0.672s and still reports 480.4 MiB; database fingerprints are
unchanged. No production write, restart, deployment or diagnostic VM was used.
Physical storage recovery and production publication canaries remain blocked.

Confirmed another publication-recovery gap: all three active-season workers
saved their COMMITTED receipt only after diagnostic count queries and local
cleanup. Any failure there could hide an already successful data commit from
the workflow's existing cache-only recovery handler. The workers now save the
confirmed bundle receipt immediately after Fly returns COMMITTED, before those
steps. One shared writer atomically replaces the local JSON and saves before
printing; interrupted diagnostic rewrites and broken stdout cannot truncate or
discard the earlier receipt. Source-manifest completeness is not changed.
There are no new Fly calls, fetches, transformations or publication changes.

Two regression cases reproduced the existing stdout-before-file failure; eleven
new receipt-boundary cases failed before the shared helper existed. Three added
real in-memory DuckDB lifecycle tests verify that the earliest saved receipt
can become committed_cache_pending and complete through existing cache-only
recovery for Yahoo, ESPN and Sleeper, without a second publication. A newer
publication generation rejects that recovery. Independent review caught a
Yahoo CLI import-path regression before push: an isolated subprocess reproduced
the missing `scripts` package, with network denied and startup stopped before
reader construction. Yahoo now has the same two-line root bootstrap as ESPN
and Sleeper. All three CLI startup tests pass; review found no remaining scoped
blocker. Receipt/status/workflow tests: 120 passed in 8.86s. Manifest-coverage/
claim tests: 26 passed in 0.76s. Both commands had 38s hard caps. Ruff, diff checks
and public boundary validation pass.
Fix pushed to canonical public main as `cef9cf5f847c295bd310e636b049a54977363aad`;
boundary guard `35319172793` passed that exact revision in an eight-second job.
This is source/boundary verification, not a production data-update canary.

Limits: these tests do not run full provider ingestion or actual UI canaries.
They do not close a runner kill before the first receipt save, an ambiguous Fly
response, or loss of the runner filesystem before status persistence. This is
the confirmed-response-to-later-diagnostics gap only, not storage repair.

Read-only draft-routing investigation also confirmed the weekless draft gap
remains OPEN. The existing offseason helper is not yet a safe drop-in: it
requires existing draft/matchup history, the Sleeper draft-only fetch does not
apply franchise_merges, and its publication handoff lacks the weekly claim and
receipt integration. No alternate pipeline or draft routing was introduced.
These worker modules exist only in the canonical public worker checkout; no
new duplicate app-repo copies, branch, worktree, snapshot or rebuild was made.

## Bounded continuation - 2026-09-18 07:01 UTC

Production readiness timed out twice (10s/8s) at the start of this continuation,
then readiness, health and server-state returned serving with zero queries and
publications. No restart, diagnostic VM, deployment or write was issued. League
attachment metadata changed to 06:52:14 but the league fingerprint remained
`sha256:935b1b973ff578d5`; OPS changed externally to `sha256:f66c11fc16f14413` at
06:53:14. These observations do not prove a restart or explain the brief timeout.
A single metadata-only `pragma_database_size()` read took 0.859s and still
reported the league WAL at 480.4 MiB. Thus the write-headroom blocker is unchanged.

Confirmed and fixed another narrow prevention gap in delta/fleet publication:
their process-exit timer was armed before connection/setup and remained armed
after rollback during autocommit failure-receipt writes. Those writes can invoke
automatic checkpoints. Six actual HTTP/DuckDB tests reproduced the pending kill
callback at setup, VALIDATED and FAILED_MERGE; two transaction positive controls
passed. The timer now arms only after BEGIN and is cancelled before rollback,
failure reporting and cleanup. Existing pre-COMMIT cancellation is retained.
No new queries, dependencies, hydration, pipeline or publication format changes.

The eight new tests passed in 19.10s. Existing timer/commit/receipt regression
selection: seven passed in 5.63s. Existing replay, generation and older-bundle
selection: six passed in 11.88s. Each command had a 38s subprocess cap. Ruff and
diff checks passed after removing a trailing blank in the new test; independent
review found no blocker. Public main `5a6288db733a5001ad8d4feade6d194f8dca5633`
passed boundary guard `35317470542`. The root-app mirror
received only the same narrow hunks and new test, preserving unrelated changes.

Deadline qualification: the server process-kill budget now starts after BEGIN,
not at connection setup. Setup/failure cleanup deliberately have no process-kill
timer. The external 120-second worker cap is unchanged but does not terminate
server-side work. Legacy/admin timers and cross-connection process-kill hazards
remain OPEN. This patch is NOT proof of the physical corruption's original cause.

The server patch is NOT deployed: current startup checkpoint protection and the
known bad block make a Fly restart unsafe. Existing storage damage, twelve
remaining scoped reaggregations and production publication canaries remain
blocked. No snapshot restore, rebuild or storage-architecture change authorized.

## Bounded continuation - 2026-09-18 06:51 UTC

Tested a distinct remaining donor hypothesis: an intact duplicate of the damaged
metadata block might remain at another offset on the already-existing September
17 recovery volume, even though the September 15 witness had no matching header.
Public main `c5dbdf2e41a6ec90da08e490d126875fc30ed8f1` permits only the byte-only
`retained_headers` action on exact `vol_4919j2m0wzg0xw5r` /
`wkupd_rebuild_35166181636`; the SQL donor action still rejects that volume.
No new volume, restore, database copy, SQL connection or production mutation.

Run `35316606712` inspected all 59,339 physical block headers. The only matching
header was the known corrupt block 346: stored checksum 18392342689821271652,
computed checksum 5168518579405463287. No intact duplicate was found. This rules
out this donor hypothesis; it is NOT a repair. Do not repeat the same scan.
The helper took 15.292s, pilot 26s including startup, full job 40s including
cleanup/artifact/setup. It read 474,712 logical header bytes plus 548,864
validation bytes and the initial 274,432-byte damaged-block probe. Physical
filesystem I/O may be greater. Disposable machine `e8204deb3e1748` was destroyed;
the original isolated volume and WAL were retained.

The new action/volume authorization test failed before implementation. A real
small DuckDB-file test also distinguishes an invalid original from a valid
retained duplicate without opening SQL during the probe or modifying the file.
The two-file test selection passed 24 tests in 1.99s under a 38s cap; Ruff,
workflow YAML parsing, mirror equality and diff checks passed. Independent
review found no blockers. Public boundary guard `35316597901` passed exact SHA.

Afterward production remains serving/accepting with zero queries, OPS writes
or publications; both database fingerprints are unchanged. Northern League's
overview returned HTTP 200 in 0.134s. Availability is not refresh correctness.
No deployment, restart, write, checkpoint retry, snapshot, restore, rebuild or
architecture change was performed. Physical removal remains unproven; new
snapshot-backed donor inspection and storage-boundary changes remain unapproved.
Do not substitute more reaggregation or unchanged pilots for that blocker.

## Bounded continuation - 2026-09-18 06:41 UTC

Current source inspection corrected a stale audit note: historical PPG already
uses the existing OPS precomputations in `40778c937`. The three existing real
transformation regressions passed again in 12.84s under a 38s subprocess cap.
No second PPG fix, NFL download, provider fetch, or production write was made.

A separate confirmed freshness defect was still present: `build_refresh_plan`
retains changed historical partitions in `weeks_by_season`, but the three active
workers assessed source completeness only from their active-year provider fetch.
Thus a completed active-year fetch could acknowledge the entire captured
manifest, including an untouched older correction.

One shared in-memory coverage predicate now compares the captured plan's seasons
and weeks against the actual active partition. Yahoo OAuth, ESPN and Sleeper all
use it before setting `source_manifest_complete`. Missing plan, wrong year,
unfetched week or any other-season partition leaves freshness pending. Provider
draft/game/outcome checks still apply. No query, retry, dependency, new pipeline,
historical hydration or publication-schema change is added.

Twenty-one new cross-platform cases cover real provider completeness helpers
and real in-memory DuckDB commit/cache/success transitions. Bypassing the new
predicate reproduced three incorrect historical-digest promotions; the normal
guard retains the old published digest while preserving successful active-data
and cache status. Focused coverage/planner/status/pending-game suite: 76 passed
in 1.77s. Provider/worker-contract/coverage suite: 126 passed in 5.69s. Ruff and
diff checks pass. Independent review found no blocker after the existing
completeness fixtures were supplied explicit captured plans.

This closes false whole-chain acknowledgment only. Older-season correction
execution and weekless draft-only execution are NOT implemented by this patch;
the active workers still do not consume those other partitions. Physical storage
recovery and live publication acceptance remain blocked. No server deployment,
Fly restart/write, restore, snapshot, rebuild or architecture change was made.
These worker modules exist only in the canonical public worker checkout; no
duplicate root-app worker files or new branch were created.

## Bounded continuation - 2026-09-18 06:26 UTC

The remaining retained-block donor hypothesis was tested, not assumed: an
intact block might remain outside the registered metadata set on the existing
September 15 recovery volume. Public main `cbf3721cd72313ab17fa77da270d27d66cfa1a2f`
adds an exact-volume-allowlisted, read-only header mode to the existing pilot.
It opens no DuckDB connection, creates no link/copy/volume, rejects primary,
caps inspection at 65,536 block headers and two full candidates, and retains
the 40-second external deadline including disposable-machine startup.

Run `35314739529` inspected all 59,270 physical block headers of existing
`vol_vp26dp2g9x3167j4`: 474,160 logical header bytes, plus two 274,432-byte
validation probes. Filesystem physical I/O may exceed logical bytes. No block
header matched the expected checksum `18392342689821271652`; `candidates=[]`.
The helper took 18.584s, storage pilot 33s including startup, full job 49s.
Machine `84ed45ece65578` was destroyed; the original volume and WAL remain.
This rejects this donor, not every recovery method. It is NOT a repair or
safe-publication proof. Do not repeat this search unchanged.

Four new tests failed before implementation, then passed. The complete two-file
selection passed 22 tests in 2.60s under a 38s subprocess ceiling; Ruff passed.
Independent review found no critical/important issue. Both workflow/helper/test
mirrors match. Public boundary guard `35314730441` passed on the exact revision.

Production `/ready` after the pilot remains serving/accepting with zero active
queries, OPS writes and publications. League fingerprint remains
`sha256:935b1b973ff578d5`; OPS remains `sha256:c285f63fef646490`. Northern League
overview returned HTTP 200 in 0.308s. No production writes, deployments,
restarts, checkpoint attempts, restore, rebuild or architecture change occurred.
Direct Fly snapshot listing is unavailable without a CLI token; this did not
trigger a restore or any credential change. Actions authentication is intact.

Physical recovery, twelve pending scoped aggregate repairs, and live publication
canaries remain blocked. No verified in-place removal exists under current
constraints. Inspecting a newly restored older isolated snapshot remains
unapproved; do not treat repeated "keep going" as approval to restore, replace
the live database, rewrite checksums, or change storage architecture.

## Bounded continuation - 2026-09-18 06:13 UTC

Confirmed another prevention gap, distinct from physical repair: the delta and
fleet merge watchdog remained armed during explicit COMMIT and was cancelled
only before the later explicit CHECKPOINT. DuckDB v1.5.4 can automatically
checkpoint inside CommitTransaction before COMMIT returns (official source:
https://github.com/duckdb/duckdb/blob/v1.5.4/src/transaction/duck_transaction_manager.cpp).
Two actual HTTP/DuckDB regressions reproduced the pending kill callback firing
at that boundary. This establishes a hazardous ordering, NOT that it caused
the existing block corruption.

The scoped fix uses one shared commit helper to disarm before COMMIT, retaining
the existing query-interrupt timeout and all worker deadlines. Review found
that cancel alone misses an already-running callback. A regression reproduced
that race. A join alternative was rejected: a deterministic logger-stall test
proved it could block publication waiting for logging. The final small timer
subclass serializes cancellation with the exit decision under one lock; logging
remains outside it. No joins, waits, queries, I/O, changed deadlines, checkpoint
settings or new dependency are added to normal publication.

Nine focused checks pass (four in 4.13s, five in 6.18s), including real
delta/fleet publication, in-flight callback cancellation, still-armed expiry,
retained query interruption, post-commit receipt preservation, validation
rollback and idempotent replay. Each validation subprocess has a 38s ceiling.
Independent review has no remaining blocker. Root app copies receive only the
narrow matching changes, not an overwrite with the public server's newer file.

This closes only the explicit delta/fleet COMMIT watchdog window. Legacy and
admin autocommit writes and pre-transaction state writes can still checkpoint
while other process-exit watchdogs are armed; that broader policy remains OPEN.
The prevention fix is not deployed: no restart is safe to infer from these
local tests while the existing damaged checkpoint remains unresolved.

Fresh readiness: serving, accepting queries, active queries/OPS writes/delta
publications all zero, unchanged league fingerprint sha256:935b1b973ff578d5.
The OPS fingerprint changed externally to sha256:c285f63fef646490 at 06:06:15;
this continuation performed no production writes. No recovery VM, restore,
rebuild, checkpoint, replacement, or deployment was launched in this continuation.

## Bounded continuation - 2026-09-18 06:00 UTC

Production remains serving with zero active reads, OPS writes and publications
at the latest readiness check. No primary restart, publication, database copy,
snapshot restore, checksum modification or architecture change was performed.
The known physical checkpoint blocker remains OPEN; green diagnostic jobs below
do not demonstrate removal, repair, or safe production publication.
Northern League's live overview returned HTTP 200 in 0.549s with summary,
standings, rankings and rivalries fields. That proves current page availability,
not fresh-score correctness or a successful new update.

Read-only header pilot `35311940464`, public main `6a21ed59c`, took seven
seconds including disposable-machine startup. The existing oldest recovery
volume has checkpoint iterations 12125 and 12126 pointing to different catalog
roots; neither header receipt proves an intact older table. It inspected only
the same 274,432 bytes already used by the block probe. The machine was removed.

The September 15 volume `vol_vp26dp2g9x3167j4` already existed. Earlier evidence
checked only the damaged offset there; the new hypothesis was that the expected
block could exist at another registered metadata location. Public main
`78ca1a65832d367aef6c564d99979ef758a8888d` adds a strictly read-only, exact-volume
allowlisted probe to the existing 40-second pilot, mirrored in the app repo.
A temporary hard link opens the checkpoint read-only without replaying, moving
or deleting its retained WAL. There is no database copy or new restored volume.
The probe caps metadata headers at 4096 and full candidate blocks at two; it
never authorizes a repair. Hard termination can leave the temporary hard link
on the isolated volume; normal completion removes it. Production is rejected.

Run `35312723061` completed the pilot in 15 seconds including machine startup
(helper 1.972s), using production DuckDB 1.5.4. It checked 1351 registered
metadata-block checksum headers (10,808 bytes, excluding catalog-open I/O and
the initial 274,432-byte block/header probe). **No checksum candidate exists in
that registered set.** This rejects this donor hypothesis, not all possible
recovery methods. Disposable machine `8d96509c194998` was destroyed; existing
volume and WAL were retained. The full Actions job took 36 seconds.

Twenty-seven focused tests passed in 2.64 seconds on a fresh C-drive temporary
directory, including real DuckDB database/WAL preservation. D-drive testing
first failed because its filesystem does not support hard links; no product
fallback or database copy was added. Independent review found no blocker and
noted the timeout hard-link cleanup caveat above. Both workflow/helper mirrors
match exactly. This is diagnostic progress, NOT completion of physical repair,
the twelve remaining aggregate recoveries, or all-platform UI canaries.

The previously requested new-snapshot restore remains unapproved and unperformed.
Do not repeat the rejected single-bit, same-offset donor, registered-metadata
donor, WAL-replay connection, or DROP attempts unchanged. A safe in-place
removal has not been established within current scope.

## Bounded continuation - 2026-09-18 05:40 UTC

Fresh `/ready` is serving with zero active reads, OPS writes and publications;
the league file hash is unchanged. A metadata-only `pragma_database_size()`
read took 0.641s and confirms the WAL remains 480.4 MiB. Ordinary imports do
not have the recovery client's 480 MiB stop. No new publication, restart,
checkpoint, database replacement or snapshot restore was launched.

Read-only pilot `35311156745`, public main
`38aefcdf1499635aac9e630c23f1201652d086e1`, tested the previously untested
single-bit corruption hypothesis against the existing oldest recovery volume.
It read only 274,432 bytes and took five seconds including machine startup
(helper elapsed 0.058s). The payload and checksum both have zero single-bit
candidates. No bytes were modified. Machine `48ee5d1ced5638` was destroyed;
the existing volume was retained. This rejects a single-bit repair, not every
possible physical fault. The diagnostic explicitly never authorizes a repair,
including when a candidate exists. Fifteen diagnostic/pilot tests passed in
2.67s, including checksum ambiguity and file-preservation cases. Algorithm
reference: DuckDB v1.5.4 `src/common/checksum.cpp`.

Another bounded defect is now regression-tested: shared JSON reads, Parquet
reads and SQL writes retried permanent storage failures six times, wasting
30s read backoff or 60s writer backoff. Uploads already rejected checksum
errors, but still spent 270s default backoff on an invalidated-database error.
Ten new red cases reproduced the repeated requests. One shared string-only
classifier now rejects identical retries for corruption/checksum mismatches
and invalidated databases. It adds no requests and preserves transient retries.
Both delta/fleet paths still reconcile COMMITTED receipts after an HTTP 500:
one publication request, one receipt request, no republish/backoff. Sixty-one
focused tests passed in 13.03s; independent review found no blockers. This is
retry amplification containment, NOT physical storage recovery or successful
production publication. The older root checkout's differing client APIs were
not overwritten; executing public workers consume the canonical public copy.

The prior 30 scoped aggregate repairs remain separate from the twelve pending
aggregate repairs and absent-fact/credential cases. The one damaged metadata
block still prevents checkpointing. No safe in-place physical repair has been
established under the no-restore/no-rebuild/no-architecture-change constraints.
An older isolated donor-block snapshot probe remains unapproved; do not perform
it or repeat unchanged failing DROP/connect pilots.

## Bounded continuation - 2026-09-18 05:00 UTC

Byte-only pilots `35308583724` and `35308584901` each took 11s including
disposable-machine startup. Each read 274,432 bytes from a different existing
recovery volume, without opening DuckDB. Both contain the same damaged block;
neither is a usable replacement witness. Their disposable machines were removed;
volumes were retained. No snapshot restore, copy, rebuild or production mutation.

Read-only locate pilot `35308750221` on the oldest existing recovery volume
reached `connect_start` but not catalog inspection before its absolute deadline.
The remote process exited and owned machine `48ee5d1ced9538` was destroyed.
This is a bounded failure, not successful SQL-level localization/removal. Do not
repeat it unchanged. Physical recovery remains open.

Fresh readiness is HTTP 200 / serving, with zero active queries, OPS writes or
delta publications. The 480.4 MiB WAL headroom stop remains in effect. Current
main's stricter startup checkpoint behavior has NOT been deployed over this
known checkpoint failure; doing so could prevent service startup.

Worker regression verification: 143 focused Yahoo/ESPN/Sleeper, preservation,
writer-boundary and claim tests passed in 14.69s. This is local regression
evidence, not a new live provider-refresh result.

The existing shared PhaseTimer previously withheld stage timings until the final
receipt. A regression failed on absent immediate output. The five-line change
flushes each completed phase and its elapsed time immediately, retaining the
same receipt and adding no queries, dependency, pipeline or retry. Timing plus
worker-contract tests: 72 passed in 0.39s. Review identified closed-output errors
escaping the timer; three regressions reproduced this, then passed with a guard
around only the diagnostic print. Timing validation still raises. Nine focused
timing/projection tests passed in 7.23s; the actual cache-patch caller's three
success/failure-output cases passed in 1.25s.

Forty local integration/delta/lineage tests passed in 20.28s. Two live-data,
non-publishing worker pilots had 38s subprocess kill deadlines and no installs:
NYU Sleeper returned NO_FINALIZED_WEEKS in 5.828s (no provider fetch exercised);
TFL ESPN returned DRY_RUN_READY in 29.828s, validating 12 teams, 192 roster rows,
12 finalized matchup rows and 12 transaction rows. These are NOT publication
canaries. No credentials were displayed and neither pilot wrote league data.

Existing Yahoo run `35297219552` separates a 72.006s processing path into 3.782s
provider fetch, 11.574s shared transformations and 33.393s Fly publication.
The latter includes 9.8669s seasons, 4.808s careers and 15.3293s homepage work.
These measured costs are not a new performance improvement claim.

A second concrete defect was reproduced through both real HTTP publication
paths: commit the transaction, then inject a checkpoint/reporting exception.
Both paths overwrote the committed receipt with FAILED_MERGE despite the data
being committed. The shared state helper now checks for an existing COMMITTED
receipt only on FAILED_MERGE/CONFLICT transitions and preserves it. There is no
extra successful-path query and no new retry path. Existing status/idempotent
replay can now recover the commit. Ten HTTP tests passed in 28.44s, covering
these two cases, genuine rollback, full-history aggregation and repeat receipts;
23 existing DataFrame-fragmentation warnings remain. Independent review found
no remaining blockers in the logging and receipt guards. Final timing/worker
contracts: 75 passed in 0.37s. These scoped fixes are being pushed to public
main; the server change is NOT deployed. Fresh production readiness after all
checks remains serving / zero active reads or writes / unchanged league hash.

The fixes are now on public main `56f274f14792c14e5bf3b5cd6976cfb034ea8333`.
No server deployment, machine restart or checkpoint was requested. The worker
timing change is available to subsequent main executions; the server receipt
guard and preceding OPS attachment release remain undeployed.

The specifically reported failed import `35287522419` targets
`i_95_gridlock_league_2k27`. Its upload log reports the exact known checkpoint
checksum failure / invalidated database. A fresh bounded league-filtered read
finds zero matchup rows; the latest saved bundle states are VALIDATED, not
COMMITTED. Thus this is NOT an already-successful import whose cache merely
needs expiring, and it must not be marked recovered. No retry was launched
against the nearly-full WAL.

The continuation inventory now covers 92 additional executions after the old
September 16 16:58:35 UTC cutoff, through this September 18 inspection, across
34 leagues. See `update-league-cohort-continuation-2026-09-18.csv`. Ninety-one
identities came from workflow logs; NYU run `35130659510` is identified by the
earlier UI/publication receipt in this ledger (its log API failed). GitHub-only
inventory processing took 16.828s plus a 2.844s UTF-8 decoding retry. No Fly load.

Twelve league identities were outside the old inventory. `go_pats_2021` was
already separately checked with missing facts. Eleven new individual read-only
key checks each took 0.609-0.672s, with the process completing in under eight
seconds. `monsters_of_the_midway` has all 11,870 expected player-season keys;
that key check alone is not numerical/ranking validation. Ten others have
readable player-week facts but zero canonical player-season keys:

| League | Missing player-season keys | Persisted source years |
| --- | ---: | --- |
| `a_good_day_to_dynasty` | 8,461 | 2021-2025 |
| `a_league_has_no_name` | 5,787 | 2018-2026 |
| `aaron_rodgers_hates_his_family` | 3,870 | 2020-2025 |
| `bitter_a_old_guys` | 6,532 | 2016-2025 |
| `chuck_noris_is_god` | 9,971 | 2011-2025 |
| `dynasty_849e` | 1,229 | 2024-2025 |
| `g_club` | 13,905 | 2018-2026 |
| `legacy` | 167 | 2026 |
| `the_acl` | 3,470 | 2022-2026 |
| `the_rubes` | 13,577 | 2006-2026 |

These are pending the same already-proven scoped reaggregation, not full-import
or full-database rebuild candidates. No write was started past the WAL headroom
stop. The recovered 30 must not be presented as every affected league.
An explicit decision was requested before any older-snapshot probe: Fly would
require a full-size isolated restored volume even though only one block would
be examined. No restore has been authorized or performed in this continuation.

## Scoped aggregate recovery works - 2026-09-18

This evidence supersedes the earlier assumption that physical-block removal must
precede every logical aggregate recovery. Successful import `35305903072`
published `crosswater_pigskins`: its matching bundle receipt is COMMITTED,
38 tables / 0.2 MB merged in 2.7s, and cache warming succeeded. Existing pooled
reads and commits work; this does not prove the damaged block can checkpoint.

The already-deployed `/reaggregate-damaged-derived` endpoint with
`mode=scoped_rebuild` rebuilt only the five allowlisted aggregate tables for
each league below, using canonical aggregators over persisted facts. No provider
fetch, full database copy/replacement, quarantine DROP, restart or deployment.

| League | Server work | HTTP elapsed | Player-season keys reconciled | Missing keys / numeric mismatches |
| --- | ---: | ---: | ---: | --- |
| `nyu_ffl` | 2.694s | 3.719s | 5,904 | 0 / 0 |
| `tfl_of_extraordinary_gentleman` | 2.275s | 2.875s | 10,043 | 0 / 0 |
| `kmffl` | 1.918s | 2.531s | 8,058 | 0 / 0 |

Numeric witnesses reconcile fantasy points, manager LAMAR, started-player clutch
and games against persisted player-week facts. An initial NYU witness incorrectly
included bench clutch; using the canonical started-player definition removes all
339 apparent mismatches. Matchup counts/history and identity hashes were unchanged:
NYU 1,608 rows / 2018-2026; ESPN 2,760 / 2012-2026; KMFFL 1,820 / 2015-2026.
This preserves the history currently present, not proof of older missing seasons.
Existing import cache-expiration helper succeeded for all three; live overviews
returned 12, 15 and 10 manager rankings respectively, with current standings.

All three responses report `checkpointed:false`. Physical checkpoint corruption
remains unresolved. The deployed response lacks the generation field present on
current main; deployed revision parity is not established. HTTP requests were
bounded to 35s and returned within four seconds, but client timeout is NOT a
server-side cancellation guarantee. Full refresh/UI acceptance is still open.

Continuation through the previously identified update-attempt cohort uses the
same single-league endpoint and the same reconciliation. Each row below has
zero missing player-season keys, zero points/LAMAR/started-clutch/games
mismatches, unchanged matchup counts/year bounds/identity hash, successful cache
expiration and HTTP 200 overview with populated rankings. Times include network;
the final column also includes before/after checks and live cache verification.

| League | Rebuilt player-season keys | Rebuild HTTP | Repair/check/cache total |
| --- | ---: | ---: | ---: |
| `the_real_ff_league` | 2,077 | 3.219s | 7.391s |
| `the_chulent_bowl` | 4,802 | 2.234s | 6.062s |
| `playing_for_keeps_league` | 3,527 | 2.109s | 5.828s |
| `afi_data` | 432 | 2.937s | 8.953s |
| `always_sunny_in_emmitsburg` | 8,485 | 3.094s | 7.938s |
| `bethany_beach_league` | 1,826 | 2.891s | 8.078s |
| `bfl` | 2,521 | 2.531s | 7.094s |
| `clemson_fantasy_league` | 10,968 | 3.078s | 7.406s |
| `dom_s_year` | 26,357 | 3.469s | 8.172s |
| `fanball_3e8d` | 4,149 | 3.797s | 8.125s |
| `fantasy_football_8ad9` | 398 | 2.422s | 6.359s |
| `fight_club_except_we_do_talk` | 3,977 | 3.219s | 7.438s |
| `franchise_mode_fantasy` | 6,231 | 2.625s | 6.609s |
| `group_chat_foosball` | 4,971 | 2.531s | 6.312s |
| `handegg_dynasty_league` | 5,984 | 2.110s | 5.922s |
| `joes_pussy` | 3,983 | 2.875s | 7.078s |
| `l_78_shootas` | 4,060 | 2.203s | 6.328s |
| `live_draft_beer_league` | 11,599 | 2.703s | 6.891s |
| `mawhinney_s_vixens` | 7,183 | 2.391s | 6.297s |
| `mirabeau_fantasy_football` | 2,729 | 2.468s | 7.157s |
| `new_league_same_result` | 1,842 | 2.797s | 6.984s |
| `not_for_long` | 11,138 | 2.735s | 6.719s |
| `pimps_and_ochos` | 10,426 | 2.703s | 7.531s |
| `superleague_v2_0` | 7,088 | 2.453s | 7.125s |
| `the_beata_cup` | 8,688 | 2.640s | 7.640s |
| `the_dfb_league_ii` | 2,842 | 2.297s | 6.688s |
| `the_fucking_catalina_wine_mixer` | 3,601 | 3.015s | 7.484s |

Fresh focused regression checks: 16 canonical recovery-helper tests passed in
3.22s; five recovery endpoint tests passed in 3.32s. These cover the existing
five-target and single-league scope, not the entire weekly-update acceptance plan.
The existing deployed DuckDB automatic checkpoint threshold is 488.2 MiB;
recovery checks WAL size between leagues and starts no further repair at or
above 480 MiB. No threshold,
checksum, durability or checkpoint setting was changed. The physical issue is
separate from the demonstrated ability to regenerate scoped aggregate rows.

The scoped recovery completed for 30 leagues / 185,889 player-season keys. The next league,
`the_super_bowlava`, was stopped BEFORE any write at 480.4 MiB WAL; it and
`world_league_of_howell` remain unrepaired in this cohort (6,166 and 14,178
expected player-season keys respectively). This is a checkpoint-headroom stop,
not expensive or unreproducible aggregate math. Do not raise the threshold,
blindly retry, disable checksums or restart production to claim completion.

The 185,889 total is the sum of the 30 successful per-league reconciliations.
A redundant combined-cohort reconciliation exceeded its five-second HTTP read
budget and was not retried; it is not passing evidence. The scoped witnesses
above are the numerical acceptance evidence.

Read-only diagnostics `35308042544` (16s inspection) and `35308232185` (21s)
observed a separate OPS metadata write timing out, then application startup
checkpoint failure on the same damaged block. No manual restart, new volume,
snapshot, database replacement or write was requested by either diagnostic.
Pool initialization completed at 04:45:17 UTC. Subsequent public readiness is
200 / serving / zero active queries or writes; NYU and Catalina repaired rows
remain readable (5,904 / 3,601). WAL remains 480.4 MiB. The temporary failure is
not proof that the combined read caused the interruption or that storage healed.

A concrete lifecycle defect was reproduced locally: scoped recovery left OPS
attached read-only after both COMMIT and ROLLBACK. Both regression cases failed
on the leaked catalog. The narrow fix uses the existing reference-counted OPS
acquire/release pair, releasing before returning the pooled connection. Both
cases then passed, and eight focused server cases passed in 7.90s. Independent
review found no concurrency/lifecycle blocker; overlapping-user behavior was
reviewed in the existing helper but is not a new concurrent execution test.
This code fix is not yet deployed; it is not a claimed cause of the live restart.

The existing 40-second isolated pilot gains a byte-only `inspect` action. It reads
274,432 bytes from the known block offset on an existing recovery volume and
never opens DuckDB or replays WAL. A red/green test proves a different valid
candidate is reportable without opening a database. All 23 pilot/block/workflow
tests pass in 4.21s. This can compare existing candidates without the earlier
connection timeout; it is not a block-transplant authorization or repair proof.

Other scope limits: the cohort inventory is frozen September 16 and does not
prove every subsequent attempted league was covered. `the_league` was excluded
as the user requested. `agusta_fantasy_league` currently has only 2026 player
source data; `go_pats_2021` has no matchup facts in the current store. Missing
facts cannot be restored by recomputing aggregate rows. `league_of_snakes` and
`pass_interferance` already had matching player-season key coverage and were
not rewritten here. Reconnect requirements, all-time NFL rank correctness,
provider freshness and the full UI refresh path remain separate open evidence.

## User scope correction and 40-second pilots - 2026-09-18

Actual isolated removal pilot: run `35305006093`, public main
`8625d2cc51d639b2eb1742996b90c45bc4cdefe0`, existing volume
`vol_vgnpo57xd112npj4`. The pilot step ran 03:55:28-03:55:36 UTC (8s,
including machine startup). DuckDB 1.5.4 loaded; the read-only block probe
confirmed the exact known bad checksum after 1.378s of helper execution,
reading only 274,432 bytes. At 1.380s the retained-WAL guard stopped the
operation before opening a database connection. No DROP, COMMIT, CHECKPOINT,
reopen or reaggregation was attempted. This is a blocked removal test, not a
successful removal or storage repair. Do not retry it unchanged or remove its
WAL guard to obtain a green result.

Cleanup destroyed only owned disposable machine `8576200a245108`, in 8s;
the volume and retained WAL were preserved. Queue/setup/artifact time is not
included in the 8s pilot figure (the complete Actions job was 21s). A subsequent
live readiness check still reports serving, no active queries/writes, and the
same league fingerprint `sha256:935b1b973ff578d5`. It does not establish durable
publication. Find = confirmed; remove = blocked before SQL; fix = not attempted.
No production canary is authorized by this diagnostic result. A supported
method to remove the damaged physical metadata remains the prerequisite;
renaming/reaggregating its logical table is not that proof.

The user rejected per-league database migration. That proposal is withdrawn.
Scope is only the blocking derived-table objects; no all-league storage split.
The new hard limit is 40 seconds per pilot, including its Fly machine startup,
with no retries or automatic escalation into a rebuild. Actions queue/setup and
bounded disposable-machine cleanup are reported separately from pilot execution.

Fresh read-only evidence: catalog lists all five old `__corrupt_recovery_*`
objects. Storage metadata for `player_fantasy_season`,
`homepage_manager_rankings`, `matchup_h2h_career`, and `standings_by_year`
fails on the exact known block at 90714112. The fifth metadata request timed out
at five seconds, so it is not new confirming evidence. A scoped `nyu_ffl` 2025
source query still reads 8,010 player-week rows and sums their scores in 1.38s.
Readable source data exists; this does not establish complete historical recovery.

A disposable 536,576-byte local fixture pilot stopped in 1.531s: corrupting its
metadata also prevented opening the catalog. It did not reach DROP and is NOT a
successful or production-representative removal test (local DuckDB 1.5.1).

The existing `fly_duckdb_reaggregate_recovery.yml` all-league, 180-minute route is
replaced by a one-table isolated pilot. It uses only an existing unattached
`wkupd_rebuild_*` volume and the primary's current image; it creates no volume,
snapshot, database copy, replacement, or deployment. The helper requires DuckDB
1.5.4, the exact observed bad block, a known table and independent fact witness.
Removal additionally requires both quarantine and canonical table names, and
refuses any retained WAL rather than replaying it during the pilot. Only one
old allowlisted table can be dropped; success requires commit, checkpoint,
reopen, object absence and unchanged witness. Rebuild is deliberately unavailable
until that storage-removal gate passes. The production machine and volume are
rejected by both workflow and helper. An outer OS timeout, remote OS timeout,
and absolute helper deadline bound the operation; only its disposable VM is
cleaned up afterward. Stage output is immediate.

Review found two deadline gaps: SSH connection delay could extend the remote OS
timeout, and blocked logging could stall the Python watchdog. Both are fixed:
the remote shell recalculates remaining time after SSH connects, and the watchdog
exits without I/O. The blocked-logger regression failed before the fix. All 22
focused tests pass in 4.08s, including real deadline termination, late-SSH refusal
and the workflow's primary-volume rejection in Bash. Ruff/diff checks pass;
both local workflow copies and helper copies match. Actual isolated pilot
evidence and a supported removal mechanism remain pending. This is an enforcement
and diagnostic change, not a claim that physical corruption is repaired.

## Connection safeguards and storage decision - 2026-09-18

The preceding goal-statement-only turn made no implementation progress. This
continuation rechecked public main, the working diff, the completed diagnostic
runs and live readiness before proceeding. Fly still serves reads with zero
active queries/writes at the check; its league file fingerprint is unchanged.
This is not evidence that publication is safe.

- Diagnostic `35302921750` found exactly the same damaged block 346 on the
  existing `vol_vgnpo57xd112npj4`; its temporary probe machine was destroyed.
- Diagnostic `35302920583` could not inspect `vol_4919j2m0wzg0xw5r`: Fly reported
  insufficient host resources to create a machine on that existing volume.
  This candidate remains unverified, not classified as damaged or safe. No
  unchanged retry, new volume, snapshot restore or database copy was initiated.
- `connect_database` previously retried every DuckDB open failure twice, first
  removing the checkpoint setting and then removing all connect-time settings.
  It now makes one fully configured open and propagates the original error.
  Reader/writer handles share one thread configuration, retaining the greater
  configured capacity, instead of relying on the fallback for incompatible
  per-handle thread settings. No production connection behavior has changed yet.
- Fresh verification: 17 selected connection/startup/checkpoint/admission tests
  passed in 19.70 seconds; Ruff and `git diff --check` passed. Prior red/green
  runs reproduced repeated opens and the thread mismatch. Independent review
  approved the patch. The earlier wider 58-test run remains recorded as 57
  passed / one admission-test timeout, despite that case passing in isolation
  and again in the current selection. Local DuckDB is 1.5.1; production pins
  1.5.4. These tests are not a production-runtime or complete-goal verification.

Deployment remains held: starting the unrepaired database with the previously
committed fail-closed WAL safeguard could interrupt service. Physical recovery,
publication, all-platform canaries and affected-league recovery are still open.
Logical `db_name` filters cannot meet physical league isolation while writes
share one database file. Any proposal to change that boundary must retain the
existing pipeline and get explicit approval before implementation. It must not
split a league's data and publication journal across separate writable databases:
DuckDB only provides transactional atomicity within one database file.

## Bounded physical-block evidence - 2026-09-18 03:14 UTC

Previous turn classification: progress (published regression-tested safeguards
and reconciled the actual failed import). This turn tested a different bounded
hypothesis: an existing recovery volume might contain the exact intact block.

Public main `0e64967e1` adds a read-only diagnostic to the existing diagnostics
workflow, not a recovery writer. Six tests against a small real DuckDB file pass,
including intentional corruption and unchanged-file checks. The two local
workflow copies match; the app-repository mirror is currently uncommitted.
The diagnostic reads 274,432 bytes per file (headers plus one 256 KiB block),
never opens a DuckDB connection, and outputs only hashes/checksums. Its mode
rejects snapshot creation/restoration, restart and cleanup inputs.

- Run `35302239058` completed in 42 seconds. Existing
  `vol_rnzedj36djy0nzpr`, sourced from `vs_vaV5mX0ZV1AT2yyvy0gz`, has exactly
  the same corrupt block 346 as primary: SHA256
  `7bbcf166a70b06eb12c19888577060bf17e867a6f8b81ac7b802bffb3cab1186`.
- Run `35302351182` completed in 44 seconds. Existing
  `vol_vp26dp2g9x3167j4`, sourced from the September 15 snapshot
  `vs_PAq3Q9kRqKnfnAJNXyOb`, has a valid block at that offset, but its checksum
  is `4489825339806310033`, NOT the primary's expected
  `18392342689821271652`. It is therefore not an established donor for the
  live block; copying it would be unjustified. Its SHA256 is
  `a88410212183c07947a2544a118730218dfad981eb317719fbd249eceac96aff`.

Both probe machines were destroyed after the read. No database bytes, WAL,
production configuration or league rows were changed. No volume was created,
copied, restored or deleted. The snapshot metadata was read to identify the
already-existing volumes, not used to initiate a restore.

The user's isolation requirement is unmet: `db_name` scopes logical rows, but
all leagues still share the same physical file/checkpoint. An unrelated ESPN
upload failing on that block is direct evidence of the shared failure boundary.
Neither repeating the import, replacing a checksum, nor substituting a different
historical block is an acceptable remedy. A safe supported way to remove/repair
the corrupt physical reference remains unproven; storage recovery and all live
publication acceptance checks remain open. Do not expand into a storage migration
or a full database copy without a new user decision.

## Current checkpoint - 2026-09-18 03:00 UTC

**The storage defect is OPEN. This checkpoint supersedes earlier claims that
reaggregating the five tables established durable recovery.**

ESPN full import `35287522419`, attempt 4, for
`i_95_gridlock_league_2k27` failed at publication on public-worker revision
`83cd41dbd1cc0d7c90fcd1ff045206577d5f4dfb`. Provider fetching, expected
records, playoff odds and aggregate processing completed. The delta contained
38 tables / 6.6 MB and took 5.1 seconds to build. Fly rejected publication with
a fatal checkpoint checksum mismatch at physical block 90714112 (computed
5168518579405463287, stored 18392342689821271652). This is the same known
physical block, not a new ESPN fetch defect or a full-database upload.

Read-only reconciliation found the exact bundle
`i_95_gridlock_league_2k27-35287522419-4-280a80409000-b77fa581`
still `VALIDATED`, with `committed_at=null`. League-filtered checks of matchup,
league_settings and player_fantasy_season each returned no rows (about 1 second
per query). Cache finalization was skipped. No unchanged rerun was dispatched.
Fly `/ready` subsequently returned serving, but that is not durability evidence.

A related startup defect is reproduced locally: an exception opening/replaying
the WAL caused automatic WAL quarantine and startup against the older database;
a checkpoint exception was separately swallowed. Existing diagnostic run
`35295551400` lists multiple preserved league WAL quarantines, including 111 MB,
563 MB and 65 MB files. These prove quarantine has occurred, not which specific
publications each file contains. The latest restart has not yet been reconciled.
The pending patch removes automatic quarantine, preserves the WAL in place,
propagates startup checkpoint errors, and prevents pool initialization afterward.
Three regressions failed before the fix; all 9 targeted startup/checkpoint tests
pass in 4.65 seconds afterward. This prevents silent rollback; it does not repair
the block. Deployment is held because fail-closed startup could leave the current
unrepaired production database unavailable. No WAL was moved or deleted here.

Other pending, reviewed weekly changes scope identity reapplication and the six
season rollups to the selected year(s), retain complete history for career and
homepage outputs, reject missing historical rollup keys, and atomically advance
publication generations after the existing five-table scoped recovery. The 56
HTTP/transformation regressions pass (116.33 seconds locally). Related tests
previously passed 35/35. Ruff and diff checks pass. These are not deployed and
their production latency is unverified. The retained-key guard checks coverage,
not numeric correctness; its same-connection production cost is still unmeasured.

Fresh bounded live checks contradict the older recovery baseline:
`tfl_of_extraordinary_gentleman` is missing player-season keys for 2012-2025;
`nyu_ffl` is missing them for 2018-2025. NYU's 2025 season-player table is empty
although its weekly source still includes, for example, Christian McCaffrey's
386.1 points. Yahoo `monsters_of_the_midway` passed retained-key coverage.
Do not deploy the strict guard as if these missing baselines were recovered.

No production writes, database/volume copies, full downloads, snapshots, restarts,
or deployments were performed during this checkpoint. The physical corruption,
retained-history recovery, complete cohort verification, live changed-data
canaries and under-90-second end-to-end target remain open. A proven bounded
storage remedy is still required; retrying the import or rebuilding aggregates
alone does not supply one.

Follow-up: code and tests are now on public `main` at `7e9a1b487`; the Fly
deployment is deliberately NOT dispatched. Read-only diagnostic `35301672028`
completed in 29 seconds, with all mutation options false. It found a current
415 MB league WAL and the same historical quarantine files; no new quarantine
file from the 02:49 failure appears in that inventory. Thus automatic quarantine
is a proven code defect and a prior operational event, but is NOT established as
the cause of the latest restart or this league's missing rows. No bundle artifact
is available for run `35287522419`. Next: identify a supported, bounded remedy
for the exact corrupt block and reconcile retained publication/WAL evidence
before any deploy or retry; do not repeat an unchanged aggregate rebuild.

## Current checkpoint - 2026-09-17 09:02 UTC

The stale/unknown-freshness control defect is fixed and live. App main
`48ef0a5db66a16758841c8bd1c20455459cb80d7` makes the existing Update League
component run the guarded, leased production probe before rendering an update
action. It renders no action when the resulting manifest matches publication
and still exposes the action when a real delta exists. The focused real-browser
suite passed 4/4, including both outcomes. Production deployment
`dpl_6HGtAzA3QLUmR4szC73se1fXwMYi` reached Ready and owns the
`leaguehistory.app` aliases. A production browser check now reports no Update
League control for current `agusta_fantasy_league`.

Direct workflow executions now capture the same production source manifest as
UI dispatches instead of publishing without a freshness record. Public worker
main `dd4aec32e58b743b6a339277b39849b643796ca7` adds one stdlib helper and one
pre-claim step shared by Yahoo OAuth, ESPN, and Sleeper; app mirror
`2ac5fcdf6464aec7e3ead2200ae8710995db624d` contains byte-identical copies.
The focused workflow/helper suite passed 69 tests and all three YAML documents
parsed.

Actual production UI canary `35202162447` used the exact public SHA above. It
probed, dispatched, refreshed Sleeper week 1, committed generation 6, warmed
the required cache endpoints, promoted the exact manifest, and reached
`succeeded:hidden`. The receipt reports complete provider validation,
historical/configuration preservation, and all required homepage/season/career
outputs. Worker processing was 43.207s, but click-to-visible was 98.772s, so the
strict under-90-second requirement FAILED for this run.

The overage was isolated to setup: a redundant 4-second `requests` install plus
a 23-second full dependency install. Public main
`71503619fdbaf362fa3c166b68c5daf7b4a6e2ad` now installs once, before claim
handling, with no ephemeral pip cache or bytecode compilation; app mirror is
`b56f18396ec0c8eba52ee72a07129fefe3b4ca54`. Regression tests remain 69/69.
Production no-change/manual canary `35202656669` finished in 43s, captured the
same digest as the UI, made no publication, and reduced dependency setup to
13s--a measured 14-second setup saving. A full changed-data click is therefore
expected under 90 seconds, but that exact post-optimization case is not yet
production-verified and remains open.

Also live on public main: active-week rank materialization repair `961b5c904`,
active-only Sleeper lineage proof `a30cb92bc`, and incremental authoritative
player-position correction `5437dce24`. Augusta's prior recovery run
`35199008631` remains the validated five-aggregate/historical-preservation
witness. The player-position code is shipped but the canonical NFL weekly
source has not yet naturally republished that correction; no large NFL rebuild
was launched.

Still open before completion: post-optimization changed-data UI canaries for
Yahoo OAuth, ESPN, Sleeper, and multiplatform; the credential-blocked leagues
`league_of_snakes`, `the_chulent_bowl`, and `pass_interferance`; and publication
of the pending canonical NFL position correction. No claim of full completion
is made.

## Current checkpoint - 2026-09-16 19:27 UTC

Yahoo Update League is OAuth-only. The production caller maps Yahoo to
`yahoo_incremental_refresh_worker.yml`; that workflow supplies the Yahoo OAuth
client credentials and encrypted credential key, and
`refresh_yahoo_active_season.py` obtains the provider session through
`LeagueContext.get_oauth_session()`. No Yahoo cookie credential, cookie jar, or
cookie fallback was used by the production canary below. The separate legacy
cookie-import workflow is not an Update League caller and is outside this
worker's execution path.

Actual UI-dispatched KMFFL OAuth retry `35140405563` ran on public main
`6f94712e87c5c04948708b13311e4c7d0daebd6b`, refreshed an expired stored OAuth
access token, resolved the saved 2015-2026 Yahoo renewal chain, committed one
generation (`10 -> 11`), finalized the cache, and completed successfully. The
browser remained on the production page without a manual reload. Worker phases
totaled 45.189s; the GitHub job ran for 79s; click-to-visible was approximately
101s. Correctness passed for this canary, but the strict under-90-second
acceptance requirement FAILED.

All compact pre-2026/configuration witnesses were unchanged after publication.
The 2026 week-1 partition contains 10 team rows, 10 nonblank franchise IDs, and
1298.14 total points. Ten manager careers reconcile exactly to the complete
season history (1,490 games; zero games/wins/losses/seasons mismatches), and the
homepage now reports `data_year=2026`, `data_week=1` rather than the stale week
17 value.

The initial player-career audit used the wrong grain and its mismatch count is
withdrawn. The exact production aggregate groups all regular-season
`player_fantasy` rows by `NFL_player_id`, including the expanded unrostered
population. Re-running that exact contract against Fly returned 2,177 source
players, 2,177 target careers, zero rows missing in either direction, and zero
mismatches across first/last year, years active, fantasy points, player/manager
LAMAR, started-player clutch, starts, rostered games, wins, and losses.

The preceding KMFFL attempt `35139531195` failed before publication because the
active snapshot treated legacy server-generated `keeper_config.created_at` as
an unregistered canonical field. Public main `6f94712e8` drops only that legacy
metadata from the local non-published transform frame and makes existing
`___ops` attachment detection idempotent. Generation remained 10 on the failed
attempt. The focused weekly suite passed 361 tests, workflow/writer boundary
suite passed 110 tests, and Ruff passed before the successful retry.

### Sleeper current-state UI no-op - 2026-09-16 19:43 UTC

The production NYU page displayed `Update League` even though its observed and
published source manifests were identical and its last dispatch was healthy.
The status API reported `probe_stale=true` solely because the successful probe
was older than five minutes. One actual UI click ran the existing leased
Sleeper freshness probe and returned `already_current=true`; no worker was
dispatched, the prior run ID `35130659510`, claim version 5, publication bundle,
and source fingerprint remained unchanged, and the control hid after 13.061s.

Whole-row before/after fingerprints were identical for all pre-2026 matchup
(1,596 rows), player_fantasy (61,442), draft (1,452), transactions (9,281),
league_settings (8), league_context (1), and empty manager_overrides. This is a
verified no-op, not a Sleeper publication canary. It confirms a frontend
freshness-state defect: expiring a matching manifest exposes an update control
that only rediscovers the league is current. The proposed bounded fix is to run
the already leased/rate-limited probe off-control and expose the action only
when that probe finds a changed manifest; implementation remains gated on the
requested design approval.

### Sleeper publication canary and full-chain season dependency defect - 2026-09-16 19:52 UTC

The production UI dispatched exactly one Sleeper run for `the_real_ff_league`:
`35142909254`, public-main SHA `5909d3abc6e0b3e5db54b628f03b3eca4e74b83e`.
The persisted renewal chain resolved 2023-2026 and the provider returned ten
final week-1 team rows, 175 roster rows, 198 draft picks, and 52 transaction
perspectives. Publication committed generation 8 to bundle
`fleet-4f868dddc88a163eb681355f021bd205892febe98bf7fa921800da63f0d1bcb5`.
Worker phases totaled 43.688s, the GitHub job took 94s, and click-to-visible was
116.118s. Correctness and the strict under-90-second requirement therefore
remain unaccepted.

Independent Fly witnesses show the source merge itself was scoped correctly.
All pre-2026 whole-row hashes remained unchanged: matchup 510 /
18436341627321814689; player_fantasy 23,666 / 14420224626798944037;
draft 520 / 17283269705718972665; transactions 2,193 /
4263570373966355587; league_settings 3 / 17032997245871009279. League context
also remained unchanged at 13167564389908315858. The new 2026 matchup scope has
10 rows, 10 franchises, zero blank franchise IDs, week 1, and 1,434.64 points.
Caleb Williams is no longer mislabeled best-ever: his week-1 QB all-time rank is
216, while Josh Allen is 17.

The full validator then exposed a real dependency-order defect hidden by the
green workflow. The atomic v3 merge rebuilt career and homepage tables from the
complete weekly chain but retained historical season aggregate rows made under
older semantics. Consequently 398 player careers disagreed with season totals;
the mismatches were confined to win/loss/playoff outcome fields in 2023-2025,
while fantasy points, player/manager LAMAR, clutch, starts, rostered games, and
year coverage reconciled. The same stale-boundary issue left eight matchup
career/homepage win checks and 23 transaction report-card checks inconsistent.
The direct homepage calculation itself was correct for this H2H+median league;
the stale `matchup_season` dependency made the career comparison wrong.

A local TDD fix now reuses the existing matchup, fantasy, draft, and transaction
season aggregators on Fly's already-open complete-chain connection, inside the
same v3 publication transaction, before career and homepage aggregation. It
does not refetch provider history or rewrite matchup/player/draft/transaction
source facts. The publication receipt now records season tables and timing for
Yahoo OAuth, Sleeper, and ESPN. Regression coverage proves stale historical
season rows and transaction report cards are corrected, careers then reconcile,
source hashes are unchanged, and failures remain transactionally rollback-safe.
Expanded server/worker/receipt/ownership/aggregate suites: 509 passed; Ruff and
`git diff --check` passed. This fix is local and uncommitted at this checkpoint; deployment and a
second production canary are still required. Draft warnings, one missing PPG,
and three historical sacko-placement failures remain independently unclosed.

## Disk-blocked continuation checkpoint

D: again reports zero free bytes after three consecutive goal turns with this
same capacity blocker. The current worker HEAD remains a36d876fda916bd0817b7c5479ca117625e3ec0d;
the tested lineage guard is local/uncommitted. No canary or lineage-test process
remains running. Earlier exact temporary-test cleanup was denied; no alternate
deletion mechanism was used. Small C: fixtures allowed the Python regressions,
but the frontend runner still requires its D:/temp module directory and fails
with ENOSPC. Commit/build/deployment cannot proceed safely with zero headroom.

Resume when the user frees D: space: run the pending frontend regression RED,
finish the same ownership check in the existing freshness reader, verify the
worker/frontend suites, then commit only this scoped fix and deploy through
the existing main paths. Recover the_league through the original ESPN/Sleeper
import identity, not its incorrect Yahoo leg. NYU latency/rank issues, remaining
platform canaries, cohort recovery and every other open acceptance item remain
in scope; this is not completion or a narrower replacement objective.

## Latest checkpoint - 2026-09-16 17:50 UTC

The six-file traded-pick/homepage fix is now committed and pushed to public main
as a36d876fda916bd0817b7c5479ca117625e3ec0d. Deployment 35129686647 succeeded
on that exact SHA at 17:42:47 UTC. Fly /ready is serving and accepting queries.
The final transaction/homepage/career/HTTP-publication suite passed 81 tests
in 48.49s using small C: test fixtures. Dirty ownership/planner experiments were
excluded; no workflow or replacement pipeline was introduced.

Second actual NYU UI canary: clicked Retry Update once at 17:50:12.851 UTC;
POST acknowledgment took 17.873s, dispatching run 35130659510. Publication,
rendered results and under-90-second latency remain unverified while it runs.
Read-only observer processes were closed before this single click. D: has no
free space; the browser uses a small C: profile, not another checkout or lake.

NYU UI run 35130659510 subsequently COMMITTED bundle
fleet-83ca13699546f4303aa27e20f09f8724628f6afc2a7a5b367b7056351f180c3b.
Browser observed committed at 150.062s, succeeded/hidden at 153.003s, and the
updated rendered page at 154.911s. The false +13 current-season trade card is
gone; the 2025 +815 all-time trade card, all nine-year manager careers and the
weekly/season/career clutch cards remain visible. No manual page reload.
Worker phases total 103.344s: NFL-week cache patch 54.255s (fetch 51.944s),
shared transformations 8.001s, Fly publication 29.422s. Server career rebuild
3.26s and homepage rebuild 9.0593s are within that publication phase.
The under-90-second requirement FAILED; success is not fleet completion.
Independent Fly career reconciliation: all 12 franchises agree with all-season
games/wins/losses totals (1,400 career games), zero discrepancies. Matchup
identities are nonempty and the same 12 display aliases appear in 2018-2026.
Further independent score/rank checks and remaining platform canaries are open.

Independent post-publication NYU checks: all 12 week-1 team scores match live
Sleeper exactly. All eight mirrored pick perspectives now name the four correct
conveyed players, each with zero trade-asset LAMAR. The old Rams DST mapping is
absent. Fly's all-time trade winner remains KrispyChris at 814.65.
All 430 active player rows match live OPS season/career PPG. Caleb Williams is
QB rank 216 with standard-scoring career PPG 17.49, not rank 1.
One position-rank discrepancy remains: unrostered Bo Melton is displayed WR
but carries DB rank 3699; current Fly WR rank is null, DB rank is 3699. Nine
rostered, nonstarted zero-point players lack a week-1 OPS row. Do not silently
count these as fully verified. Cache-bio synchronization currently refreshes
only missing identities, so stale cached position metadata is a concrete
candidate; source-position/eligibility consistency still requires resolution.

### Yahoo canary paused before dispatch: conflicting active-chain ownership

At 17:59 UTC the actual the_league page offered Update League. No click was
made; the read-only browser was closed. Fly context says platform=sleeper and
league_id=1385696375349448704, while its league_ids_json contains a Yahoo chain
ending 470.l.164172. Persisted settings contain ESPN 110800 for 2011-2024,
Sleeper 1223059438311575552 for 2025, and Yahoo 470.l.164172 for 2026.
The live Sleeper API independently confirms 1385696375349448704 is the 2026
successor of 1223059438311575552. Yahoo 2026 has a different manager identity
set. This is an unresolved ownership/provenance conflict, not proof of an
intentional platform transition. The prior manual Yahoo success is therefore
not a chain-correctness receipt. Do not refresh that Yahoo target again until
the intended active leg is established from import provenance or owner input.
Current frontend and worker selection prefer persisted current-year settings
over the conflicting context, so merely repeating the same canary cannot
resolve the ambiguity. Reuse original import-chain ownership; do not create
another name-based resolver or directly rewrite Fly rows.

### Ownership provenance and local regression fix - 2026-09-16 18:08 UTC

The existing accounts.pending_paid_imports record dated 2026-09-09 03:55:08
resolves the_league's intended import: platform=multi-platform, target index=1,
segments ESPN 110800 then Sleeper 1385696375349448704. No Yahoo segment is in
that saved plan. The league_context platform/ID agree with the Sleeper target;
its updated_at is 2026-09-11 05:42:37. Thus the 2026 Yahoo leg is not supported
by the original import plan. Recovery must retain the ESPN/Sleeper history and
use the established Sleeper continuation, not another Yahoo canary.

Added three real worker regression cases for context/provider contradictions.
All three failed before the fix (wrong provider was accepted). The existing
resolve_active_update_segment now rejects active ownership that contradicts
the saved import target. The lineage + three platform-worker suites pass
40 tests in 1.62s. This two-file worker change is LOCAL ONLY, not committed or
deployed. Unrelated ownership/planner experiments remain excluded.

Frontend regression added in the existing freshness-chain test, but execution
is environment-blocked: Vitest cannot mkdir D:/temp/<run>/ssr (ENOSPC). Its
configuration loader issue was isolated using a small C: test config, without
installing packages or moving the repository. TEMP/TMP remain D:/temp. No
frontend implementation was changed before a valid RED test can run.
D: remains at zero free bytes. Need user to free disk before frontend tests,
commit/deployment and the verified existing-path recovery can continue safely.

## Current sequential checkpoint — 2026-09-16 16:58 UTC

One implementation lane; no new checkout, branch, worktree, or parallel agent.
App UI revision 84cb66f5af4a7be6917104affca26228da355bf3 was deployed in the
preceding continuation; this is not evidence of worker completion. Public worker
checkout remains dc3f3e0221a1bcb730413d90c52b5b698e932c35. Existing modified
ownership experiments are excluded from this work and must not be staged.

Recovery inventory: `update-league-cohort-all-attempts-2026-09-16.json` is the
supporting all-attempts evidence for this ledger (not a second tracker).
All pages of the three incremental-refresh workflow histories returned 219 runs:
113 Sleeper, 51 Yahoo, 55 ESPN, starting 2026-09-10. Run logs resolve 212 runs to
36 unique league database identities. Every league remains unverified for the
full recovery contract, regardless of previous green runs.

Three unmatched runs (35031859653, 35031862539, 35031858907) are push-triggered
workflow-definition failures with no jobs, not league update dispatches.
Four actual dispatches remain identity-unresolved: 34863624106, 34895614878,
34876171739, 34861484048. Three failed before any steps; Yahoo 34895614878 failed
restoring the ops cache before the worker. Do not silently drop these attempts.
The older offseason-update workflow and persisted dispatch registry still need
scope reconciliation before declaring the recovery inventory exhaustive.

Next active task: finish identity/scope reconciliation and compact before-update
witnesses, then trace a real Sleeper UI update. Browser connector returns no
available browser; existing Playwright Chromium is being checked as the browser
test fallback. No new production update has been dispatched in this continuation.

### Live Sleeper UI canary failed — 17:08 UTC

Compact before-witnesses for 36 identified leagues are saved in
`update-league-cohort-before-witnesses-2026-09-16.json`: selected value hashes by
season, configuration hashes, and stable before/after publication generations.
The first whole-row player_fantasy hash timed out without writes; the successful
witness hashes explicit identity/score/alias/LAMAR/clutch columns instead.
No league-history rows or NFL lake were downloaded.

Playwright Chromium loaded the actual production NYU page, with no mocked HTTP
routes. One click at 17:06:51.439Z dispatched public run 35126173284 on exact
worker main dc3f3e0221a1bcb730413d90c52b5b698e932c35. Acknowledgment took 18.802s.
The workflow's paid UI claim succeeded; manual-claim steps were skipped.
NYU before publication generation is 3. The UI reported failure at 99.406s;
the refresh step ran about 59s. Fly rejected publication with HTTP 422:
`Homepage summary lost populated value: season_trade_winner`. Generation remains
3 and the previous +12.93 season trade highlight remains. No successful or
under-90-second publication occurred. This additional attempt belongs to the
same NYU recovery entry; NYU is not recovered.

Source verification found a real shared traded-pick mapping defect: Sleeper's
`pick_{season}_{round}_{roster_id}` was matched to draft slot for startup drafts.
NYU's original roster 4 owns draft slot 6, not slot 4. The old mapping borrowed
an unrelated Rams DST's -12.925 LAMAR and manufactured the +12.93 trade winner.
The four actual conveyed bench assets currently have zero manager LAMAR.

Local fix uses the existing original-roster mapping for every draft category.
The full transaction-enrichment test file passed 26 tests. Homepage correction
uses the existing DDL trade fields and shared calculation: valid no-winner
results are explicit nulls; SQL failures propagate; the existing mirrored
trade-asset contract is checked before calculating the highlight. Tests first
reproduced six failures, then the 72 transaction/homepage regressions passed
within the wider run (95 passing tests total before the failures below).
No replacement pipeline, new checkout, or production write has been introduced.

Read-only cohort inspection found invalid mirrored trade assets in 10 leagues,
across 15 league/year partitions. Examples include 2026 sent-only trade rows in
franchise_mode_fantasy and the_dfb_league_ii, and missing historical sent values
in bethany_beach_league. These are open source/enrichment recovery defects, not
credential blockers. NYU's complete 2018–2026 trade assets pass this check.

Broader verification found an unchanged baseline test disagreement:
`test_draft_manager_season_pk_excludes_draft_category` expects category absent,
but the existing DDL explicitly includes category in the key. Neither file was
changed in this fix. HTTP merge tests could not complete because D: filled with
temporary test databases (2 failures/7 errors total in the 104-test run, with
the remaining failures/errors attributable to disk exhaustion). Cleanup of
only this run's disposable pytest-2419 directory was denied by tool policy;
no alternative deletion mechanism was attempted. User was asked to free space.
The same 8 HTTP publication tests were subsequently rerun with small disposable
fixtures on C: (`--basetemp=C:/Users/joeye/AppData/Local/Temp/league-update-trade-http-20260916-1733`):
8 passed in 16.66s, including atomic rollback and replay. TEMP/TMP remained D:/temp.
No repository, NFL lake, or league-history dataset was moved to C:.
Remaining D: space is approximately 5.5 MiB, insufficient headroom to safely
stage/commit/deploy. The cleanup request remains outstanding.

Review also found pre-existing ambiguous traded-pick fallback branches when
original-roster metadata is absent; these still require a source-backed fix and
regressions. The current mapping fix proves only the metadata-present branch.
Application and public-worker Python files have diverged; do not overwrite the
app's files wholesale. The executing incremental workflow and Fly Dockerfile
both read the canonical public-worker checkout. No workflow was changed here.
The local changes have NOT been committed, deployed, or production-verified.

### Follow-up read-only isolation while deployment is disk-blocked

The next continuation rechecked D: at 4 MiB free; pytest-2419 still exists.
No cleanup retry, new checkout, production rerun, or deployment was attempted.

For the_real_ff_league, Fly contains only 5 sent perspectives for 2026
transaction 1401414684367687680 in league 1389755141288124416. The live Sleeper
transactions/1 response still contains all five assets (one player/four picks).
The current real parser and canonical normalizer produce 5 received + 5 sent
rows, and a second normalization preserves all 10. This is not evidence of a
current provider or normalizer omission.

A subsequent in-memory DuckDB replay loaded only those five live Fly rows into
the canonical DDL, then called the actual merge_provider_refresh_table with the
provider-parsed assets. It explicitly loaded committed HEAD ownership code to
exclude the dirty local ownership experiments. First merge and identical repeat
both yielded 5 received + 5 sent, with zero duplicate transaction/sequence keys.
No real league data was written. Thus the committed merge can restore these
missing perspectives; do not add a speculative replacement normalizer. Full
enrichment/publication of this league remains unverified and must follow the
sequential NYU canary after the publication fix is deployed.

The four identity-unresolved old dispatch IDs have no matches in the current
Fly dispatch registry. They remain unresolved, not credential-blocked/recovered.
September predecessor audit enumerated 93 offseason-draft workflow runs; their
logs invoke check_offseason_drafts.py, not any active-season refresh script.
They are not silently added as weekly-update recoveries: indirect-call/scope
reconciliation remains open, and names require the predecessor's DB_NAMES field.

## Objective and acceptance

Use the existing September shared quick-import transformations for Yahoo OAuth,
ESPN, Sleeper, and multiplatform updates. Fetch changes since the last committed
publication, merge them into the established renewal chain, rebuild derived
outputs with complete historical context, and publish atomically through Fly.
No replacement pipeline, MotherDuck, local full-history hydration, or NFL-lake
download. Actual UI-dispatched updates must visibly complete in under 90 seconds.

All original requirements remain open unless independently evidenced below:

- Full history, franchise identities, shared-team aliases, settings and user configuration.
- Partial-week updates, final scores, transactions, roster/settings changes, missed weeks and older corrections.
- Full-context career/NFL rankings, clutch, simulations, player aggregates and homepage outputs.
- Fail-closed provider completeness; atomic publication, generation checks, idempotence, cancellation and ambiguous-commit recovery.
- Stored Yahoo OAuth; established ESPN/Sleeper credential paths; reconnect-and-resume when required.
- Paid/grandfathered gating in API and worker; existing unpaid upgrade path; agreed availability/abuse limits.
- Shared freshness contract, resilient polling, post-publication UI refresh, compact Almanac control and intact branding/Buy Me a Coffee.
- Canonical public main for every caller/workflow and matching deployed revisions.
- Recover the complete attempted-update cohort, with credential-only blockers supported by evidence.

## Confirmed defects and work

### Career tables computed from active-season-only scratch data â€” OPEN, production defect

Read-only Fly check on 2026-09-16, `___leagues`, filtered to `db_name='the_league'`:

```sql
SELECT year, count(*) AS rows, sum(games) AS games
FROM public.matchup_season
WHERE db_name='the_league'
GROUP BY year ORDER BY year;

SELECT count(*) AS rows, sum(games) AS games
FROM public.matchup_career WHERE db_name='the_league';
```

Observed: each 2011â€“2025 season has 12 rows/168 games; 2026 has 12 rows/12 games.
Season total: 2,532 games. Career: 12 rows/12 games. Historical season rows remain
present, but career totals do not represent that history.

Public worker SHA inspected: `0bfe8122c8d31c094bffb1872e1a62651368e6d7`.
`scripts/refresh_yahoo_active_season.py` hydrates active-year sources but calls
career builders. `active_refresh_publish_tables` includes those whole-league
tables. Existing career-preservation checks skip tables absent from the input
witness map. Therefore a green run and the historical-preservation boolean do
not establish correct career outputs.

Earlier Yahoo run `35057508667` is evidence of publication, not successful
full-history preservation. Do not dispatch further canaries with this defect.

Local changes on `D:\yahoo_oauth`, branch `main`:

- Existing `aggregate_matchup_h2h` accepts `season_years`; an empty set rebuilds
  careers from all persisted matchup years without rewriting season summaries.
- `aggregation_utils.aggregate_career_rollups` directly invokes the existing
  eight career builders on the complete `___leagues` connection. It does not
  open a connection or commit. It rejects a scratch catalog and missing source
  tables before deleting outputs.
- These changes are NOT yet wired into the server merge or deployed.

Evidence: `test_career_rollup_refresh.py`, `test_aggregate_matchup_context.py`,
`test_aggregate_draft_context.py`, `test_aggregate_transaction_context.py`:
17 passed in 3.45s. Tests cover two-year career totals, shared alias, clutch,
quarter-point corrections, repeat execution, another league's isolation,
rollback, missing source rejection, and unchanged historical season summaries.
Draft/transaction fixture coverage is not a production/fleet proof.

Next implementation boundary: call the SAME shared career routines on Fly's
existing merge connection after changed source and season partitions are merged,
before generation/publication-state commit. Package canonical shared modules in
the server image; do not duplicate aggregation SQL or fetch historical raw data.
Verify real merge rollback on aggregation failure, exact season-to-career totals,
unchanged historical partition fingerprints and authoritative publication receipts.
Remove active-only career outputs from the worker contract once server capability
is enforced; an older server must fail closed, not silently skip recomputation.

### Provider score preservation skipped bonus enrichment â€” LOCAL FIX, not deployed

`populate_fantasy_points` scoped its entire UPDATE to rows eligible for score
recomputation. Thus rostered modern ESPN/Sleeper rows with authoritative scores
kept null bonus/premium columns. This explains the null-bonus publication failure
in ESPN runs `35057665071` and `35058032294` more directly than the earlier bio
cache hypothesis.

Change: apply the preservation condition only to `fantasy_points`; enrich bonus
and premium components separately in the same UPDATE. Scope the staging SELECT
to the intended league. No extra points are added to the provider's total.

Regression evidence: six initial cases failed before the fix with null bonus
fields. Expanded 12-case matrix passes for ESPN, Sleeper and Fleaflicker,
zero/nonzero bonuses, TE premiums, retained authoritative score/alias, and repeats.

Existing full `test_sql_player_enrichments.py` has two additional failures:
`applies_custom_offense_corrections` and `preserves_yahoo_rostered_api_points`.
Both were independently reproduced by loading the unmodified HEAD module into
the test process. They are pre-existing, remain open, and must not be hidden by
claiming the full scoring suite passes.

## Production verification still required

No production writes, workflow reruns, or deployments were performed in this
continuation. No new UI canary or under-90-second claim is justified. Root changes
are not yet mirrored to the public worker checkout. Its two existing modified
ownership files were left untouched; the abandoned null-value restoration helper
must not be staged as a fix because it can resurrect invalid all-time ranks.

After the atomic career repair: finish completeness/freshness/watermark and
concurrency audits, reproduce Gray's exact ESPN correction, reconcile aliases
and historical/NFL ranking values, dispatch the real UI path for each platform
and multiplatform support, recover the full attempted-update cohort, and record
worker processing versus click-to-visible latency separately.

## Atomic publication integration follow-up

The shared career routine is now integrated into the existing fleet merge as
`fleet-partition-v2`. V2 omits worker-built career tables and recomputes all eight
careers after source/season partition merge, before publication-state and
generation commit. V1 remains supported for existing callers. Older servers
reject V2 rather than silently omitting its career work.

Tests exercise the actual merge and HTTP publication endpoint, not only helper
functions. Verified locally: historical plus new season totals, untouched prior
season summaries, idempotent HTTP replay, stale generation rejection, exact
uploaded-league/generation scope match, transaction rollback on missing sources
and query timeout, and FAILED_MERGE status on NFL attachment failure.

The HTTP test initially failed because ___ops was not attached on the merge
connection. The fix reuses the server's reference-counted read-only attachment,
releasing it on success or failure. A second injected attachment failure exposed
a VALIDATED status left behind; it now records FAILED_MERGE.

The server image copies canonical Python aggregation modules with an explicit
Fly --ignorefile allowlist. No database, Parquet, NFL lake or credential files
are included in the build context. A Docker build import check verifies the
shared aggregation dependencies before deployment.

Public worker changes are prepared for Yahoo, ESPN and Sleeper: require V2,
exclude scratch careers from publication, and retain the server's career
counts/timings in receipts. Enable them only after server deployment succeeds.
Local worker regression group: 122 passed. Client/merge group: 22 passed.
The two pre-existing scoring tests above still fail; do not claim a wholly
green scoring suite or production recovery.

## Deployment and first canary, 2026-09-16

Server revision `e42b94ec8bc290d67ef6dd61e2affbb9fb30f14d` deployed successfully
via public run `35095334443`. Build context was 7.13 MB. Worker invocation changes
were activated on public main at `cffa425bf98f61cb918b222e919c1d818732c9a9`.

Yahoo manual canary `35095696367` FAILED before publication. Its preflight still
required `matchup_career`, `player_fantasy_career`, and `player_fantasy_career_all`
inside the upload, despite V2 rebuilding them atomically on Fly. No recovery is
claimed. Worker step took 84 seconds; whole job took 121 seconds, so the latency
target also remains unmet.

The validator now accepts the explicit fleet schema contract. V1 still requires
uploaded careers. V2 requires Fly-built careers, prohibits uploading scratch
careers, and retains source identity, local derived-value, and homepage checks.
All three callers declare the same V2 contract used by their bundle builder.
Regression cases reproduced the missing-contract failure before implementation.
The expanded validation suite passes 46 tests; the preceding six-file worker,
career, and validator run passed 166 tests before two provider variants were added.

Actual UI verification remains unverified: the browser surface reported no
connected browsers. Backend/manual canaries do not satisfy the UI criterion.
## Yahoo recovery canary verified, 2026-09-16 12:34 UTC

Public main revision: `372ee4e106c22cbd4a8a3353edf9d1fad1205dc8`.
Manual run: https://github.com/jeleff1000/mfl-league-fetcher/actions/runs/35096470346
COMMITTED bundle: `fleet-0c2397c55cda84e10acb0715b75a5563358cde17a416df2d72017629d283ef99`.
Dispatch 12:32:56; refresh step 12:33:25–12:34:16; cache finalized 12:34:20.
Manual dispatch-to-cache: 84 seconds; worker phases: 49.435 seconds.
Server full-chain career aggregation: 4.4958 seconds. This is NOT UI-click evidence.

Independent Fly checks for `the_league`:
- Career games recovered from 12 to 2,532, exactly matching season totals.
- All 37 franchise careers match season games, wins, losses, points and latest
  manager/alias; zero discrepancies.
- All 4,316 regular-season and 4,319 all-games player careers match persisted
  weekly games, fantasy points, manager LAMAR and started-player clutch sums.
  Zero discrepancies at 1e-5 tolerance. The first audit incorrectly summed
  bench clutch; corrected audit follows the established started-player metric.
- Historical rows and row fingerprints unchanged:
  matchup 2940 / 11727621620591406241;
  player_fantasy 186017 / 13727482781365112600;
  draft 2577 / 12085463288980457930;
  transactions 12512 / 14259586135160535248;
  league_settings 15 / 11640897616840183947.
- league_context fingerprint unchanged: 13149953313368572827; manager_overrides,
  keeper_config, league_rules and standings_config remain empty.
- Receipt downloaded: D:/temp/update-evidence-35096470346/yahoo_active_season_refresh.json.

Additional confirmed open gap: `homepage_refresh._load_homepage_source_frames`
still downloads skinny all-history matchup, draft, transaction and started-player
rows to rebuild homepage outputs locally. Although bounded by league, it does
not satisfy the no-local-history-hydration objective. The existing
`homepage_summary.compute_homepage_frames` accepts a complete connection and is
the reuse target; do not duplicate its formulas or declare this requirement done.

ESPN canary now queued/running: `35096876781` for
`tfl_of_extraordinary_gentleman`, same public main revision.
## Sleeper verification and ESPN remaining failure

Sleeper manual canary `35097064614` on public `372ee4e10` COMMITTED for `nyu_ffl`.
Dispatch 12:39:06; worker 12:39:28–12:40:12; cache finalized 12:40:18 UTC.
Manual dispatch-to-cache: 72 seconds. Processing phases: 42.855 seconds;
server career rebuild: 3.0701 seconds. Not an actual UI-click canary.

Independent Fly reconciliation:
- 12 franchise careers, 1,400 career games; every franchise's games, wins,
  losses, points and latest manager/alias matches its retained season summaries.
- 1,686 regular-season and 1,689 all-games player careers match weekly games,
  points, manager LAMAR and started-player clutch; zero mismatches at 1e-5.
- All pre-2026 row counts and fingerprints unchanged:
  matchup 1596 / 13524715644287174441;
  player_fantasy 61442 / 7588907656652896125;
  draft 1452 / 17688804873191818872;
  transactions 9281 / 5272688981035236134;
  league_settings 8 / 1549569747058823479.
- league_context unchanged: 13373334383361378998. Empty user-configuration
  tables remain empty.
- Receipt: D:/temp/update-evidence-35097064614/sleeper_active_season_refresh.json.

ESPN manual canary `35096876781` FAILED before publication on the same revision.
The bonus-points failure no longer occurs. Preservation now rejects null
`position_alltime_rank` for `00-0034160_2026_1`, Michael Dickson, position P.
Persisted legacy rank is 7; shared position-rank mapping supports QB/RB/WR/TE/K/
DEF/LB/DL/DB but no P source column. Do NOT blindly restore rank 7 or relax
all rank validation. Next: prove the unsupported-position semantics with the
actual shared rank function and distinguish legitimate non-applicability from
missing supported-position source data.

Historical audit finding, superseded by `40778c937` and the shared historical
PPG correction below: `optimal_lineup.position_rank` formerly computed
`alltime_ppg` from local active-year rows. Current code uses OPS precomputed
PPG; do not treat this old note as a still-unfixed implementation defect.

No workers from these three canaries remain running. No all-platform completion
claim is justified. UI verification, all attempted-league recovery, multiplatform
canary, exact Gray correction, full source-watermark reconciliation and the
remaining audit items stay open.

## Fly corruption recovery and post-snapshot import preservation

The live `league_settings` checksum failure was isolated to a damaged physical
block. Validated source snapshot `vs_PAq3Q9kRqKnfnAJNXyOb` predates 33 leagues
that were imported or updated afterward. Preservation snapshot
`vs_Avgke1o8gO9fwXw7p9bw` is created and healthy. The guarded settings swap
preserved all 215 post-snapshot settings rows: live remains 6,255 rows, 6,255
distinct `(db_name, year)` keys, and 1,782 leagues. The generation ledger's
latest publication is 2026-09-16 20:04:35 UTC, before the independently captured
overlay at 21:03:10 UTC. No interim league loss is observed.

Five derived tables share the damaged block: `homepage_manager_rankings`,
`matchup_h2h_career`, `player_fantasy_season`, `player_fantasy_season_all`, and
`standings_by_year`. An isolated restore proved all five readable and key-unique
in the source snapshot. Recovery now exports those exact tables, overlays every
post-snapshot league from live at each atomic swap, rejects a stale overlay
cohort or row count, and rebuilds changed leagues through the existing shared
full-chain aggregators. A post-commit checkpoint failure is reported explicitly
instead of becoming an ambiguous 500; same-run retries are idempotent.

Local evidence: 62/62 DuckDB server integration tests, 3/3 recovery-workflow
contract tests, YAML parse, Ruff, compileall, and diff checks pass. Production
recovery has not yet run. The live source tables remain intact, but the five
derived tables still require the guarded recovery before update canaries resume.

The attempted in-place empty-shell repair was rejected by DuckDB at commit and
rolled back. Live source counts remained unchanged afterward: 6,255 settings,
1,769 contexts, 1,015,510 matchups, 911,103 draft rows, and 3,867,484
transactions; the latest generation timestamp remained 2026-09-16 20:04:35
UTC. A temporary zero-row repair table was removed and the catalog has no
`__repair_*` or `__corrupt_*` leftovers. This path must not be retried.

Follow-up evidence narrowed that statement: the failed path attempted to drop
the damaged objects. Direct reaggregation run `35169976934` proved the first
canonical calculation completes, then `DELETE FROM player_fantasy_season`
fails on physical block 90714112 (computed checksum 5168518579405463287,
stored checksum 18392342689821271652). The fail-fast runner stopped on
`a_circle_of_jerks` with zero completed leagues; it did not mutate production.

Isolated run `35170289421` then tested a different, non-destructive catalog
operation against retained recovery volume `vol_4919j2m0wzg0xw5r`: atomically
rename each of the five damaged objects to `__corrupt_recovery_*` and install
an empty canonical shell without reading or dropping the damaged block. All
five swaps committed successfully in 1m33s including Fly machine startup.
Production remained untouched. This proves the safe fast recovery boundary is
catalog quarantine followed by the already-built snapshot seed and changed-
league reaggregation; no full logical database copy is required.

The next recovery stage is isolated and fail-closed: restore the preservation
snapshot to a new Fly volume, copy every healthy table from its exact catalog
DDL, and recreate only the five proven-damaged derived tables empty. The helper
rejects unknown catalog-object types, absent exclusions, row-count mismatches,
or a retained WAL. It cannot stop, clone, deploy, replace, or otherwise mutate
the primary machine. Local helper/contract evidence: 7/7 tests, YAML parse,
Ruff, compileall, and diff checks pass. Isolated production-volume evidence is
still pending and no cutover is authorized yet.

## Shared historical PPG and rank-applicability corrections

Regression evidence: with a single hydrated week scoring 18.76, the old shared
position-rank function returned both season and career PPG as 18.76. Three
scoring variants failed before the fix. It now directly maps the existing
precomputed OPS PPG columns using the existing scoring-column helper; no local
career AVG or NFL-history download. Missing selected PPG columns fail before
outputs are cleared. Valid zero PPG is retained.

Read-only live OPS witness: Caleb Williams 2026 week 1 has half-PPR/4pt season
PPG 37.26 and career PPG 17.52; all-time QB rank 216. Michael Dickson has
position P, season PPG 0 and career PPG -0.02, with no QB rank. All 1,149 source
rows in that week contain the selected season and career PPG values.

ESPN's legacy non-applicable all-time rank is handled only for unchanged,
recognized nonranked NFL positions (punters, long snappers, offensive line).
Supported, unknown, blank or changed positions remain fail-closed. This
exception affects only position_alltime_rank, not points, clutch or PPG.
Tests exercise the real shared rank function and the preservation gate.
Eight applicability regressions failed before implementation.
Combined worker/validation/rank suite: 185 passed; focused preservation/rank
group: 53 passed. Ruff passes. No production result for these new changes yet.

## Five-table production reaggregation

The snapshot-derived recovery was not used. Run `35171039263` waited 17m22s
for Fly snapshot infrastructure and then failed closed before any production
write because the live overlay advanced from the prepared 33 leagues / 215
settings rows to 35 leagues / 239 rows. That proved the stale-overlay gate,
but also proved this was the wrong-granularity mechanism for five derived
tables. It was not a 17-minute SQL validation.

Production was instead repaired directly from intact persisted facts and
healthy intermediate aggregates in one atomic transaction. The transaction
rebuilt exactly `homepage_manager_rankings`, `matchup_h2h_career`,
`player_fantasy_season`, `player_fantasy_season_all`, and
`standings_by_year`; it did not fetch provider data, replace settings, rewrite
source facts, copy the database, or use MotherDuck. End-to-end database work
took 122.98 seconds, dominated by the two required scans of 50,785,407 weekly
player rows.

Production result and canonical-key validation (6.46 seconds total):

- `homepage_manager_rankings`: 21,856 rows / 21,856 distinct
  `(db_name, franchise_id)` keys.
- `matchup_h2h_career`: 248,797 rows / 248,797 distinct
  `(db_name, franchise_id, opponent_franchise_id)` keys.
- `player_fantasy_season`: 4,372,442 rows / 4,372,442 distinct
  `(db_name, NFL_player_id, year)` keys.
- `player_fantasy_season_all`: 4,380,213 rows / 4,380,213 distinct
  `(db_name, NFL_player_id, year)` keys.
- `standings_by_year`: 62,041 rows / 62,041 distinct
  `(db_name, franchise_id, year)` keys.

Source reconciliation across Yahoo `the_league`, Sleeper `nyu_ffl`, ESPN
`tfl_of_extraordinary_gentleman`, and `kmffl` found zero manager-ranking
mismatches, zero player points/games mismatches, and zero standings
wins/losses/points mismatches. The `the_league` projection counts matched the
pre-write witness exactly: rankings 37, H2H 298, regular player seasons 16,020,
all-games player seasons 16,043, and standings 192. KMFFL has 2,145 rebuilt
season-player rows with nonzero clutch equity across 2015-2026, so the repair
did not null the clutch aggregate.

The five damaged original objects remain quarantined under
`__corrupt_recovery_*`; they were not read, dropped, or treated as fallbacks.
This closes the live five-table outage only. It does not close the broader
Update League goal, UI-dispatched verification, or the remaining
complete-history audit items above.

## Sequential production canaries after five-table repair

All runs below executed public-worker `main` at exact SHA
`793ebe8547155fe0710369343234c14f7d21d281`.

- Sleeper `nyu_ffl`: run `35172946763` succeeded in 24 seconds and correctly
  returned `NO_FINALIZED_WEEKS` without publishing. Source planning took
  0.273 seconds. The persisted renewal chain remained complete for 2018-2026;
  rankings retained 108 manager-seasons, standings retained 2018-2026, and
  player season output retained 1,911 nonzero clutch rows.
- A deliberately incorrect Yahoo worker dispatch for multiplatform
  `the_league` (run `35173026092`) failed closed before provider fetch or
  publication because its active 2026 segment is Sleeper. This confirms the
  active-segment guard; it must not be bypassed by forcing a historical
  platform worker.
- Yahoo OAuth `kmffl`: run `35173145823` succeeded in 41 seconds and correctly
  returned `NO_FINALIZED_WEEKS` without publishing. Source planning took
  0.186 seconds. The persisted Yahoo renewal chain remained complete for
  2015-2026, manager aliases remained present, and the repaired player-season
  output retained 2,145 nonzero clutch rows.
- ESPN `tfl_of_extraordinary_gentleman`: run `35173226958` committed week 1
  and published the refreshed cache. Worker processing was 51.176 seconds:
  provider fetch 4.614, shared transformations 8.103, full season rollups
  7.143, homepage outputs 8.570, and atomic Fly publication 20.787 seconds.
  Cache publication and hot verification took about 1.3 seconds. The workflow
  job took 95 seconds because setup/claim/cache-restore consumed roughly 39
  seconds before the 51-second worker; run creation to verified hot cache was
  about 94 seconds, so the strict under-90-second click-to-visible criterion is
  not yet closed by this manual canary.

Post-publication validation for the ESPN canary was limited to the five
repaired derived tables. Every canonical key was unique. Rankings and H2H had
15/15 and 178/178 rows/keys; regular and all-game player season tables had
10,043/10,043 and 10,058/10,058; standings had 180/180. Every table spans
2012-2026. The regular player-season table retains 3,097 nonzero clutch rows.
The live overview API reports `last_updated=2026-09-17T02:09:17.619718` and
serves full-history leaders from 2013, 2017, 2019, and 2022 alongside 2026
week 1, rather than treating the refreshed week as the entire history.

These canaries close the five-table reaggregation defect on all three platform
paths. They do not yet close actual UI-dispatched verification, the
multiplatform dispatch canary, full recovery-cohort verification, or the final
under-90-second click-to-visible requirement.
