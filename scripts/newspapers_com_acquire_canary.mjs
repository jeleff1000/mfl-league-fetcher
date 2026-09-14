#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_QUEUE =
  "D:/league-history-data/nfl/derived/validation/game_completeness_inventory/20260616T043000Z_v22_pbp_parser_rebuild_closed/game_completeness_review_queue.csv";
const DEFAULT_OUT_ROOT =
  "D:/league-history-data/nfl/raw/newspaper_archives/source_horizon_game_candidates/game_completeness_download_pilots";

const TEAM_NAMES = {
  AKR: "Akron Indians",
  ARI: "Arizona Cardinals",
  ATL: "Atlanta Falcons",
  BAL: "Baltimore Colts",
  BB: "Buffalo Bisons",
  BDA: "Brooklyn Dodgers",
  BCL: "Baltimore Colts",
  BKN: "Brooklyn Dodgers",
  BOS: "Boston Bulldogs",
  BRL: "Brooklyn Lions",
  BRO: "Brooklyn Dodgers",
  BUF: "Buffalo Bills",
  BYK: "Boston Yanks",
  CAR: "Carolina Panthers",
  CHI: "Chicago Bears",
  CHC: "Chicago Cardinals",
  CHH: "Chicago Hornets",
  CHR: "Chicago Rockets",
  CRA: "Chicago Rockets",
  CIN: "Cincinnati Bengals",
  CLI: "Cleveland Indians",
  CLE: "Cleveland Browns",
  COL: "Columbus Tigers",
  CLT: "Baltimore Colts",
  CRD: "St. Louis Cardinals",
  DAL: "Dallas Cowboys",
  DAY: "Dayton Triangles",
  DEN: "Denver Broncos",
  DET: "Detroit Wolverines",
  DUL: "Duluth Kelleys",
  FRN: "Frankford Yellow Jackets",
  GB: "Green Bay Packers",
  GNB: "Green Bay Packers",
  HOU: "Houston Oilers",
  HAM: "Hammond Pros",
  HRT: "Hartford Blues",
  IND: "Indianapolis Colts",
  JAX: "Jacksonville Jaguars",
  KAN: "Kansas City Chiefs",
  KEN: "Kenosha Maroons",
  LAC: "Los Angeles Chargers",
  LAD: "Los Angeles Dons",
  LAB: "Los Angeles Buccaneers",
  LA: "Los Angeles Rams",
  MIN: "Minneapolis Red Jackets",
  MIA: "Miami Dolphins",
  MIL: "Milwaukee Badgers",
  MUN: "Muncie Flyers",
  NE: "New England Patriots",
  NOR: "New Orleans Saints",
  NWE: "New England Patriots",
  NYB: "New York Bulldogs",
  NYG: "New York Giants",
  NYJ: "New York Jets",
  NYT: "New York Titans",
  NYY: "New York Yankees",
  OAK: "Oakland Raiders",
  PHI: "Philadelphia Eagles",
  PHO: "Phoenix Cardinals",
  PIT: "Pittsburgh Steelers",
  POT: "Pottsville Maroons",
  PRT: "Portsmouth Spartans",
  PRV: "Providence Steam Roller",
  RAI: "Los Angeles Raiders",
  RAM: "Los Angeles Rams",
  RII: "Rock Island Independents",
  SDG: "San Diego Chargers",
  SEA: "Seattle Seahawks",
  SF: "San Francisco 49ers",
  SFO: "San Francisco 49ers",
  STL: "St. Louis Cardinals",
  TB: "Tampa Bay Buccaneers",
  TAM: "Tampa Bay Buccaneers",
  SIS: "Staten Island Stapletons",
  TOR: "Orange Tornadoes",
  WAS: "Washington Redskins",
};

const HISTORICAL_TEAM_NAMES = {
  AKR: [
    { through: 1925, name: "Akron Pros" },
    { through: 1926, name: "Akron Indians" },
  ],
  BAL: [
    { through: 1983, name: "Baltimore Colts" },
    { through: 9999, name: "Baltimore Ravens" },
  ],
  BOS: [
    { through: 1929, name: "Boston Bulldogs" },
    { through: 1932, name: "Boston Braves" },
    { through: 1936, name: "Boston Redskins" },
    { through: 1948, name: "Boston Yanks" },
    { through: 9999, name: "Boston Patriots" },
  ],
  BRL: [{ through: 1926, name: "Brooklyn Lions" }],
  BDA: [
    { through: 1948, name: "Brooklyn Dodgers" },
    { through: 1949, name: "Brooklyn-New York Yankees" },
  ],
  BCL: [{ through: 1949, name: "Baltimore Colts" }],
  BKN: [
    { through: 1943, name: "Brooklyn Dodgers" },
    { through: 1944, name: "Brooklyn Tigers" },
  ],
  BUF: [
    { through: 1923, name: "Buffalo All-Americans" },
    { through: 1925, name: "Buffalo Bisons" },
    { through: 1926, name: "Buffalo Rangers" },
    { through: 1929, name: "Buffalo Bisons" },
    { through: 1946, name: "Buffalo Bisons" },
    { through: 1949, name: "Buffalo Bills" },
    { through: 9999, name: "Buffalo Bills" },
  ],
  BYK: [{ through: 1948, name: "Boston Yanks" }],
  CAN: [{ through: 1926, name: "Canton Bulldogs" }],
  CAR: [{ through: 9999, name: "Carolina Panthers" }],
  CHI: [
    { through: 1920, name: "Decatur Staleys" },
    { through: 1921, name: "Chicago Staleys" },
    { through: 9999, name: "Chicago Bears" },
  ],
  CHH: [{ through: 1949, name: "Chicago Hornets" }],
  CHR: [
    { through: 1948, name: "Chicago Rockets" },
    { through: 1949, name: "Chicago Hornets" },
  ],
  CRA: [
    { through: 1948, name: "Chicago Rockets" },
    { through: 1949, name: "Chicago Hornets" },
  ],
  CHT: [{ through: 1920, name: "Chicago Tigers" }],
  CLI: [{ through: 1923, name: "Cleveland Indians" }],
  CIN: [{ through: 1934, name: "Cincinnati Reds" }],
  CLE: [
    { through: 1920, name: "Cleveland Tigers" },
    { through: 1921, name: "Cleveland Indians" },
    { through: 1931, name: "Cleveland Bulldogs" },
    { through: 9999, name: "Cleveland Browns" },
  ],
  COL: [
    { through: 1922, name: "Columbus Panhandles" },
    { through: 1926, name: "Columbus Tigers" },
  ],
  CRD: [
    { through: 1921, name: "Racine Cardinals" },
    { through: 1959, name: "Chicago Cardinals" },
    { through: 1987, name: "St. Louis Cardinals" },
    { through: 9999, name: "Arizona Cardinals" },
  ],
  DAY: [{ through: 1929, name: "Dayton Triangles" }],
  DET: [
    { through: 1920, name: "Detroit Heralds" },
    { through: 1921, name: "Detroit Tigers" },
    { through: 1926, name: "Detroit Panthers" },
    { through: 1928, name: "Detroit Wolverines" },
    { through: 9999, name: "Detroit Lions" },
  ],
  DHR: [{ through: 1920, name: "Detroit Heralds" }],
  DUL: [
    { through: 1925, name: "Duluth Kelleys" },
    { through: 1928, name: "Duluth Eskimos" },
  ],
  DTX: [{ through: 1962, name: "Dallas Texans" }],
  EVN: [{ through: 1922, name: "Evansville Crimson Giants" }],
  FRN: [{ through: 1931, name: "Frankford Yellow Jackets" }],
  HAM: [{ through: 1926, name: "Hammond Pros" }],
  HRT: [{ through: 1926, name: "Hartford Blues" }],
  HOU: [
    { through: 1996, name: "Houston Oilers" },
    { through: 1998, name: "Tennessee Oilers" },
    { through: 9999, name: "Tennessee Titans" },
  ],
  IND: [{ through: 9999, name: "Indianapolis Colts" }],
  JAX: [{ through: 9999, name: "Jacksonville Jaguars" }],
  KAN: [
    { through: 1924, name: "Kansas City Blues" },
    { through: 1926, name: "Kansas City Cowboys" },
    { through: 9999, name: "Kansas City Chiefs" },
  ],
  KEN: [{ through: 1924, name: "Kenosha Maroons" }],
  LAC: [{ through: 1960, name: "Los Angeles Chargers" }],
  LAD: [{ through: 1949, name: "Los Angeles Dons" }],
  LAB: [{ through: 1926, name: "Los Angeles Buccaneers" }],
  LOU: [
    { through: 1924, name: "Louisville Brecks" },
    { through: 1926, name: "Louisville Colonels" },
  ],
  MIN: [
    { through: 1924, name: "Minneapolis Marines" },
    { through: 1930, name: "Minneapolis Red Jackets" },
    { through: 9999, name: "Minnesota Vikings" },
  ],
  MIA: [
    { through: 1946, name: "Miami Seahawks" },
    { through: 9999, name: "Miami Dolphins" },
  ],
  MUN: [{ through: 1921, name: "Muncie Flyers" }],
  NYB: [
    { through: 1949, name: "New York Bulldogs" },
    { through: 1951, name: "New York Yanks" },
  ],
  POT: [{ through: 1928, name: "Pottsville Maroons" }],
  PRT: [{ through: 1933, name: "Portsmouth Spartans" }],
  PRV: [{ through: 1931, name: "Providence Steam Roller" }],
  RCH: [{ through: 1925, name: "Rochester Jeffersons" }],
  MIL: [{ through: 1926, name: "Milwaukee Badgers" }],
  NYY: [
    { through: 1928, name: "New York Yankees" },
    { through: 1948, name: "New York Yankees" },
    { through: 1949, name: "Brooklyn-New York Yankees" },
    { through: 1951, name: "New York Yanks" },
  ],
  NYT: [{ through: 1962, name: "New York Titans" }],
  OOR: [{ through: 1923, name: "Oorang Indians" }],
  PHO: [
    { through: 1993, name: "Phoenix Cardinals" },
    { through: 9999, name: "Arizona Cardinals" },
  ],
  PHI: [
    { through: 1942, name: "Philadelphia Eagles" },
    { through: 1943, name: "Phil-Pitt Steagles" },
    { through: 9999, name: "Philadelphia Eagles" },
  ],
  PIT: [
    { through: 1939, name: "Pittsburgh Pirates" },
    { through: 1942, name: "Pittsburgh Steelers" },
    { through: 1943, name: "Phil-Pitt Steagles" },
    { through: 1944, name: "Card-Pitt" },
    { through: 9999, name: "Pittsburgh Steelers" },
  ],
  RAC: [
    { through: 1924, name: "Racine Legion" },
    { through: 1926, name: "Racine Tornadoes" },
  ],
  RAI: [
    { through: 1994, name: "Los Angeles Raiders" },
    { through: 9999, name: "Oakland Raiders" },
  ],
  RAM: [
    { through: 1994, name: "Los Angeles Rams" },
    { through: 2015, name: "St. Louis Rams" },
    { through: 9999, name: "Los Angeles Rams" },
  ],
  RII: [{ through: 1926, name: "Rock Island Independents" }],
  SIS: [{ through: 1932, name: "Staten Island Stapletons" }],
  STL: [
    { through: 1923, name: "St. Louis All-Stars" },
    { through: 1987, name: "St. Louis Cardinals" },
    { through: 2015, name: "St. Louis Rams" },
  ],
  TOL: [{ through: 1923, name: "Toledo Maroons" }],
  TOR: [{ through: 1930, name: "Orange Tornadoes" }],
};

const QUERY_OVERRIDES = {
  "192010030rii": [
    "\"Rock Island Independents\" \"Muncie Flyers\"",
    "Rock Island Independents Muncie Flyers",
  ],
  "195912270clt": [
    "\"Baltimore Colts\" \"New York Giants\" championship",
    "Baltimore Colts New York Giants championship",
    "\"Colts\" \"Giants\" \"31-16\"",
  ],
  "197909020crd": [
    "\"Dallas Cowboys\" \"St. Louis Cardinals\" football",
    "\"Cowboys\" \"Cardinals\" \"1979\"",
    "\"St. Louis Cardinals\" \"Dallas Cowboys\"",
  ],
};

const DEFAULT_CANARY_BOXSCORES = [
  "192010030rii",
  "195912270clt",
  "197909020crd",
];

const MONTHS = {
  january: 0,
  february: 1,
  march: 2,
  april: 3,
  may: 4,
  june: 5,
  july: 6,
  august: 7,
  september: 8,
  october: 9,
  november: 10,
  december: 11,
};

function parseArgs() {
  const args = {
    port: 9224,
    queue: DEFAULT_QUEUE,
    outRoot: DEFAULT_OUT_ROOT,
    boxscores: DEFAULT_CANARY_BOXSCORES,
    firstN: null,
    startAfter: null,
    yearMin: 1920,
    yearMax: 1996,
    pagesPerGame: 3,
    maxQueriesPerGame: 2,
    skipExisting: true,
    fetchTimeoutMs: 20_000,
    imageFetchAttempts: 3,
    imageFetchRetryDelayMs: 30_000,
    assetMode: "all",
    pdfWaitMs: 35_000,
    browserPrintPdfFallback: false,
    freshCaptureTabs: true,
    freshMainTarget: true,
    captureDelayMs: 10_000,
    searchAttempts: 3,
    searchRetryDelayMs: 5_000,
    allowLooseYearFallback: false,
  };

  for (let i = 2; i < process.argv.length; i += 1) {
    const arg = process.argv[i];
    const next = process.argv[i + 1];
    if (arg === "--port") {
      args.port = Number(next);
      i += 1;
    } else if (arg === "--queue") {
      args.queue = next;
      i += 1;
    } else if (arg === "--out-root") {
      args.outRoot = next;
      i += 1;
    } else if (arg === "--boxscores") {
      args.boxscores = next.split(",").map((x) => x.trim()).filter(Boolean);
      i += 1;
    } else if (arg === "--first-n") {
      args.firstN = Number(next);
      i += 1;
    } else if (arg === "--start-after") {
      args.startAfter = next;
      i += 1;
    } else if (arg === "--year-min") {
      args.yearMin = Number(next);
      i += 1;
    } else if (arg === "--year-max") {
      args.yearMax = Number(next);
      i += 1;
    } else if (arg === "--pages-per-game") {
      args.pagesPerGame = Number(next);
      i += 1;
    } else if (arg === "--max-queries-per-game") {
      args.maxQueriesPerGame = Number(next);
      i += 1;
    } else if (arg === "--fetch-timeout-ms") {
      args.fetchTimeoutMs = Number(next);
      i += 1;
    } else if (arg === "--image-fetch-attempts") {
      args.imageFetchAttempts = Number(next);
      i += 1;
    } else if (arg === "--image-fetch-retry-delay-ms") {
      args.imageFetchRetryDelayMs = Number(next);
      i += 1;
    } else if (arg === "--asset-mode") {
      args.assetMode = next;
      i += 1;
    } else if (arg === "--pdf-only") {
      args.assetMode = "pdf-only";
    } else if (arg === "--pdf-wait-ms") {
      args.pdfWaitMs = Number(next);
      i += 1;
    } else if (arg === "--browser-print-pdf-fallback") {
      args.browserPrintPdfFallback = true;
    } else if (arg === "--no-fresh-capture-tabs") {
      args.freshCaptureTabs = false;
    } else if (arg === "--reuse-main-target") {
      args.freshMainTarget = false;
    } else if (arg === "--capture-delay-ms") {
      args.captureDelayMs = Number(next);
      i += 1;
    } else if (arg === "--search-attempts") {
      args.searchAttempts = Number(next);
      i += 1;
    } else if (arg === "--search-retry-delay-ms") {
      args.searchRetryDelayMs = Number(next);
      i += 1;
    } else if (arg === "--allow-loose-year-fallback") {
      args.allowLooseYearFallback = true;
    } else if (arg === "--no-skip-existing") {
      args.skipExisting = false;
    } else if (arg === "--help") {
      console.log(`Usage:
  node scripts/newspapers_com_acquire_canary.mjs [options]

Options:
  --port <n>                 Edge remote debugging port, default 9224
  --queue <path>             game_completeness_review_queue.csv path
  --out-root <path>          output root on D:
  --boxscores <ids>          comma-separated boxscore IDs
  --first-n <n>              process first n queue rows after year/start filters
  --start-after <id>         skip queue rows through this boxscore ID
  --year-min <yyyy>          default 1920
  --year-max <yyyy>          default 1996
  --pages-per-game <n>       candidate pages to capture per game, default 3
  --max-queries-per-game <n> search queries per game, default 2
  --fetch-timeout-ms <n>     per-artifact fetch timeout, default 20000
  --image-fetch-attempts <n> retry count for full-page JPG fetches, default 3
  --image-fetch-retry-delay-ms <n> delay after image HTTP 429, default 30000
  --asset-mode <all|pdf-only> capture all assets or only browser PDF, default all
  --pdf-only                 shorthand for --asset-mode pdf-only
  --pdf-wait-ms <n>          wait time for browser PDF download, default 35000
  --browser-print-pdf-fallback save a clearly labeled browser-rendered PDF if official PDF fails
  --no-fresh-capture-tabs    reuse the main Newspapers.com tab for captures
  --reuse-main-target        attach to an existing Newspapers.com tab instead of opening a fresh one
  --capture-delay-ms <n>     delay after each captured candidate page, default 10000
  --search-attempts <n>      reload attempts for blank search pages, default 3
  --search-retry-delay-ms <n> delay between blank search retries, default 5000
  --allow-loose-year-fallback allow arbitrary same-year candidate if no date-window result
  --no-skip-existing         recapture existing image IDs
`);
      process.exit(0);
    }
  }
  if (!["all", "pdf-only"].includes(args.assetMode)) {
    throw new Error(`Unsupported --asset-mode ${args.assetMode}; expected all or pdf-only`);
  }
  return args;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];
    if (quoted) {
      if (ch === "\"" && next === "\"") {
        field += "\"";
        i += 1;
      } else if (ch === "\"") {
        quoted = false;
      } else {
        field += ch;
      }
    } else if (ch === "\"") {
      quoted = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (ch !== "\r") {
      field += ch;
    }
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  const header = rows.shift();
  return rows
    .filter((r) => r.some((v) => v !== ""))
    .map((r) => Object.fromEntries(header.map((h, idx) => [h, r[idx] ?? ""])));
}

async function readQueue(queuePath) {
  const text = await fs.readFile(queuePath, "utf8");
  return parseCsv(text);
}

function slug(value, max = 120) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, max);
}

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function sanitizeUrl(raw) {
  if (!raw) return raw;
  try {
    const url = new URL(raw);
    for (const key of [...url.searchParams.keys()]) {
      if (/iat|user|token|auth|signature|key|session/i.test(key)) {
        url.searchParams.set(key, "[redacted]");
      }
    }
    return url.toString();
  } catch {
    return String(raw)
      .replace(/(iat=)[^&]+/g, "$1[redacted]")
      .replace(/(user=)[^&]+/g, "$1[redacted]");
  }
}

function sanitizeObject(value) {
  if (Array.isArray(value)) return value.map(sanitizeObject);
  if (value && typeof value === "object") {
    for (const key of Object.keys(value)) {
      value[key] = sanitizeObject(value[key]);
    }
    return value;
  }
  return typeof value === "string" ? sanitizeUrl(value) : value;
}

function parseNewspapersDate(text) {
  const match = String(text || "").match(
    /\b(?:Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\b/
  );
  if (!match) return null;
  const month = MONTHS[match[1].toLowerCase()];
  if (month == null) return null;
  return new Date(Date.UTC(Number(match[3]), month, Number(match[2])));
}

function daysBetween(a, b) {
  return Math.round((a.getTime() - b.getTime()) / 86_400_000);
}

function dateIso(date) {
  return date ? date.toISOString().slice(0, 10) : null;
}

function addUtcDays(date, days) {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function teamNameFor(row, code) {
  const year = Number(row.year || String(row.game_date || "").slice(0, 4));
  const historical = HISTORICAL_TEAM_NAMES[code];
  if (historical) {
    const hit = historical.find((entry) => year <= entry.through);
    if (hit) return hit.name;
  }
  return TEAM_NAMES[code] || code;
}

function yearForRow(row) {
  return Number(row.year || String(row.game_date || "").slice(0, 4));
}

function teamCity(teamName) {
  const words = String(teamName || "").trim().split(/\s+/).filter(Boolean);
  if (words.length <= 1) return String(teamName || "").trim();
  return words.slice(0, -1).join(" ");
}

function teamNickname(teamName) {
  const words = String(teamName || "").trim().split(/\s+/).filter(Boolean);
  return words.length ? words[words.length - 1] : String(teamName || "").trim();
}

function uniqueQueries(queries) {
  const seen = new Set();
  const out = [];
  for (const query of queries) {
    const normalized = String(query || "").replace(/\s+/g, " ").trim();
    const key = normalized.toLowerCase();
    if (!normalized || seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
  }
  return out;
}

function modernQueriesForGame(row, home, away, year) {
  const homeCity = teamCity(home);
  const awayCity = teamCity(away);
  const homeNick = teamNickname(home);
  const awayNick = teamNickname(away);
  return uniqueQueries([
    `"${home}" "${away}" "box score"`,
    `"${home}" "${away}" statistics`,
    `${homeNick} ${awayNick} "NFL summaries" ${year}`,
    `${homeNick} ${awayNick} "NFL roundup" ${year}`,
    `${homeNick} ${awayNick} "box score" ${year}`,
    `${homeCity} ${awayCity} football ${year}`,
    `${home} ${away} ${year}`,
    `${homeNick} ${awayNick} football statistics ${year}`,
  ]);
}

function searchUrlFor(query, row) {
  const params = new URLSearchParams({ keyword: query });
  if (row && row.game_date) {
    const gameDate = new Date(`${row.game_date}T00:00:00Z`);
    const modernWindow = yearForRow(row) >= 1979;
    params.set("date-start", dateIso(addUtcDays(gameDate, modernWindow ? 1 : 0)));
    params.set("date-end", dateIso(addUtcDays(gameDate, modernWindow ? 4 : 14)));
  }
  return `https://www.newspapers.com/search/results/?${params.toString()}`;
}

function queryForGame(row) {
  if (QUERY_OVERRIDES[row.boxscore_id]) return QUERY_OVERRIDES[row.boxscore_id];
  const home = teamNameFor(row, row.home_team);
  const away = teamNameFor(row, row.away_team);
  const year = row.year || String(row.game_date || "").slice(0, 4);
  if (yearForRow(row) >= 1979) return modernQueriesForGame(row, home, away, year);
  return [`"${home}" "${away}"`, `${home} ${away} ${year}`, `${home} ${away} football`];
}

class CdpClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.nextId = 1;
    this.pending = new Map();
  }

  async connect() {
    this.socket = new WebSocket(this.wsUrl);
    this.socket.addEventListener("message", (event) => {
      let msg;
      try {
        msg = JSON.parse(String(event.data));
      } catch {
        return;
      }
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject, timer } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        clearTimeout(timer);
        if (msg.error) reject(new Error(msg.error.message || JSON.stringify(msg.error)));
        else resolve(msg);
      }
    });
    await new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, { once: true });
      this.socket.addEventListener("error", reject, { once: true });
    });
    await this.send("Runtime.enable");
  }

  async send(method, params = {}, timeoutMs = 30_000) {
    const id = this.nextId++;
    const payload = JSON.stringify({ id, method, params });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`CDP timeout for ${method}`));
        }
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.socket.send(payload);
    });
  }

  async evalJson(fnSource, timeoutMs = 30_000) {
    const result = await this.send(
      "Runtime.evaluate",
      {
        returnByValue: true,
        expression: `JSON.stringify((${fnSource})())`,
      },
      timeoutMs
    );
    return JSON.parse(result.result.result.value);
  }

  async evalValue(expression, timeoutMs = 30_000) {
    const result = await this.send(
      "Runtime.evaluate",
      { returnByValue: true, awaitPromise: true, expression },
      timeoutMs
    );
    return result.result.result.value;
  }

  close() {
    try {
      this.socket?.close();
    } catch {
      // Best-effort shutdown; the process can still exit without a clean close.
    }
  }
}

async function getNewspapersTarget(port, options = {}) {
  if (options.freshMainTarget) {
    const created = await createPageTarget(port, "https://www.newspapers.com/browse/").catch(() => null);
    if (created?.webSocketDebuggerUrl) return created;
  }

  const targets = await fetch(`http://127.0.0.1:${port}/json/list`).then((r) => r.json());
  const target =
    targets.find((t) => t.type === "page" && /newspapers\.com\/image\//i.test(t.url)) ||
    targets.find((t) => t.type === "page" && /newspapers\.com/i.test(t.url) && !/\/search\//i.test(t.url)) ||
    targets.find((t) => t.type === "page" && /newspapers\.com/i.test(t.url) && !/\/search\/results/i.test(t.url));
  if (target) return target;

  const created = await fetch(
    `http://127.0.0.1:${port}/json/new?${encodeURIComponent("https://www.newspapers.com/browse/")}`,
    { method: "PUT" }
  ).then((r) => (r.ok ? r.json() : null)).catch(() => null);
  if (!created?.webSocketDebuggerUrl) {
    throw new Error(`No Newspapers.com page target found on 127.0.0.1:${port}. Sign in first.`);
  }
  return created;
}

async function createPageTarget(port, url) {
  const created = await fetch(`http://127.0.0.1:${port}/json/new?${encodeURIComponent(url)}`, {
    method: "PUT",
  }).then((r) => {
    if (!r.ok) throw new Error(`target create failed: HTTP ${r.status}`);
    return r.json();
  });
  if (!created?.webSocketDebuggerUrl) throw new Error(`target create did not return a websocket for ${url}`);
  return created;
}

async function closePageTarget(port, targetId) {
  if (!targetId) return;
  await fetch(`http://127.0.0.1:${port}/json/close/${targetId}`, { method: "PUT" }).catch(() => null);
}

async function wait(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForPageText(cdp, predicate, timeoutMs = 20_000) {
  const start = Date.now();
  let last = "";
  while (Date.now() - start < timeoutMs) {
    try {
      last = await cdp.evalValue(
        "String((document.body && document.body.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 3000))",
        20_000
      );
    } catch (error) {
      last = `page_text_probe_error: ${error.message || String(error)}`;
    }
    if (predicate(last)) return last;
    await wait(1_500);
  }
  return last;
}

async function readSearchPage(cdp, attempt, waitText) {
  return await cdp.evalJson(`() => {
    function clean(s) { return (s || '').replace(/\\s+/g, ' ').trim(); }
    const body = clean(document.body?.innerText || '');
    const total = (body.match(/Clear all ([0-9,]+) matches/) || body.match(/([0-9,]+) matches/))?.[1] || null;
    const links = Array.from(document.querySelectorAll('a[href]'))
      .map((a, idx) => ({
        idx,
        text: clean(a.innerText || a.getAttribute('aria-label') || a.title || '').slice(0, 300),
        href: a.href,
        cls: String(a.className || '').slice(0, 100)
      }))
      .filter(x => /\\/image\\/[0-9]+/.test(x.href || '') || / Page |Page \\d+/i.test(x.text || ''))
      .slice(0, 120);
    return {
      url: location.href,
      title: document.title,
      totalMatches: total,
      bodyLead: body.slice(0, 1800),
      links,
      attempt: ${attempt},
      waitText: ${JSON.stringify(String(waitText || "").slice(0, 400))}
    };
  }`, 45_000);
}

async function search(cdp, query, options = {}) {
  const url = searchUrlFor(query, options.searchRow);
  const attempts = Math.max(1, options.searchAttempts || 3);
  let lastResult = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    let searchCdp = cdp;
    let targetToClose = null;
    try {
      if (options.port) {
        const target = await createPageTarget(options.port, url);
        targetToClose = target.id;
        searchCdp = new CdpClient(target.webSocketDebuggerUrl);
        await searchCdp.connect();
      } else {
        await cdp.send("Page.navigate", { url });
      }
      const waitText = await waitForPageText(
        searchCdp,
        (text) =>
          /matches|No matches|Start Free Trial|Sign in|Account|Print or Download/i.test(text) &&
          !/Paper Name .* Loading Date .* Loading Location/i.test(text),
        60_000
      );
      await wait(2_000);
      lastResult = await readSearchPage(searchCdp, attempt, waitText);
      const hasUsableDom = Boolean(lastResult.bodyLead || lastResult.title || lastResult.links.length > 0);
      if (hasUsableDom) {
        return { ...lastResult, searchBlank: false, attempts: attempt };
      }
    } catch (error) {
      lastResult = {
        url,
        title: "",
        totalMatches: null,
        bodyLead: "",
        links: [],
        attempt,
        searchError: error?.message || String(error),
      };
    } finally {
      if (searchCdp !== cdp) searchCdp.close();
      await closePageTarget(options.port, targetToClose);
    }
    if (attempt < attempts) {
      await wait(options.searchRetryDelayMs || 5_000);
    }
  }
  return { ...lastResult, searchBlank: true, attempts };
}

function extractImageId(href) {
  return String(href || "").match(/\/image\/(\d+)/)?.[1] || null;
}

function parseResultDetails(text) {
  const page = String(text || "").match(/Page\s+([A-Z]?\d+)/i)?.[1] || null;
  const publication = String(text || "").split(" • Page ")[0]?.trim() || null;
  return { publication, page };
}

function rankCandidates(row, searchResults) {
  const gameDate = new Date(`${row.game_date}T00:00:00Z`);
  const seen = new Map();
  for (const result of searchResults) {
    for (const link of result.links) {
      const imageId = extractImageId(link.href);
      if (!imageId) continue;
      const existing = seen.get(imageId);
      if (existing && (!link.text || existing.text.length >= link.text.length)) continue;
      const resultDate = parseNewspapersDate(link.text);
      const detail = parseResultDetails(link.text);
      let score = 0;
      const diff = resultDate ? daysBetween(resultDate, gameDate) : null;
      if (diff != null && diff >= 1 && diff <= 4) score += 150 - Math.abs(diff) * 10;
      else if (diff != null && diff > 4 && diff <= 14) score += 50 - diff;
      if (/championship|defeat|rout|capture|win|football|grid|lineup|meet|clash/i.test(link.text)) score += 15;
      score -= link.idx * 0.1;
      seen.set(imageId, {
        imageId,
        href: link.href,
        text: link.text,
        publication: detail.publication,
        page: detail.page,
        resultDate: dateIso(resultDate),
        score,
      });
    }
  }
  return [...seen.values()].sort((a, b) => b.score - a.score);
}

function selectCandidates(row, ranked, limit, options = {}) {
  const gameDate = new Date(`${row.game_date}T00:00:00Z`);
  const selected = [];
  const add = (candidate) => {
    if (!candidate || selected.some((x) => x.imageId === candidate.imageId)) return;
    selected.push(candidate);
  };

  for (const candidate of ranked) {
    if (!candidate.resultDate) continue;
    const diff = daysBetween(new Date(`${candidate.resultDate}T00:00:00Z`), gameDate);
    if (diff >= 1 && diff <= 4) add(candidate);
    if (selected.length >= limit) return selected;
  }
  if (selected.length > 0) return selected;
  for (const candidate of ranked) {
    if (!candidate.resultDate) continue;
    const diff = daysBetween(new Date(`${candidate.resultDate}T00:00:00Z`), gameDate);
    if (diff <= 0 || diff > 14) continue;
    add(candidate);
    if (selected.length >= limit) return selected;
  }
  if (options.allowLooseYearFallback) {
    for (const candidate of ranked) {
      add(candidate);
      if (selected.length >= limit) return selected;
    }
  }
  return selected;
}

async function waitForFileCount(dir, ext, minCount, timeoutMs = 30_000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
    const files = entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(ext))
      .map((entry) => entry.name);
    if (files.length >= minCount) return files;
    await wait(1_000);
  }
  const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
  return entries
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(ext))
    .map((entry) => entry.name);
}

function manifestHasSuccessfulFullPagePair(manifestText) {
  let hasPdf = false;
  let hasJpg = false;
  for (const line of manifestText.split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line);
      if (!entry.ok) continue;
      const classification = String(entry.classification || "");
      if (classification === "newspapers_com_fullpage_pdf_browser_download") hasPdf = true;
      if (classification.startsWith("newspapers_com_fullpage_jpg")) hasJpg = true;
    } catch {
      continue;
    }
  }
  return hasPdf && hasJpg;
}

function manifestHasSuccessfulPdf(manifestText) {
  for (const line of manifestText.split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line);
      if (
        entry.ok &&
        (entry.classification === "newspapers_com_fullpage_pdf_browser_download" ||
          entry.classification === "newspapers_com_browser_rendered_pdf_fallback")
      ) {
        return true;
      }
    } catch {
      continue;
    }
  }
  return false;
}

async function clickDownloadMenu(cdp) {
  return await cdp.evalValue(`(async () => {
    const clean = s => (s || '').replace(/\\s+/g, ' ').trim();
    const byText = t => Array.from(document.querySelectorAll('button,a,[role=button]'))
      .find(x => clean(x.innerText || x.getAttribute('aria-label') || x.title || '').toLowerCase().includes(t.toLowerCase()));
    const print = document.getElementById('btn-print') || byText('Print or Download');
    if (!print) return { opened: false, step: 'print missing', text: clean(document.body?.innerText || '').slice(0, 1200) };
    print.click();
    await new Promise(r => setTimeout(r, 800));
    const entire = byText('Entire Page');
    if (!entire) return { opened: false, step: 'entire missing', text: clean(document.body?.innerText || '').slice(-1200) };
    entire.click();
    await new Promise(r => setTimeout(r, 800));
    return { opened: true, text: clean(document.body?.innerText || '').slice(-1200) };
  })()`);
}

async function captureCandidate(cdp, row, candidate, outDir, options) {
  const resultDate = candidate.resultDate || "unknown_date";
  const publicationSlug = slug(candidate.publication || "newspaper");
  const pageSlug = slug(String(candidate.page || "page"));
  const assetSuffix = options.assetMode === "pdf-only" ? "_pdf_only" : "";
  const resultRoot = path.posix.join(
    outDir,
    `result_${candidate.imageId}_${publicationSlug}_${resultDate}_p${pageSlug}${assetSuffix}`
  );
  const downloadDir = path.posix.join(resultRoot, "downloads");
  const manifestPath = path.posix.join(resultRoot, "fetch_manifest.jsonl");

  if (options.skipExisting) {
    const existingManifest = await fs
      .readFile(manifestPath, "utf8")
      .then((text) => text)
      .catch(() => "");
    const hasCompleteExisting =
      options.assetMode === "pdf-only"
        ? manifestHasSuccessfulPdf(existingManifest)
        : manifestHasSuccessfulFullPagePair(existingManifest);
    if (hasCompleteExisting) {
      return { imageId: candidate.imageId, skipped: true, resultRoot };
    }
  }

  if (options.freshCaptureTabs && options.port) {
    const target = await createPageTarget(options.port, "about:blank");
    const freshCdp = new CdpClient(target.webSocketDebuggerUrl);
    await freshCdp.connect();
    try {
      return await captureCandidate(freshCdp, row, candidate, outDir, {
        ...options,
        freshCaptureTabs: false,
      });
    } finally {
      freshCdp.close();
      await closePageTarget(options.port, target.id);
    }
  }

  await fs.mkdir(downloadDir, { recursive: true });
  await cdp.send("Browser.setDownloadBehavior", {
    behavior: "allow",
    downloadPath: downloadDir.replaceAll("/", "\\"),
    eventsEnabled: true,
  });

  await cdp.send("Page.navigate", { url: candidate.href });
  const text = await waitForPageText(
    cdp,
    (pageText) => /Print or Download|Subscribe|Sign in|captcha|verify you are human/i.test(pageText),
    30_000
  );
  if (/Sign in/i.test(text) || /Subscribe|Start Free Trial/i.test(text) || /captcha|verify you are human/i.test(text)) {
    return {
      imageId: candidate.imageId,
      blocked: true,
      reason: text.match(/captcha|verify you are human/i)
        ? "captcha"
        : text.match(/Sign in/i)
          ? "signed_out"
          : "subscription_wall",
      resultRoot,
    };
  }

  const viewerMeta = await cdp.evalJson(`() => {
    function clean(s) { return (s || '').replace(/\\s+/g, ' ').trim(); }
    const text = clean(document.body?.innerText || '');
    return {
      url: location.href,
      title: document.title,
      bodyLead: text.slice(0, 1800),
      accessSignals: {
        signIn: /sign in|log in|login/i.test(text),
        subscribe: /subscribe|free trial|publisher extra/i.test(text),
        printDownload: /Print or Download|Save as JPG|Save as PDF/i.test(text),
        captcha: /captcha|verify you are human|robot/i.test(text)
      },
      controls: Array.from(document.querySelectorAll('button,a[href],[role=button]'))
        .map((el, idx) => ({
          idx,
          tag: el.tagName,
          role: el.getAttribute('role'),
          text: clean(el.innerText || el.getAttribute('aria-label') || el.title || '').slice(0, 160),
          href: el.href || null,
          id: el.id || null
        }))
        .filter(x => /Print|Download|Save as JPG|Save as PDF|Clip|Share|Zoom|page|Newspaper|Herald|Review|Argus|Pantagraph|Colts|Giants|Cowboys|Cardinals/i.test([x.text, x.href, x.id].join(' ')))
        .slice(0, 140)
    };
  }`);

  const manifest = [];
  const pdfBefore = await waitForFileCount(downloadDir, ".pdf", 0, 100);
  const pdfMenu = await clickDownloadMenu(cdp);
  const pdfClick = await cdp.evalValue(`(() => {
    const clean = s => (s || '').replace(/\\s+/g, ' ').trim();
    const pdf = Array.from(document.querySelectorAll('button,a,[role=button]'))
      .find(x => clean(x.innerText || x.getAttribute('aria-label') || x.title || '').toLowerCase().includes('save as pdf'));
    if (!pdf) return 'missing';
    pdf.click();
    return 'clicked';
  })()`);
  await waitForFileCount(downloadDir, ".pdf", Math.max(1, pdfBefore.length + 1), options.pdfWaitMs || 35_000);

  let jpgMenu = null;
  let jpgClick = "unattempted";
  if (options.assetMode !== "pdf-only") {
    jpgMenu = await clickDownloadMenu(cdp);
    jpgClick = await cdp.evalValue(`(async () => {
      const clean = s => (s || '').replace(/\\s+/g, ' ').trim();
      const jpg = Array.from(document.querySelectorAll('button,a,[role=button]'))
        .find(x => clean(x.innerText || x.getAttribute('aria-label') || x.title || '').toLowerCase().includes('save as jpg'));
      if (!jpg) return 'missing';
      jpg.click();
      await new Promise(r => setTimeout(r, 5000));
      return 'clicked';
    })()`);
  }

  const resources = await cdp.evalJson(`() => performance.getEntriesByType('resource')
    .map(r => r.name)
    .filter(n => /${candidate.imageId}|download|jpg|pdf|print|image|api|hits|article|clipping/i.test(n))
    .slice(-160)`);

  async function fetchSave(url, outPath, classification) {
    if (!url) {
      manifest.push({
        capturedAtUtc: new Date().toISOString(),
        classification,
        ok: false,
        error: "missing source url",
      });
      return;
    }
    const timeoutMs = options.fetchTimeoutMs || 20_000;
    const isFullPageImage = classification.startsWith("newspapers_com_fullpage_jpg");
    const maxAttempts = isFullPageImage ? Math.max(1, options.imageFetchAttempts || 3) : 1;
    let lastError = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(new Error(`fetch timeout after ${timeoutMs}ms`)), timeoutMs);
      try {
        const res = await fetch(url, { signal: controller.signal });
        const buf = Buffer.from(await res.arrayBuffer());
        if (res.ok) await fs.writeFile(outPath, buf);
        if (res.ok || res.status !== 429 || attempt === maxAttempts) {
          manifest.push({
            capturedAtUtc: new Date().toISOString(),
            classification,
            sourceUrlSanitized: sanitizeUrl(url),
            outputPath: res.ok ? outPath.replaceAll("/", "\\") : null,
            status: res.status,
            ok: res.ok,
            contentType: res.headers.get("content-type"),
            bytes: buf.length,
            sha256: sha256(buf),
            attempts: attempt,
          });
          return;
        }
        lastError = `HTTP ${res.status} after ${attempt} attempt(s)`;
      } catch (error) {
        lastError = error?.message || String(error);
        if (attempt === maxAttempts) {
          manifest.push({
            capturedAtUtc: new Date().toISOString(),
            classification,
            sourceUrlSanitized: sanitizeUrl(url),
            outputPath: null,
            status: null,
            ok: false,
            error: lastError,
            attempts: attempt,
          });
          return;
        }
      } finally {
        clearTimeout(timer);
      }
      await wait(options.imageFetchRetryDelayMs || 30_000);
    }
    manifest.push({
      capturedAtUtc: new Date().toISOString(),
      classification,
      sourceUrlSanitized: sanitizeUrl(url),
      outputPath: null,
      status: null,
      ok: false,
      error: lastError || "fetch failed without response",
      attempts: maxAttempts,
    });
  }

  const jpgUrl =
    options.assetMode === "pdf-only"
      ? null
      : resources
          .filter(
            (url) => /\/img\/img\?/.test(url) && url.includes(`id=${candidate.imageId}`) && /a=(print|download)/.test(url)
          )
          .slice(-1)[0];
  const hitsUrl = resources
    .filter((url) => /\/api\/search\/hits/.test(url) && url.includes(candidate.imageId))
    .slice(-1)[0];
  const clippingUrl =
    resources
      .filter((url) => /\/api\/clipping\/page/.test(url) && url.includes(candidate.imageId))
      .slice(-1)[0] ||
    `https://www.newspapers.com/api/clipping/page?page_id=${candidate.imageId}&start=0&count=25`;

  if (options.assetMode === "pdf-only") {
    manifest.push({
      capturedAtUtc: new Date().toISOString(),
      classification: "newspapers_com_fullpage_jpg_unattempted_pdf_only_run",
      ok: null,
      note: "Skipped JPG fetch because --asset-mode pdf-only was used.",
    });
  } else {
    await fetchSave(
      jpgUrl,
      path.posix.join(downloadDir, `${publicationSlug}_${resultDate}_p${pageSlug}_fullpage.jpg`),
      "newspapers_com_fullpage_jpg_from_official_print_image"
    );
  }
  await fetchSave(hitsUrl, path.posix.join(resultRoot, "search_hits.json"), "newspapers_com_search_hits_json");
  await fetchSave(clippingUrl, path.posix.join(resultRoot, "page_clippings.json"), "newspapers_com_page_clippings_json");

  const pdfFiles = await waitForFileCount(downloadDir, ".pdf", 1, 1_000);
  if (pdfFiles.length === 0) {
    manifest.push({
      capturedAtUtc: new Date().toISOString(),
      classification: "newspapers_com_fullpage_pdf_browser_download",
      sourceUrlSanitized: "browser official Save as PDF control",
      outputPath: null,
      status: null,
      ok: false,
      contentType: "application/pdf",
      error: `no PDF file appeared after pdfClick=${pdfClick}`,
      pdfMenu,
      pdfWaitMs: options.pdfWaitMs || 35_000,
    });
    if (options.browserPrintPdfFallback) {
      try {
        await cdp.send("Input.dispatchKeyEvent", {
          type: "keyDown",
          key: "Escape",
          code: "Escape",
          windowsVirtualKeyCode: 27,
        });
        await cdp.send("Input.dispatchKeyEvent", {
          type: "keyUp",
          key: "Escape",
          code: "Escape",
          windowsVirtualKeyCode: 27,
        });
        await wait(750);
        const prePrintText = await cdp
          .evalValue(
            "String((document.body && document.body.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 2000))",
            10_000
          )
          .catch((error) => `pre_print_text_probe_error: ${error?.message || String(error)}`);
        if (/client-side exception has occurred while loading www\.newspapers\.com/i.test(prePrintText)) {
          throw new Error(`viewer application error before browser PDF fallback: ${prePrintText.slice(0, 300)}`);
        }
        await cdp.send("Emulation.setDeviceMetricsOverride", {
          width: 1800,
          height: 2600,
          deviceScaleFactor: 1,
          mobile: false,
        });
        await cdp.evalValue(`(async () => {
          const zoomOut = document.getElementById('btn-zoom-out');
          for (let i = 0; i < 4; i += 1) {
            if (zoomOut) zoomOut.click();
            await new Promise(r => setTimeout(r, 400));
          }
          window.scrollTo(0, 0);
          await new Promise(r => setTimeout(r, 1500));
          return true;
        })()`);
        const printed = await cdp.send(
          "Page.printToPDF",
          {
            printBackground: true,
            landscape: false,
            paperWidth: 11,
            paperHeight: 17,
            marginTop: 0.2,
            marginBottom: 0.2,
            marginLeft: 0.2,
            marginRight: 0.2,
            scale: 0.65,
            preferCSSPageSize: false,
          },
          60_000
        );
        const pdfPath = path.posix.join(
          downloadDir,
          `${publicationSlug}_${resultDate}_p${pageSlug}_browser_rendered.pdf`
        );
        const pdfBase64 = printed.result?.data || printed.data || "";
        const buf = Buffer.from(pdfBase64, "base64");
        if (buf.length === 0) throw new Error("Page.printToPDF returned an empty PDF payload");
        await fs.writeFile(pdfPath, buf);
        manifest.push({
          capturedAtUtc: new Date().toISOString(),
          classification: "newspapers_com_browser_rendered_pdf_fallback",
          sourceUrlSanitized: "Chrome DevTools Page.printToPDF of accessible Newspapers.com viewer",
          outputPath: pdfPath.replaceAll("/", "\\"),
          status: 200,
          ok: true,
          contentType: "application/pdf",
          bytes: buf.length,
          sha256: sha256(buf),
          note: "Fallback PDF generated from the rendered browser page after official Save as PDF produced no file.",
        });
      } catch (error) {
        manifest.push({
          capturedAtUtc: new Date().toISOString(),
          classification: "newspapers_com_browser_rendered_pdf_fallback",
          sourceUrlSanitized: "Chrome DevTools Page.printToPDF of accessible Newspapers.com viewer",
          outputPath: null,
          status: null,
          ok: false,
          contentType: "application/pdf",
          error: error?.message || String(error),
        });
      }
    }
  }
  for (const file of pdfFiles) {
    const pdfPath = path.posix.join(downloadDir, file);
    const buf = await fs.readFile(pdfPath);
    manifest.push({
      capturedAtUtc: new Date().toISOString(),
      classification: "newspapers_com_fullpage_pdf_browser_download",
      sourceUrlSanitized: "browser official Save as PDF control",
      outputPath: pdfPath.replaceAll("/", "\\"),
      status: 200,
      ok: true,
      contentType: "application/pdf",
      bytes: buf.length,
      sha256: sha256(buf),
    });
  }

  const sourceMeta = {
    source: "newspapers_com",
    capturedAtUtc: new Date().toISOString(),
    boxscoreId: row.boxscore_id,
    imageId: candidate.imageId,
    publication: candidate.publication,
    publicationDate: candidate.resultDate,
    page: candidate.page,
    canonicalUrl: candidate.href,
    resultText: candidate.text,
    score: candidate.score,
    sourcePageTitle: viewerMeta.title,
    accessSignals: viewerMeta.accessSignals,
    notes: [
      "Captured by canary runner using signed-in Edge session and official Print or Download controls.",
      "Signed URL tokens and account identifiers are redacted from manifests.",
    ],
  };

  sanitizeObject(viewerMeta);
  await fs.writeFile(path.posix.join(resultRoot, "viewer_meta.json"), JSON.stringify(viewerMeta, null, 2), "utf8");
  await fs.writeFile(path.posix.join(resultRoot, "source_meta.json"), JSON.stringify(sourceMeta, null, 2), "utf8");
  await fs.writeFile(manifestPath, manifest.map((entry) => JSON.stringify(entry)).join("\n") + "\n", "utf8");

  return {
    imageId: candidate.imageId,
    publication: candidate.publication,
    date: candidate.resultDate,
    page: candidate.page,
    resultRoot,
    assetMode: options.assetMode,
    pdfMenu,
    pdfClick,
    jpgMenu,
    jpgClick,
    artifacts: manifest.map((entry) => ({
      classification: entry.classification,
      ok: entry.ok,
      bytes: entry.bytes,
      outputPath: entry.outputPath,
    })),
  };
}

async function writeJsonPreservingUsefulCanonical(filePath, data, runId, dataIsUseful, existingIsUseful) {
  const text = JSON.stringify(data, null, 2);
  if (!dataIsUseful) {
    const existingText = await fs.readFile(filePath, "utf8").catch(() => "");
    let keepExistingCanonical = false;
    try {
      keepExistingCanonical = existingIsUseful(JSON.parse(existingText));
    } catch {
      keepExistingCanonical = false;
    }
    if (keepExistingCanonical) {
      const parsed = path.posix.parse(filePath);
      await fs.writeFile(path.posix.join(parsed.dir, `${parsed.name}_${runId}_empty${parsed.ext}`), text, "utf8");
      return;
    }
  }
  await fs.writeFile(filePath, text, "utf8");
}

async function main() {
  const args = parseArgs();
  const queue = await readQueue(args.queue);
  let rows;
  if (args.firstN != null) {
    let eligible = queue.filter((row) => {
      const year = Number(row.year);
      return year >= args.yearMin && year <= args.yearMax;
    });
    if (args.startAfter) {
      const idx = eligible.findIndex((row) => row.boxscore_id === args.startAfter);
      if (idx >= 0) eligible = eligible.slice(idx + 1);
    }
    rows = eligible.slice(0, args.firstN);
  } else {
    const byBoxscoreId = new Map(queue.map((row) => [row.boxscore_id, row]));
    const missing = args.boxscores.filter((id) => !byBoxscoreId.has(id));
    if (missing.length > 0) {
      throw new Error(`Requested boxscore IDs not found in ${args.queue}: ${missing.join(", ")}`);
    }
    rows = args.boxscores.map((id) => byBoxscoreId.get(id));
  }
  if (rows.length === 0) {
    throw new Error(`No requested boxscores found in ${args.queue}`);
  }

  const target = await getNewspapersTarget(args.port, args);
  const cdp = new CdpClient(target.webSocketDebuggerUrl);
  await cdp.connect();

  const runRoot = path.posix.join(
    args.outRoot,
    `_canary_runs`,
    new Date().toISOString().replace(/[-:]/g, "").replace(/\..+$/, "Z")
  );
  await fs.mkdir(runRoot, { recursive: true });
  const runId = path.posix.basename(runRoot);
  const runManifestPath = path.posix.join(runRoot, "run_manifest.json");

  const runManifest = {
    generatedAtUtc: new Date().toISOString(),
    port: args.port,
    queue: args.queue,
    outRoot: args.outRoot,
    runRoot,
    pagesPerGame: args.pagesPerGame,
    maxQueriesPerGame: args.maxQueriesPerGame,
    fetchTimeoutMs: args.fetchTimeoutMs,
    imageFetchAttempts: args.imageFetchAttempts,
    imageFetchRetryDelayMs: args.imageFetchRetryDelayMs,
    assetMode: args.assetMode,
    pdfWaitMs: args.pdfWaitMs,
    browserPrintPdfFallback: args.browserPrintPdfFallback,
    freshCaptureTabs: args.freshCaptureTabs,
    freshMainTarget: args.freshMainTarget,
    captureDelayMs: args.captureDelayMs,
    searchAttempts: args.searchAttempts,
    searchRetryDelayMs: args.searchRetryDelayMs,
    allowLooseYearFallback: args.allowLooseYearFallback,
    games: [],
  };
  const writeRunManifest = async () => {
    await fs.writeFile(runManifestPath, JSON.stringify(runManifest, null, 2), "utf8");
  };
  await writeRunManifest();
  console.log(`run manifest: ${runManifestPath}`);

  for (const row of rows) {
    console.log(`\n=== ${row.boxscore_id} ${row.game_date} ${row.away_team}@${row.home_team} ===`);
    const gameOut = path.posix.join(args.outRoot, row.boxscore_id, "newspapers_com_auto_canary");
    await fs.mkdir(gameOut, { recursive: true });
    const queries = queryForGame(row).slice(0, args.maxQueriesPerGame);
    const searchResults = [];
    for (const query of queries) {
      console.log(`search: ${query}`);
      const result = await search(cdp, query, { ...args, searchRow: row });
      searchResults.push({ query, ...result });
      const searchPayload = { source: "newspapers_com", boxscoreId: row.boxscore_id, query, ...result };
      await writeJsonPreservingUsefulCanonical(
        path.posix.join(gameOut, `search_results_${slug(query, 60)}.json`),
        searchPayload,
        runId,
        Array.isArray(result.links) && result.links.length > 0,
        (existing) => Array.isArray(existing.links) && existing.links.length > 0
      );
    }

    const ranked = rankCandidates(row, searchResults);
    await writeJsonPreservingUsefulCanonical(
      path.posix.join(gameOut, "candidate_rankings.json"),
      { boxscoreId: row.boxscore_id, game: row, queries, candidates: ranked },
      runId,
      ranked.length > 0,
      (existing) => Array.isArray(existing.candidates) && existing.candidates.length > 0
    );

    const selectedCandidates = selectCandidates(row, ranked, args.pagesPerGame, args);
    const captured = [];
    const gameManifest = {
      capturedAtUtc: new Date().toISOString(),
      boxscoreId: row.boxscore_id,
      game: row,
      queries,
      candidateCount: ranked.length,
      selectedCandidates,
      captured,
    };
    const gameManifestPath = path.posix.join(gameOut, "auto_canary_manifest.json");
    runManifest.games.push(gameManifest);
    await writeJsonPreservingUsefulCanonical(
      gameManifestPath,
      gameManifest,
      runId,
      captured.length > 0,
      (existing) => Array.isArray(existing.captured) && existing.captured.length > 0
    );
    await writeRunManifest();

    for (let idx = 0; idx < selectedCandidates.length; idx += 1) {
      const candidate = selectedCandidates[idx];
      console.log(`capture: ${candidate.imageId} ${candidate.publication || ""} ${candidate.resultDate || ""}`);
      let result;
      try {
        result = await captureCandidate(cdp, row, candidate, gameOut, args);
      } catch (error) {
        result = {
          imageId: candidate.imageId,
          publication: candidate.publication,
          date: candidate.resultDate,
          page: candidate.page,
          assetMode: args.assetMode,
          captureError: true,
          error: error?.message || String(error),
          stack: error?.stack || null,
          artifacts: [
            {
              classification: "newspapers_com_capture_error",
              ok: false,
              error: error?.message || String(error),
            },
          ],
        };
        await cdp.send("Page.navigate", { url: "about:blank" }).catch(() => null);
      }
      captured.push(result);
      console.log(JSON.stringify(result, null, 2));
      gameManifest.updatedAtUtc = new Date().toISOString();
      await writeJsonPreservingUsefulCanonical(
        gameManifestPath,
        gameManifest,
        runId,
        captured.length > 0,
        (existing) => Array.isArray(existing.captured) && existing.captured.length > 0
      );
      await writeRunManifest();
      if (args.captureDelayMs > 0) {
        await wait(args.captureDelayMs);
      }
    }

    gameManifest.updatedAtUtc = new Date().toISOString();
    await writeJsonPreservingUsefulCanonical(
      gameManifestPath,
      gameManifest,
      runId,
      captured.length > 0,
      (existing) => Array.isArray(existing.captured) && existing.captured.length > 0
    );
    await writeRunManifest();
  }

  await writeRunManifest();
  cdp.close();
  console.log(`\nrun manifest: ${runManifestPath}`);
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});
