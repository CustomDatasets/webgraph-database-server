<?php
declare(strict_types=1);


/*
 * ccwg-db-server.php — CommonCrawl WebGraph Rank Lookup Server (PHP + Swoole)
 *
 * RUN:
 *   php ccwg-db-server.php
 *
 * INSTALL SWOOLE:
 *   pecl install swoole
 *   echo "extension=swoole.so" >> $(php -i | grep 'php.ini' | head -1 | awk '{print $NF}')
 *
 *   Or on Debian/Ubuntu:
 *     apt install php-swoole
 *
 * REQUIRES:
 *   PHP 8.1+ with ext-swoole and ext-sqlite3
 *
 * DESCRIPTION:
 *   Single-file Swoole HTTP server for CommonCrawl WebGraph rank lookups.
 *   Accepts plain-text POST bodies (one hostname per line, max 10), looks
 *   up each hostname in two sets of sharded SQLite databases (host + domain),
 *   and returns structured JSON with historical rank data.
 *
 *   Based on ApiServer.php but without rate limiting.
 *
 * ARCHITECTURE:
 *   Swoole spawns WORKER_NUM separate OS processes. Each worker opens all
 *   64 SQLite databases (32 domain shards + 32 host shards) as read-only
 *   handles on startup. Swoole dispatches incoming connections across
 *   workers automatically via its event loop.
 *
 * DATA FLOW:
 *   1. Client POSTs plain text to / (one hostname per line, max 10).
 *   2. Each hostname is normalized: lowercase, strip trailing dot, reverse
 *      dot notation (e.g. "Google.COM" -> "com.google").
 *   3. The reversed key is DJB2-hashed to select a shard (0-31).
 *   4. Both the host DB and domain DB for that shard are queried.
 *   5. Results are returned as JSON keyed by the original submitted hostname.
 */


// --- Network ---

const LISTEN_HOST = '127.0.0.1';
const LISTEN_PORT = 8999;


// --- Workers ---

const WORKER_NUM = 16;


// --- Database paths ---

const DOMAIN_DB_DIR = '/home/oa_adm/cc-webgraph/data-merged-domains';
const HOST_DB_DIR   = '/home/oa_adm/cc-webgraph/data-merged-hosts';


// --- Sharding ---

const SHARD_COUNT    = 32;
const SHARD_BIT_MASK = 0x1F;
const DJB2_SEED      = 5381;
const DJB2_U32_MASK  = 0xFFFFFFFF;


// --- Request limits ---

const MAX_HOSTS_PER_REQUEST = 10;


// --- CORS ---

const ENABLE_CORS = true;


// --- Access logging ---

const LOG_DIR              = 'logs';
const LOG_FLUSH_INTERVAL_S = 300;
const LOG_FLUSH_SIZE_BYTES = 6144;


// --- SQLite tuning (per-handle PRAGMAs) ---

const SQLITE_CACHE_SIZE_KB = 65536;
const SQLITE_MMAP_SIZE     = 268435456;


################################################################################
class HostNormalizer
{
	//============================================================================
	public function __construct()
	{
	}

	//----------------------------------------------------------------------------
	public function Normalize(string $hostname): string
	{
		$hostname = strtolower(trim($hostname));
		$hostname = rtrim($hostname, '.');

		$parts = explode('.', $hostname);
		$parts = array_reverse($parts);

		return implode('.', $parts);
	}

	//----------------------------------------------------------------------------
	public function GetShard(string $reversed_host): int
	{
		return $this->djb2Hash($reversed_host) & SHARD_BIT_MASK;
	}

	//----------------------------------------------------------------------------
	private function djb2Hash(string $str): int
	{
		$hash = DJB2_SEED;
		$len  = strlen($str);

		for ($i = 0; $i < $len; $i++) {
			$c    = ord($str[$i]);
			$hash = (($hash << 5) + $hash) + $c;
			$hash = $hash & DJB2_U32_MASK;
		}

		return $hash;
	}
}


################################################################################
class DataParser
{
	//============================================================================
	public function __construct()
	{
	}

	//----------------------------------------------------------------------------
	public function ParseHostData(string $raw_data): array
	{
		$entries = [];
		$rows    = explode("\n", $raw_data);

		foreach ($rows as $row) {
			$row = trim($row);
			if ($row === '') {
				continue;
			}

			$fields      = explode("\t", $row);
			$field_count = count($fields);

			if ($field_count < 7) {
				continue;
			}

			$entry = [
				'year_month'  => $fields[0],
				'hc_pos'      => (int)$fields[1],
				'hc_val'      => (int)$fields[2],
				'pr_pos'      => (int)$fields[3],
				'pr_val'      => (int)$fields[4],
				'hc_val_norm' => (int)$fields[5],
				'pr_val_norm' => (int)$fields[6],
			];

			if ($field_count >= 8) {
				$entry['n_hosts'] = (int)$fields[7];
			}

			$entries[] = $entry;
		}

		return $entries;
	}
}


################################################################################
class DbPool
{
	private string $domainDbDir = '';
	private string $hostDbDir   = '';

	/** @var array<int, \SQLite3|null> */
	private array $domainDbs = [];

	/** @var array<int, \SQLite3|null> */
	private array $hostDbs = [];

	//============================================================================
	public function __construct(string $domain_db_dir, string $host_db_dir)
	{
		$this->domainDbDir = $domain_db_dir;
		$this->hostDbDir   = $host_db_dir;

		for ($i = 0; $i < SHARD_COUNT; $i++) {
			$this->domainDbs[$i] = null;
			$this->hostDbs[$i]   = null;
		}
	}

	//----------------------------------------------------------------------------
	public function OpenAll(): void
	{
		for ($i = 0; $i < SHARD_COUNT; $i++) {
			$this->domainDbs[$i] = $this->openShard($this->domainDbDir, 'domain', $i);
			$this->hostDbs[$i]   = $this->openShard($this->hostDbDir, 'host', $i);
		}
	}

	//----------------------------------------------------------------------------
	public function LookupDomain(int $shard, string $reversed_host): string|false
	{
		return $this->query($this->domainDbs[$shard] ?? null, $reversed_host);
	}

	//----------------------------------------------------------------------------
	public function LookupHost(int $shard, string $reversed_host): string|false
	{
		return $this->query($this->hostDbs[$shard] ?? null, $reversed_host);
	}

	//----------------------------------------------------------------------------
	public function CloseAll(): void
	{
		for ($i = 0; $i < SHARD_COUNT; $i++) {
			if ($this->domainDbs[$i] !== null) {
				$this->domainDbs[$i]->close();
				$this->domainDbs[$i] = null;
			}
			if ($this->hostDbs[$i] !== null) {
				$this->hostDbs[$i]->close();
				$this->hostDbs[$i] = null;
			}
		}
	}

	//----------------------------------------------------------------------------
	private function openShard(string $dir, string $type, int $shard): \SQLite3|null
	{
		$path = sprintf('%s/%s.%d.db', $dir, $type, $shard);

		if (!file_exists($path)) {
			return null;
		}

		$db = new \SQLite3($path, SQLITE3_OPEN_READONLY);
		@$db->exec('PRAGMA cache_size = -' . SQLITE_CACHE_SIZE_KB . ';');
		@$db->exec('PRAGMA mmap_size = ' . SQLITE_MMAP_SIZE . ';');

		return $db;
	}

	//----------------------------------------------------------------------------
	private function query(\SQLite3|null $db, string $reversed_host): string|false
	{
		if ($db === null) {
			return false;
		}

		$stmt = $db->prepare('SELECT data FROM host_data WHERE host = :host LIMIT 1');
		if ($stmt === false) {
			return false;
		}

		$stmt->bindValue(':host', $reversed_host, SQLITE3_TEXT);
		$result = $stmt->execute();
		if ($result === false) {
			$stmt->close();
			return false;
		}
		$row = $result->fetchArray(SQLITE3_ASSOC);
		$stmt->close();

		if ($row === false) {
			return false;
		}

		return $row['data'];
	}
}


################################################################################
class AccessLogger
{
	private string $filePath = '';
	private string $buffer   = '';

	//============================================================================
	public function __construct(int $worker_id)
	{
		if (!is_dir(LOG_DIR)) {
			@mkdir(LOG_DIR, 0755, true);
		}

		$timestamp      = date('Ymd-His');
		$wid            = sprintf('%02d', $worker_id);
		$this->filePath = LOG_DIR . "/CCWG-DB-{$timestamp}-{$wid}.log";
	}

	//----------------------------------------------------------------------------
	public function Append(string $ip, array $hostnames): void
	{
		$ts            = date('Y-m-d H:i:s');
		$hosts_str     = implode(' ', $hostnames);
		$this->buffer .= "{$ts}\t{$ip}\t{$hosts_str}\n";

		if (strlen($this->buffer) >= LOG_FLUSH_SIZE_BYTES) {
			$this->Flush();
		}
	}

	//----------------------------------------------------------------------------
	public function Flush(): void
	{
		if ($this->buffer === '') {
			return;
		}

		file_put_contents($this->filePath, $this->buffer, FILE_APPEND | LOCK_EX);
		$this->buffer = '';
	}
}


################################################################################
class LookupHandler
{
	private ?HostNormalizer $normalizer = null;
	private ?DataParser $parser         = null;
	private ?DbPool $dbPool             = null;

	//============================================================================
	public function __construct(HostNormalizer $normalizer, DataParser $parser, DbPool $db_pool)
	{
		$this->normalizer = $normalizer;
		$this->parser     = $parser;
		$this->dbPool     = $db_pool;
	}

	//----------------------------------------------------------------------------
	public function Handle(array $hostnames): array
	{
		$results = [];

		foreach ($hostnames as $hostname) {
			if (!is_string($hostname)) {
				continue;
			}

			$hostname = trim($hostname);
			if ($hostname === '') {
				continue;
			}

			$reversed = $this->normalizer->Normalize($hostname);
			$shard    = $this->normalizer->GetShard($reversed);

			$host_data   = [];
			$domain_data = [];

			$raw_host = $this->dbPool->LookupHost($shard, $reversed);
			if ($raw_host !== false) {
				$host_data = $this->parser->ParseHostData($raw_host);
			}

			$raw_domain = $this->dbPool->LookupDomain($shard, $reversed);
			if ($raw_domain !== false) {
				$domain_data = $this->parser->ParseHostData($raw_domain);
			}

			$results[$hostname] = [
				'host'   => $host_data,
				'domain' => $domain_data,
			];
		}

		return $results;
	}

	//----------------------------------------------------------------------------
	public function GetMaxHostsPerRequest(): int
	{
		return MAX_HOSTS_PER_REQUEST;
	}
}


//------------------------------------------------------------------------------
// Server bootstrap
//------------------------------------------------------------------------------

$server = new \Swoole\HTTP\Server(LISTEN_HOST, LISTEN_PORT);

$server->set([
	'worker_num'       => WORKER_NUM,
	'max_request'      => 0,
	'enable_coroutine' => true,
	'log_level'        => SWOOLE_LOG_WARNING,
]);

//------------------------------------------------------------------------------
$server->on('WorkerStart', function (\Swoole\HTTP\Server $server, int $worker_id) {
	$normalizer = new HostNormalizer();
	$parser     = new DataParser();
	$db_pool    = new DbPool(DOMAIN_DB_DIR, HOST_DB_DIR);
	$db_pool->OpenAll();

	$handler = new LookupHandler($normalizer, $parser, $db_pool);
	$logger  = new AccessLogger($worker_id);

	$server->handler = $handler;
	$server->db_pool = $db_pool;
	$server->logger  = $logger;

	\Swoole\Timer::tick(LOG_FLUSH_INTERVAL_S * 1000, function () use ($logger) {
		$logger->Flush();
	});

	echo "Worker {$worker_id} started, databases opened.\n";
});

//------------------------------------------------------------------------------
$server->on('WorkerStop', function (\Swoole\HTTP\Server $server, int $worker_id) {
	if (isset($server->logger)) {
		$server->logger->Flush();
	}
	if (isset($server->db_pool)) {
		$server->db_pool->CloseAll();
	}
	echo "Worker {$worker_id} stopped, databases closed.\n";
});

//------------------------------------------------------------------------------
$server->on('Request', function (\Swoole\HTTP\Request $request, \Swoole\HTTP\Response $response) use ($server) {
	$response->header('Content-Type', 'application/json');

	if (ENABLE_CORS) {
		$response->header('Access-Control-Allow-Origin', '*');
		$response->header('Access-Control-Allow-Methods', 'POST, OPTIONS');
		$response->header('Access-Control-Allow-Headers', 'Content-Type');

		if ($request->server['request_method'] === 'OPTIONS') {
			$response->status(200);
			$response->end();
			return;
		}
	}

	if ($request->server['request_uri'] !== '/') {
		$response->status(404);
		$response->end(json_encode(['error' => 'Not found. Use POST /']));
		return;
	}

	if (($request->server['request_method'] ?? '') !== 'POST') {
		$response->status(405);
		$response->end(json_encode(['error' => 'Method not allowed. Use POST.']));
		return;
	}

	$body = $request->rawContent();
	if ($body === false || $body === '') {
		$response->status(400);
		$response->end(json_encode([
			'error'           => 'Request must contain 1-' . MAX_HOSTS_PER_REQUEST . ' hostnames, one per line',
			'max_per_request' => MAX_HOSTS_PER_REQUEST,
		]));
		return;
	}

	$lines     = explode("\n", $body);
	$hostnames = [];
	foreach ($lines as $line) {
		$line = trim($line);
		if ($line !== '') {
			$hostnames[] = $line;
		}
	}

	$host_count = count($hostnames);

	if ($host_count < 1 || $host_count > MAX_HOSTS_PER_REQUEST) {
		$response->status(400);
		$response->end(json_encode([
			'error'           => 'Request must contain 1-' . MAX_HOSTS_PER_REQUEST . ' hostnames, one per line',
			'max_per_request' => MAX_HOSTS_PER_REQUEST,
		]));
		return;
	}

	$ip = $request->header['x-real-ip']
		?? $request->header['x-forwarded-for']
		?? $request->server['remote_addr']
		?? '0.0.0.0';

	if (str_contains($ip, ',')) {
		$ip = trim(explode(',', $ip)[0]);
	}

	/** @var AccessLogger $logger */
	$logger = $server->logger;
	$logger->Append($ip, $hostnames);

	/** @var LookupHandler $handler */
	$handler = $server->handler;
	$results = $handler->Handle($hostnames);

	$response->status(200);
	$response->end(json_encode([
		'results' => $results,
	]));
});

//------------------------------------------------------------------------------
$server->on('Start', function (\Swoole\HTTP\Server $server) {
	echo "=== CommonCrawl WebGraph DB Server (PHP/Swoole) ===\n";
	echo "Listening on " . LISTEN_HOST . ":" . LISTEN_PORT . "\n";
	echo "Workers: " . WORKER_NUM . "\n";
	echo "Domain DB dir: " . DOMAIN_DB_DIR . "\n";
	echo "Host DB dir:   " . HOST_DB_DIR . "\n";
	echo "Max per request: " . MAX_HOSTS_PER_REQUEST . "\n\n";
});

$server->start();
