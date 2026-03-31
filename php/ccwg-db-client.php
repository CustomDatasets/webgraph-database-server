<?php
declare(strict_types=1);

/*
 * ccwg-db-client.php — Test client for ccwg-db-server.php (PHP + curl)
 *
 * RUN:
 *   php ccwg-db-client.php
 *
 * REQUIRES:
 *   PHP with the curl extension (enabled by default in most distributions)
 *
 * DESCRIPTION:
 *   Sends a POST request to the local ccwg-db-server with the hostnames
 *   listed below (one per line) and prints the JSON response with pretty
 *   formatting. Start ccwg-db-server.php first in another terminal.
 */


// --- Hostnames to look up (one per line) ---

$hostnames = "google.com\nreddit.com";


// --- Server endpoint ---

$server_url = 'http://127.0.0.1:8999/';


// --- Send request ---

$ch = curl_init($server_url);
curl_setopt($ch, CURLOPT_POST, true);
curl_setopt($ch, CURLOPT_POSTFIELDS, $hostnames);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 30);
curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 5);

$response = curl_exec($ch);

if ($response === false) {
	fprintf(STDERR, "curl error: %s\n", curl_error($ch));
	curl_close($ch);
	exit(1);
}

$http_code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);

if ($http_code !== 200) {
	fprintf(STDERR, "HTTP %d\n", $http_code);
}


// --- Pretty-print JSON response ---

$json = json_decode($response, true);

if ($json === null) {
	echo $response . "\n";
} else {
	echo json_encode($json, JSON_PRETTY_PRINT) . "\n";
}
