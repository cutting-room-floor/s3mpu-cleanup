#!/usr/bin/env node

var args = process.argv.slice(2);
if (args.length < 1) {
    console.log('Usage: s3mpu-cleanup <bucket> [expire uploads older than N sec]');
    console.log('Cleanup stale multipart uploads from an S3 bucket.');
    process.exit(1);
}

var s3mpuCleanup = require('..');
var bucket = args[0].replace(/^s3:\/\//,'');
var before = args[1] ? parseInt(args[1], 10) : undefined;
s3mpuCleanup({
    bucket: bucket,
    before: before ? new Date(+new Date - before*1000) : undefined
}, function(err, aborted) {
    if (err) throw err;
    aborted.forEach(function(item) {
        console.log('aborted %s', item);
    });
});

