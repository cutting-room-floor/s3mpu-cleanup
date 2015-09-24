var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var queue = require('queue-async');

module.exports = cleanup;
module.exports.listAll = listAll;
module.exports.s3 = s3;

/**
 * Cleanup stale S3 multipart uploads by aborting stale MPUs.
 * @param {Object} options
 * @param {String} options.bucket S3 bucket to cleanup
 * @param {Date} options.before Oldest date of MPUs to leave untouched. Defaults to the current time - 24 hours
 * @param {Function} options.logger Optional function to call after each abort operation for logging
 * @param {Function} callback Callback function
 */
function cleanup(options, callback) {
    options.before = options.before || new Date(+new Date() - 864e5);
    options.logger = options.logger || function() {};
    listAll(options, function(err, uploads) {
        if (err) return callback(err);
        var q = queue(4);
        var aborted = [];
        uploads.forEach(function(upload) {
            if (upload.Initiated > options.before) return;
            q.defer(function(params, callback) {
                s3.abortMultipartUpload(params, function(err) {
                    if (err) return callback(err);
                    aborted.push('s3://' + options.bucket + '/' + upload.Key + '@' + upload.UploadId);
                    options.logger('s3://' + options.bucket + '/' + upload.Key + '@' + upload.UploadId);
                    callback();
                });
            }, {
                Bucket: options.bucket,
                UploadId: upload.UploadId,
                Key: upload.Key
            });
        });
        q.awaitAll(function(err) {
            if (err) return callback(err);
            callback(null, aborted);
        });
    });
}

/**
 * List all MPUs for a bucket.
 * @param {Object} options
 * @param {String} options.bucket S3 bucket to list
 * @param {Function} callback Callback function
 */
function listAll(options, callback) {
    var uploads = [];
    function ls(markers) {
        var params ={ Bucket: options.bucket };
        if (markers) {
            params.KeyMarker = markers.KeyMarker;
            params.UploadIdMarker = markers.UploadIdMarker;
        }
        s3.listMultipartUploads(params, function(err, data) {
            if (err) return callback(err);
            uploads = uploads.concat(data.Uploads);
            if (data.IsTruncated) return ls({
                KeyMarker: data.Uploads[data.Uploads.length-1].Key,
                UploadIdMarker: data.Uploads[data.Uploads.length-1].UploadId
            });
            callback(null, uploads);
        });
    }
    ls();
}

