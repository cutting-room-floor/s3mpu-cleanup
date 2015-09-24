var tape = require('tape');
var s3mpuCleanup = require('./index');

tape('listAll', function(assert) {
    s3mpuCleanup.listAll({ bucket: 'test-bucket' }, function(err, data) {
        assert.ifError(err);
        assert.equal(data.length, 3, 'lists for all items using marker');
        assert.deepEqual(data.map(function(o) { return o.UploadId; }), [
            'id.1',
            'id.2',
            'id.3'
        ], 'returns upload items');
        assert.end();
    });
});

tape('cleanup (defaults)', function(assert) {
    s3mpuCleanup({bucket:'test-bucket'}, function(err, aborted) {
        assert.ifError(err);
        assert.deepEqual(aborted, [
            's3://test-bucket/a.json@id.1'
        ], 'aborts expired a.json@id.1');
        assert.end();
    });
});

tape('cleanup (before=10min ago)', function(assert) {
    s3mpuCleanup({bucket:'test-bucket', before: new Date(+new Date - 60e3)}, function(err, aborted) {
        assert.ifError(err);
        assert.deepEqual(aborted, [
            's3://test-bucket/a.json@id.1',
            's3://test-bucket/c.json@id.3'
        ], 'aborts expired a.json@id.1, c.json@id.3');
        assert.end();
    });
});

// Mock abortMultipartUpload behavior.
s3mpuCleanup.s3.abortMultipartUpload = function(options, callback) {
    if (!options.Bucket || !options.UploadId || !options.Key) {
        throw new Error('Bucket, UploadId, Key params are required');
    }
    return callback();
};

// Mock listMultipartUploads behavior.
s3mpuCleanup.s3.listMultipartUploads = function(options, callback) {
    if (options.Bucket === 'test-bucket' && !options.KeyMarker) {
        return callback(null, {
            "Bucket": "test-bucket",
            "KeyMarker": "",
            "UploadIdMarker": "",
            "NextKeyMarker": "b.json",
            "NextUploadIdMarker": "id.2",
            "MaxUploads": 2,
            "IsTruncated": true,
            "Uploads": [
                {
                    "UploadId": "id.1",
                    "Key": "a.json",
                    "Initiated": new Date("2015-01-01T00:00:00.000Z"),
                    "StorageClass": "STANDARD",
                    "Owner": {
                        "DisplayName": "test",
                        "ID": "111"
                    },
                    "Initiator": {
                        "ID": "arn:aws:iam::111:user/test",
                        "DisplayName": "test"
                    }
                },
                {
                    "UploadId": "id.2",
                    "Key": "b.json",
                    "Initiated": new Date(),
                    "StorageClass": "STANDARD",
                    "Owner": {
                        "DisplayName": "test",
                        "ID": "111"
                    },
                    "Initiator": {
                        "ID": "arn:aws:iam::111:user/test",
                        "DisplayName": "test"
                    }
                }
            ],
            "CommonPrefixes": []
        });
    } else if (options.Bucket === 'test-bucket' && options.KeyMarker === 'b.json' && options.UploadIdMarker === 'id.2') {
        return callback(null, {
            "Bucket": "test-bucket",
            "KeyMarker": "b.json",
            "UploadIdMarker": "id.2",
            "NextKeyMarker": "c.json",
            "NextUploadIdMarker": "id.3",
            "MaxUploads": 2,
            "IsTruncated": false,
            "Uploads": [
                {
                    "UploadId": "id.3",
                    "Key": "c.json",
                    "Initiated": new Date(+new Date() - 36e5),
                    "StorageClass": "STANDARD",
                    "Owner": {
                        "DisplayName": "test",
                        "ID": "111"
                    },
                    "Initiator": {
                        "ID": "arn:aws:iam::111:user/test",
                        "DisplayName": "test"
                    }
                }
            ],
            "CommonPrefixes": []
        });
    } else {
        return callback(new Error('Mock: unsupported behavior'));
    }
};


